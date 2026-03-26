"""
cluster_eval.py
---------------
Evaluate a DataFrame of embeddings using HDBSCAN clustering.
Expects a DataFrame with a 'BlockId' column plus N feature columns.

Usage:
    from cluster_eval import ClusterEvaluator
    evaluator = ClusterEvaluator(occ_df, label_df, templates_path)
    results = evaluator.evaluate(embeddings_df)
    evaluator.print_report(results)
"""

import numpy as np
import pandas as pd
import hdbscan
from collections import Counter
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    adjusted_rand_score,
    normalized_mutual_info_score,
    homogeneity_completeness_v_measure,
)

# ── Best parameters from RCA score sweep ──────────────────────────────────────
# BEST_PARAMS = {
#    "min_cluster_size": 300,
#    "min_samples": 5,
#    "cluster_selection_method": "eom",
#    "metric": "euclidean",
# } best but produces too many clusters

BEST_PARAMS = {
    #"min_cluster_size": 648,
    "min_cluster_size": 60,
    "min_samples": 20,
    "cluster_selection_method": "eom",
    "metric": "euclidean",
}


# ── Label helpers ──────────────────────────────────────────────────────────────

def build_label_encoder(occ_df: pd.DataFrame) -> LabelEncoder:
    """Reconstruct LabelEncoder from the full occ DataFrame's Type column."""
    type_filled = occ_df["Type"].fillna(-1)
    le = LabelEncoder()
    le.fit(type_filled)
    return le


def build_encoded_to_label(le: LabelEncoder, templates_path: str) -> dict:
    """Map encoded int (0-N) -> human-readable event template string."""
    templates = pd.read_csv(templates_path)

    def clean_template(t: str) -> str:
        return t.replace("[*]", "").replace("  ", " ").strip()[:40]

    event_to_template = {
        row["EventId"]: clean_template(row["EventTemplate"])
        for _, row in templates.iterrows()
    }

    type_float_to_label = {-1.0: "Normal"}
    for val in le.classes_:
        if val == -1.0:
            continue
        event_id = f"E{int(val)}"
        label = event_to_template.get(event_id, f"Unknown {event_id}")
        type_float_to_label[val] = f"E{int(val)}: {label}"

    return {
        i: type_float_to_label.get(cls, f"Type {cls}")
        for i, cls in enumerate(le.classes_)
    }


def attach_labels(
    embeddings_df: pd.DataFrame,
    occ_df: pd.DataFrame,
    le: LabelEncoder,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Join embeddings to occ_df on BlockId and return
    (y_binary, y_type) aligned to embeddings_df row order.
    """
    merged = embeddings_df[["BlockId"]].merge(
        occ_df[["BlockId", "Label", "Type"]],
        on="BlockId",
        how="left",
    )
    y_binary = (merged["Label"] == "Fail").astype(int).values
    type_filled = merged["Type"].fillna(-1)
    y_type = le.transform(type_filled)
    return y_binary, y_type


# ── Metrics ────────────────────────────────────────────────────────────────────

def cluster_purity(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    mask = y_pred != -1
    if mask.sum() == 0:
        return 0.0
    lt, lp = y_true[mask], y_pred[mask]
    correct = sum(
        Counter(lt[lp == c]).most_common(1)[0][1] for c in np.unique(lp)
    )
    return correct / mask.sum()


def anomaly_cluster_recall(
    y_binary: np.ndarray, y_pred: np.ndarray, threshold: float
) -> float:
    anomaly_clusters = {
        c
        for c in np.unique(y_pred)
        if c != -1 and y_binary[y_pred == c].mean() >= threshold
    }
    true_anom = y_binary == 1
    if true_anom.sum() == 0:
        return 0.0
    return (true_anom & np.isin(y_pred, list(anomaly_clusters))).sum() / true_anom.sum()


def cluster_contamination_stats(
    y_binary: np.ndarray, y_pred: np.ndarray
) -> dict:
    clusters = [c for c in np.unique(y_pred) if c != -1]
    if not clusters:
        return {k: 0.0 for k in [
            "mean_cluster_anomaly_frac", "pct_clusters_pure90",
            "pct_clusters_pure75", "pct_clusters_mixed", "pct_clusters_normal",
        ]}
    anomaly_fracs = np.array([y_binary[y_pred == c].mean() for c in clusters])
    return {
        "mean_cluster_anomaly_frac": round(float(anomaly_fracs.mean()), 3),
        "pct_clusters_pure90":       round(float((anomaly_fracs >= 0.90).mean()), 3),
        "pct_clusters_pure75":       round(float((anomaly_fracs >= 0.75).mean()), 3),
        "pct_clusters_mixed":        round(float(((anomaly_fracs > 0.1) & (anomaly_fracs < 0.9)).mean()), 3),
        "pct_clusters_normal":       round(float((anomaly_fracs <= 0.1).mean()), 3),
    }


def failure_type_fragmentation(
    y_type: np.ndarray, y_pred: np.ndarray
) -> dict:
    frags = []
    for t in np.unique(y_type):
        clusters_for_type = np.unique(y_pred[y_type == t])
        clusters_for_type = clusters_for_type[clusters_for_type != -1]
        frags.append(len(clusters_for_type))
    if not frags:
        return {"mean_type_fragmentation": 0.0, "max_type_fragmentation": 0}
    return {
        "mean_type_fragmentation": round(float(np.mean(frags)), 2),
        "max_type_fragmentation":  int(np.max(frags)),
    }


def fragmentation_by_type(
    y_type: np.ndarray,
    y_pred: np.ndarray,
    encoded_to_label: dict,
) -> pd.DataFrame:
    """Per-type fragmentation breakdown, sorted descending."""
    rows = []
    for t in np.unique(y_type):
        clusters_for_type = np.unique(y_pred[y_type == t])
        clusters_for_type = clusters_for_type[clusters_for_type != -1]
        rows.append({
            "encoded_type": t,
            "label": encoded_to_label.get(int(t), f"Type {t}"),
            "n_incidents": int((y_type == t).sum()),
            "n_clusters": len(clusters_for_type),
        })
    return (
        pd.DataFrame(rows)
        .sort_values("n_clusters", ascending=False)
        .reset_index(drop=True)
    )


def evaluate_flat(
    y_binary: np.ndarray,
    y_type: np.ndarray,
    cluster_labels: np.ndarray,
) -> dict:
    """Full flat-cluster metric suite."""
    mask = cluster_labels != -1
    n_clusters = len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0)
    noise_frac = (~mask).mean()

    if mask.sum() < 2 or n_clusters < 2:
        ari_bin = nmi_bin = ari_type = nmi_type = hom = com = vm = 0.0
    else:
        ari_bin  = adjusted_rand_score(y_binary[mask], cluster_labels[mask])
        nmi_bin  = normalized_mutual_info_score(y_binary[mask], cluster_labels[mask])
        ari_type = adjusted_rand_score(y_type[mask], cluster_labels[mask])
        nmi_type = normalized_mutual_info_score(y_type[mask], cluster_labels[mask])
        hom, com, vm = homogeneity_completeness_v_measure(
            y_type[mask], cluster_labels[mask]
        )

    contamination = cluster_contamination_stats(y_binary, cluster_labels)
    fragmentation = failure_type_fragmentation(y_type, cluster_labels)

    return {
        "n_clusters":         n_clusters,
        "noise_frac":         round(float(noise_frac), 3),
        "purity_binary":      round(cluster_purity(y_binary, cluster_labels), 3),
        "purity_type":        round(cluster_purity(y_type, cluster_labels), 3),
        "anomaly_recall_50":  round(anomaly_cluster_recall(y_binary, cluster_labels, 0.50), 3),
        "anomaly_recall_75":  round(anomaly_cluster_recall(y_binary, cluster_labels, 0.75), 3),
        "anomaly_recall_90":  round(anomaly_cluster_recall(y_binary, cluster_labels, 0.90), 3),
        "ari_binary":         round(float(ari_bin), 3),
        "nmi_binary":         round(float(nmi_bin), 3),
        "ari_type":           round(float(ari_type), 3),
        "nmi_type":           round(float(nmi_type), 3),
        "homogeneity":        round(float(hom), 3),
        "completeness":       round(float(com), 3),
        "v_measure":          round(float(vm), 3),
        **contamination,
        **fragmentation,
    }


def rca_score(row: dict) -> float:
    max_frag = row.get("max_type_fragmentation", 1) or 1
    norm_frag = np.log1p(row["mean_type_fragmentation"]) / np.log1p(max_frag)
    return (
        0.35 * row["anomaly_recall_90"]
        + 0.25 * row["pct_clusters_pure90"]
        + 0.20 * (1 - row["noise_frac"])
        + 0.10 * (1 - row["pct_clusters_mixed"])
        + 0.10 * (1 - norm_frag)
    )


# ── Tree hierarchy evaluation ──────────────────────────────────────────────────

def get_leaves_under_node(
    node: int,
    tree_df: pd.DataFrame,
    n_points: int,
    max_depth: int = 50,
    depth: int = 0,
) -> list[int]:
    if depth >= max_depth or node < n_points:
        return [node] if node < n_points else []
    children = tree_df[tree_df["parent"] == node]
    leaf_pts  = children[children["child_size"] == 1]["child"].astype(int).tolist()
    internal  = children[children["child_size"] > 1]["child"].tolist()
    result = leaf_pts[:]
    for child in internal:
        result.extend(
            get_leaves_under_node(child, tree_df, n_points, max_depth, depth + 1)
        )
    return result


def evaluate_tree_hierarchy(
    clusterer: hdbscan.HDBSCAN,
    y_binary: np.ndarray,
    y_type: np.ndarray,
    encoded_to_label: dict,
    min_node_size: int = 10,
) -> pd.DataFrame:
    """
    Walk the condensed tree and compute per-node purity / composition.
    Returns a DataFrame sorted by node size (largest first).
    """
    tree_df   = clusterer.condensed_tree_.to_pandas()
    n_points  = len(y_binary)
    int_nodes = tree_df[tree_df["child_size"] > 1]["child"].unique()

    rows = []
    for node in int_nodes:
        pts = np.array(get_leaves_under_node(node, tree_df, n_points))
        pts = pts[pts < n_points]
        if len(pts) < min_node_size:
            continue

        anom_mask = y_binary[pts] == 1
        anom_pts  = pts[anom_mask]
        anomaly_frac = float(anom_mask.mean())

        if len(anom_pts) == 0:
            dominant_label = "Normal"
            type_purity    = 0.0
        else:
            counts = Counter(y_type[anom_pts])
            top_type, top_count = counts.most_common(1)[0]
            type_purity    = top_count / len(anom_pts)
            dominant_label = encoded_to_label.get(int(top_type), f"Type {top_type}")

        # lambda_val at which this node appears (persistence)
        node_row   = tree_df[tree_df["child"] == node]
        lambda_val = float(node_row["lambda_val"].values[0]) if len(node_row) else 0.0

        # Count direct children
        n_children = int(tree_df[tree_df["parent"] == node]["child"].nunique())

        rows.append({
            "node":           int(node),
            "size":           len(pts),
            "n_anomalies":    int(anom_mask.sum()),
            "anomaly_frac":   round(anomaly_frac, 3),
            "dominant_type":  dominant_label,
            "type_purity":    round(type_purity, 3),
            "n_children":     n_children,
            "lambda_val":     round(lambda_val, 4),
        })

    return (
        pd.DataFrame(rows)
        .sort_values("size", ascending=False)
        .reset_index(drop=True)
    )

def evaluate_tree_levels(clusterer, y_binary, y_type,
                         cut_levels=None, min_cluster_size=10):
    """
    Evaluate clustering quality at multiple cut-distance levels of the
    HDBSCAN single linkage tree. cut_levels are distance thresholds —
    smaller = more clusters (finer), larger = fewer clusters (coarser).
    Use the diagnostic block to pick meaningful values for your data.
    """
    if cut_levels is None:
        cut_levels = [0.02, 0.05, 0.1, 0.2, 0.5, 1.0]

    single_tree = clusterer.single_linkage_tree_
    level_results = []

    for cut in cut_levels:
        labels = single_tree.get_clusters(cut, min_cluster_size=min_cluster_size)
        metrics = evaluate_flat(y_binary, y_type, labels)
        metrics['cut_distance'] = cut
        level_results.append(metrics)

    levels_df = pd.DataFrame(level_results).set_index('cut_distance')

    max_frag = levels_df['mean_type_fragmentation'].max()
    levels_df['rca_score'] = levels_df.apply(
        lambda row: (
            0.35 * row['anomaly_recall_90']
          + 0.25 * row['pct_clusters_pure90']
          + 0.20 * (1 - row['noise_frac'])
          + 0.10 * (1 - row['pct_clusters_mixed'])
          + 0.10 * (1 - np.log1p(row['mean_type_fragmentation']) /
                        np.log1p(max_frag + 1e-9))
        ),
        axis=1
    ).round(4)

    display_cols = [
        'n_clusters', 'rca_score', 'purity_type',
        'anomaly_recall_90', 'noise_frac',
        'mean_type_fragmentation', 'pct_clusters_pure90', 'pct_clusters_mixed'
    ]
    print(levels_df[display_cols].to_string())
    return levels_df

def depth_to_purity(
    clusterer: hdbscan.HDBSCAN,
    y_binary: np.ndarray,
    y_type: np.ndarray,
    purity_threshold: float = 0.90,
) -> dict:
    """
    For each anomaly point, find how many tree levels deep it needs to go
    before it sits in a node that is >= purity_threshold anomalies of one type.
    Returns summary statistics.
    """
    tree_df  = clusterer.condensed_tree_.to_pandas()
    n_points = len(y_binary)

    # Build parent lookup for fast traversal
    child_to_parent = dict(zip(tree_df["child"], tree_df["parent"]))

    anom_indices = np.where(y_binary == 1)[0]
    depths = []

    for pt in anom_indices:
        node   = pt
        depth  = 0
        found  = False
        while node in child_to_parent:
            node  = child_to_parent[node]
            depth += 1
            pts   = np.array(get_leaves_under_node(node, tree_df, n_points, max_depth=30))
            pts   = pts[pts < n_points]
            if len(pts) == 0:
                break
            anom_pts = pts[y_binary[pts] == 1]
            if len(anom_pts) / len(pts) >= purity_threshold:
                found = True
                break
        depths.append(depth if found else -1)

    depths = np.array(depths)
    found_mask = depths >= 0
    return {
        "purity_threshold":        purity_threshold,
        "pct_anomalies_reach_purity": round(float(found_mask.mean()), 3),
        "mean_depth_to_purity":    round(float(depths[found_mask].mean()), 2) if found_mask.any() else None,
        "median_depth_to_purity":  round(float(np.median(depths[found_mask])), 2) if found_mask.any() else None,
        "max_depth_to_purity":     int(depths[found_mask].max()) if found_mask.any() else None,
    }


# ── Main evaluator class ───────────────────────────────────────────────────────

class ClusterEvaluator:
    """
    End-to-end clustering evaluator for HDFS embedding DataFrames.

    Parameters
    ----------
    occ_df : pd.DataFrame
        Full occurrence DataFrame with BlockId, Label, Type columns.
    templates_path : str
        Path to HDFS.log_templates.csv
    params : dict, optional
        HDBSCAN parameters. Defaults to BEST_PARAMS from RCA sweep.
    """

    def __init__(
        self,
        occ_df: pd.DataFrame,
        templates_path: str,
        params: dict | None = None,
    ):
        self.occ_df    = occ_df
        self.params    = params or BEST_PARAMS
        self.le        = build_label_encoder(occ_df)
        self.encoded_to_label = build_encoded_to_label(self.le, templates_path)
        self.clusterer = None

    def evaluate(
        self,
        embeddings_df: pd.DataFrame,
        run_tree_eval: bool = True,
        run_depth_eval: bool = True,
        depth_purity_threshold: float = 0.90,
        tree_min_node_size: int = 10,
    ) -> dict:
        """
        Run full evaluation pipeline on an embeddings DataFrame.

        Parameters
        ----------
        embeddings_df : pd.DataFrame
            Must contain 'BlockId' + N numeric feature columns.
        run_tree_eval : bool
            Whether to evaluate the condensed tree hierarchy.
        run_depth_eval : bool
            Whether to compute depth-to-purity per anomaly (can be slow).
        depth_purity_threshold : float
            Purity threshold used for depth_to_purity metric.
        tree_min_node_size : int
            Minimum node size to include in tree evaluation.

        Returns
        -------
        dict with keys: flat_metrics, frag_by_type, tree_df, depth_stats
        """
        # Extract feature matrix
        feature_cols = [c for c in embeddings_df.columns if c != "BlockId"]
        X = embeddings_df[feature_cols].values.astype(np.float32)

        import umap

        # After loading X, before clustering
        if X.shape[1] > 50:  # only reduce if high dimensional
            print(f"Reducing {X.shape[1]}d embeddings to 30d with UMAP...")
            reducer = umap.UMAP(
                n_components=50,
                n_neighbors=30,
                min_dist=0.0,
                metric="cosine",      # cosine is correct for sentence embeddings
                random_state=42,
                low_memory=True,
            )
            X = reducer.fit_transform(X)
            print(f"UMAP done — shape {X.shape}")

        # Attach labels
        y_binary, y_type = attach_labels(embeddings_df, self.occ_df, self.le)

        # Fit HDBSCAN
        print(f"Fitting HDBSCAN on {X.shape[0]:,} points x {X.shape[1]} features...")
        self.clusterer = hdbscan.HDBSCAN(**self.params)
        labels = self.clusterer.fit_predict(X)
        print(f"Done — {len(set(labels)) - (1 if -1 in labels else 0)} clusters found.")

        # Flat metrics
        flat = evaluate_flat(y_binary, y_type, labels)
        flat["rca_score"] = round(rca_score(flat), 4)

        # Per-type fragmentation
        frag_df = fragmentation_by_type(y_type, labels, self.encoded_to_label)

        # Tree hierarchy
        tree_result = None
        if run_tree_eval:
            print("Evaluating tree hierarchy...")
            tree_result_1 = evaluate_tree_hierarchy(
                self.clusterer, y_binary, y_type,
                self.encoded_to_label, min_node_size=tree_min_node_size,
            )

            tree_result_2 = evaluate_tree_levels(
                self.clusterer, y_binary, y_type,
                cut_levels=[0.02, 0.05, 0.1, 0.2, 0.3,0.4,0.5, 0.6, 0.7, 0.8, 0.9 ,1.0],
                min_cluster_size=10,
            )

        # Depth to purity
        depth_result = None
        if run_depth_eval:
            print(f"Computing depth-to-purity (threshold={depth_purity_threshold})...")
            depth_result = depth_to_purity(
                self.clusterer, y_binary, y_type, depth_purity_threshold
            )

        return {
            "flat_metrics": flat,
            "frag_by_type": frag_df,
            "tree_df_heir":      tree_result_1,
            "tree_df_levels":    tree_result_2,
            "depth_stats":  depth_result,
            "cluster_labels": labels,
            "y_binary":     y_binary,
            "y_type":       y_type,
        }

    def print_report(self, results: dict) -> None:
        """Pretty-print the full evaluation report."""
        flat = results["flat_metrics"]

        print("\n" + "=" * 65)
        print("FLAT CLUSTER METRICS")
        print("=" * 65)
        _print_section("Cluster structure", {
            "n_clusters":   flat["n_clusters"],
            "noise_frac":   flat["noise_frac"],
            "rca_score":    flat["rca_score"],
        })
        _print_section("Purity", {
            "purity_binary": flat["purity_binary"],
            "purity_type":   flat["purity_type"],
        })
        _print_section("Anomaly recall", {
            "recall @ 50% threshold": flat["anomaly_recall_50"],
            "recall @ 75% threshold": flat["anomaly_recall_75"],
            "recall @ 90% threshold": flat["anomaly_recall_90"],
        })
        _print_section("Cluster composition", {
            "mean_cluster_anomaly_frac": flat["mean_cluster_anomaly_frac"],
            "pct_clusters_pure90":       flat["pct_clusters_pure90"],
            "pct_clusters_pure75":       flat["pct_clusters_pure75"],
            "pct_clusters_mixed":        flat["pct_clusters_mixed"],
            "pct_clusters_normal":       flat["pct_clusters_normal"],
        })
        _print_section("Type alignment", {
            "homogeneity":   flat["homogeneity"],
            "completeness":  flat["completeness"],
            "v_measure":     flat["v_measure"],
            "ari_type":      flat["ari_type"],
            "nmi_type":      flat["nmi_type"],
        })
        _print_section("Fragmentation", {
            "mean_type_fragmentation": flat["mean_type_fragmentation"],
            "max_type_fragmentation":  flat["max_type_fragmentation"],
        })

        print("\n" + "=" * 65)
        print("FRAGMENTATION BY FAILURE TYPE (top 10)")
        print("=" * 65)
        print(results["frag_by_type"].head(10).to_string(index=False))

        if results["tree_df_heir"] is not None:
            print("\n" + "=" * 65)
            print("TREE HIERARCHY — top 15 nodes by size")
            print("=" * 65)
            print(results["tree_df_heir"].head(15).to_string(index=False))

        if results["tree_df_levels"] is not None:
            print("\n" + "=" * 65)
            print("TREE LEVELS")
            print("=" * 65)
            print(results["tree_df_levels"].to_string())

        if results["depth_stats"] is not None:
            print("\n" + "=" * 65)
            print("DEPTH-TO-PURITY")
            print("=" * 65)
            for k, v in results["depth_stats"].items():
                print(f"  {k:<40} {v}")

        print()


def _print_section(title: str, metrics: dict) -> None:
    print(f"\n  {title}:")
    for k, v in metrics.items():
        print(f"    {k:<40} {v}")