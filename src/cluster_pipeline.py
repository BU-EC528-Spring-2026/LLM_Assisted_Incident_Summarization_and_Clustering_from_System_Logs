"""
cluster_pipeline.py
────────────────────────────────────────────────────────────────────────────────
Clustering stage in the HDFS RCA pipeline.

Data flow:
    consumer.py  →  incidents.ndjson
                          │
                          ▼
                 cluster_pipeline.py       ← this file
                          │
                          ▼
                 clusters_output.json      → LLM summariser

What this does:
    1. Reads incidents.ndjson produced by consumer.py
    2. Embeds each incident's log messages (mean-pool per block)
    3. Clusters with HDBSCAN (best params from RCA sweep)
    4. Outputs one JSON record per cluster, structured for LLM summarisation

Usage:
    python cluster_pipeline.py \\
        --incidents incidents.ndjson \\
        --occ       ../data/preprocessed/Event_occurrence_matrix.csv \\
        --templates ../data/preprocessed/HDFS.log_templates.csv \\
        --out       clusters_output.json \\
        --model     sentence-transformers/all-MiniLM-L6-v2
"""

import argparse
import json
import logging
import time
from collections import Counter
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import hdbscan
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import LabelEncoder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("cluster_pipeline")


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

# Best parameters from RCA sweep (mcs=648, ms=20, eom beat everything at full scale)
HDBSCAN_PARAMS = {
    "min_cluster_size":         50,
    "min_samples":              20,
    "cluster_selection_method": "eom",
    "metric":                   "euclidean",
    "core_dist_n_jobs":         -1,
    "prediction_data":          True,
}

EMBEDDING_BATCH_SIZE = 256


# ══════════════════════════════════════════════════════════════════════════════
# 1.  LOAD INCIDENTS FROM CONSUMER OUTPUT
# ══════════════════════════════════════════════════════════════════════════════

def load_incidents(ndjson_path: str) -> list[dict]:
    """
    Read incidents.ndjson — one JSON object per line as written by consumer.py.
    Each incident has: block_id, logs (list of log dicts), severity,
    components, anomaly_label, start_time, end_time, duration_seconds.
    """
    incidents = []
    with open(ndjson_path) as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                incidents.append(json.loads(line))
            except json.JSONDecodeError as e:
                log.warning("Skipping malformed line %d: %s", i, e)

    log.info("Loaded %d incidents from %s", len(incidents), ndjson_path)
    return incidents


# ══════════════════════════════════════════════════════════════════════════════
# 2.  EMBED INCIDENTS
#     Embed all log messages in an incident, mean-pool → one vector per block.
#     Only the message field is embedded — timestamp/thread/level are noise.
# ══════════════════════════════════════════════════════════════════════════════

def embed_incidents(
    incidents: list[dict],
    model: SentenceTransformer,
    batch_size: int = EMBEDDING_BATCH_SIZE,
) -> tuple[np.ndarray, list[str]]:
    """
    Returns
    -------
    embeddings : np.ndarray  shape (n_incidents, d)
    block_ids  : list[str]   same order as embeddings rows
    """
    block_ids  = [inc["block_id"] for inc in incidents]

    # Flatten all messages, track which incident each belongs to
    all_messages:  list[str] = []
    msg_to_inc:    list[int] = []

    for inc_idx, inc in enumerate(incidents):
        messages = [entry.get("message", "") for entry in inc.get("logs", [])
                    if entry.get("message", "").strip()]
        if not messages:
            # Fallback: use severity + components as a minimal signal
            messages = [f"{inc.get('severity', 'INFO')} "
                        f"{' '.join(inc.get('components', []))}"]
        all_messages.extend(messages)
        msg_to_inc.extend([inc_idx] * len(messages))

    msg_to_inc = np.array(msg_to_inc)

    log.info("Encoding %d log messages for %d incidents...", len(all_messages), len(incidents))
    t0 = time.time()
    msg_embs = model.encode(
        all_messages,
        batch_size=batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
    )
    log.info("Encoding done in %.0fs — embedding dim=%d", time.time() - t0, msg_embs.shape[1])

    # Mean-pool per incident
    d = msg_embs.shape[1]
    inc_embs = np.zeros((len(incidents), d), dtype=np.float32)
    counts   = np.zeros(len(incidents),     dtype=np.int32)
    np.add.at(inc_embs, msg_to_inc, msg_embs)
    np.add.at(counts,   msg_to_inc, 1)
    counts = np.maximum(counts, 1)   # guard against zero-message incidents
    inc_embs /= counts[:, None]

    return inc_embs, block_ids


# ══════════════════════════════════════════════════════════════════════════════
# 3.  ATTACH GROUND-TRUTH LABELS (optional — only if occ_df is available)
#     Labels are attached AFTER clustering. Never used as clustering input.
# ══════════════════════════════════════════════════════════════════════════════

def attach_labels(
    block_ids: list[str],
    occ_df: Optional[pd.DataFrame],
) -> tuple[np.ndarray, np.ndarray, dict]:
    """
    Returns
    -------
    y_binary        : 1=Anomaly, 0=Normal (from anomaly_label in consumer output
                      or from occ_df as fallback)
    y_type          : encoded failure type int (or all-zero if unavailable)
    encoded_to_label: int -> human readable type string
    """
    if occ_df is None:
        log.warning("No occ_df provided — labels will be unavailable.")
        n = len(block_ids)
        return np.zeros(n, dtype=int), np.zeros(n, dtype=int), {0: "Unknown"}

    bid_to_row = occ_df.set_index("BlockId")
    labels_list, types_list = [], []

    for bid in block_ids:
        if bid in bid_to_row.index:
            row = bid_to_row.loc[bid]
            labels_list.append(1 if row["Label"] == "Fail" else 0)
            types_list.append(row["Type"] if pd.notna(row["Type"]) else -1)
        else:
            labels_list.append(0)
            types_list.append(-1)

    y_binary = np.array(labels_list, dtype=int)

    type_series = pd.Series(types_list).fillna(-1)
    le = LabelEncoder()
    y_type = le.fit_transform(type_series)

    encoded_to_label = {i: (str(v) if v != -1.0 else "Normal")
                        for i, v in enumerate(le.classes_)}

    log.info(
        "Labels attached — anomaly rate: %.1f%%  (%d/%d blocks matched occ_df)",
        y_binary.mean() * 100,
        sum(1 for b in block_ids if b in bid_to_row.index),
        len(block_ids),
    )
    return y_binary, y_type, encoded_to_label


# ══════════════════════════════════════════════════════════════════════════════
# 4.  CLUSTER
# ══════════════════════════════════════════════════════════════════════════════

def cluster(embeddings: np.ndarray, params: dict = HDBSCAN_PARAMS) -> tuple:
    """
    Returns (cluster_labels, fitted_clusterer).
    cluster_labels[i] == -1  →  noise (unclustered).
    """
    log.info(
        "Clustering %d incidents with HDBSCAN "
        "(mcs=%d, ms=%d, method=%s)...",
        len(embeddings),
        params["min_cluster_size"],
        params["min_samples"],
        params["cluster_selection_method"],
    )
    t0 = time.time()
    clusterer = hdbscan.HDBSCAN(**params)
    labels    = clusterer.fit_predict(embeddings)
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    noise_frac = (labels == -1).mean()
    log.info(
        "Clustering done in %.0fs — %d clusters, %.1f%% noise",
        time.time() - t0, n_clusters, noise_frac * 100,
    )
    return labels, clusterer


# ══════════════════════════════════════════════════════════════════════════════
# 5.  BUILD LLM-READY CLUSTER RECORDS
#     Each record contains everything an LLM needs to write a coherent
#     incident summary for that cluster.
# ══════════════════════════════════════════════════════════════════════════════

def _top_n(counter: Counter, n: int = 5) -> list[dict]:
    return [{"value": str(k), "count": int(v)}
            for k, v in counter.most_common(n)]


def build_cluster_records(
    cluster_labels:  np.ndarray,
    incidents:       list[dict],
    embeddings:      np.ndarray,
    y_binary:        np.ndarray,
    y_type:          np.ndarray,
    encoded_to_label: dict,
) -> list[dict]:
    """
    Build one output record per cluster (plus one noise record).

    Each record contains:
        cluster_id          int
        is_noise            bool
        n_incidents         int
        n_anomalies         int
        anomaly_frac        float
        dominant_type       str   — majority failure type label
        type_purity         float — fraction belonging to dominant type
        severity_dist       {level: count}
        top_components      [{value, count}]  — most common HDFS components
        time_range          {earliest, latest}
        representative_logs list  — up to 5 log messages most central to cluster
        embedding_centroid  list  — mean embedding vector (for similarity search)
        embedding_spread    float — mean cosine distance from centroid (compactness)
        block_ids           list  — all BlockIds in this cluster
        llm_context         str   — pre-formatted paragraph for the LLM prompt
    """
    unique_cids = sorted(set(cluster_labels))
    records = []

    for cid in unique_cids:
        mask = cluster_labels == cid
        inc_subset  = [inc for inc, m in zip(incidents, mask) if m]
        emb_subset  = embeddings[mask]
        ybin_subset = y_binary[mask]
        ytype_subset = y_type[mask]

        n   = len(inc_subset)
        n_anom = int(ybin_subset.sum())

        # ── Dominant failure type ─────────────────────────────────────────
        type_counts = Counter(ytype_subset.tolist())
        top_type, top_count = type_counts.most_common(1)[0]
        dominant_label = encoded_to_label.get(int(top_type), f"Type {top_type}")
        type_purity    = round(top_count / n, 3)

        # ── Severity & component distributions ────────────────────────────
        severity_counter  = Counter(inc.get("severity", "INFO") for inc in inc_subset)
        component_counter = Counter(
            comp
            for inc in inc_subset
            for comp in inc.get("components", [])
        )

        # ── Time range ────────────────────────────────────────────────────
        start_times = [inc.get("start_time", "") for inc in inc_subset if inc.get("start_time")]
        end_times   = [inc.get("end_time",   "") for inc in inc_subset if inc.get("end_time")]
        time_range  = {
            "earliest": min(start_times) if start_times else None,
            "latest":   max(end_times)   if end_times   else None,
        }

        # ── Representative logs (closest to centroid) ─────────────────────
        centroid = emb_subset.mean(axis=0)
        # Cosine similarity to centroid
        norms = np.linalg.norm(emb_subset, axis=1, keepdims=True)
        norms = np.maximum(norms, 1e-9)
        normed = emb_subset / norms
        cent_norm = centroid / (np.linalg.norm(centroid) + 1e-9)
        similarities = normed @ cent_norm

        # Pick top-5 most central incidents, then take their first log message
        top_k = min(5, n)
        central_idx = np.argsort(-similarities)[:top_k]
        representative_logs = []
        for idx in central_idx:
            inc = inc_subset[idx]
            logs = inc.get("logs", [])
            if logs:
                representative_logs.append({
                    "block_id":  inc["block_id"],
                    "severity":  inc.get("severity", ""),
                    "message":   logs[0].get("message", ""),
                    "timestamp": logs[0].get("timestamp", ""),
                })

        # ── Embedding compactness (mean cosine distance from centroid) ────
        cos_distances  = 1 - similarities
        embedding_spread = round(float(cos_distances.mean()), 4)

        # ── Pre-formatted LLM context paragraph ───────────────────────────
        if cid == -1:
            llm_context = (
                f"This group contains {n} unclustered (noise) incidents that did not "
                f"fit clearly into any cluster. They may represent rare or ambiguous "
                f"failure patterns. {n_anom} of them are labeled anomalous."
            )
        else:
            sev_summary = ", ".join(
                f"{v}x {k}" for k, v in severity_counter.most_common(3)
            )
            comp_summary = ", ".join(
                item["value"] for item in _top_n(component_counter, 5)
            )
            sample_msg = (
                representative_logs[0]["message"][:200]
                if representative_logs else "N/A"
            )
            llm_context = (
                f"Cluster {cid} contains {n} incidents, {n_anom} of which are "
                f"anomalous ({anomaly_frac:.0%}). "
                f"The dominant failure type is '{dominant_label}' "
                f"({type_purity:.0%} of incidents). "
                f"Severity breakdown: {sev_summary}. "
                f"Most active HDFS components: {comp_summary}. "
                f"Time range: {time_range['earliest']} to {time_range['latest']}. "
                f"These incidents are tightly grouped (embedding spread={embedding_spread:.3f}), "
                f"suggesting a consistent underlying cause. "
                f"Sample log message: \"{sample_msg}\""
            )

        anomaly_frac = round(n_anom / n, 3)

        records.append({
            "cluster_id":         int(cid),
            "is_noise":           cid == -1,
            "n_incidents":        n,
            "n_anomalies":        n_anom,
            "anomaly_frac":       anomaly_frac,
            "dominant_type":      dominant_label,
            "type_purity":        type_purity,
            "severity_dist":      dict(severity_counter),
            "top_components":     _top_n(component_counter, 5),
            "time_range":         time_range,
            "representative_logs": representative_logs,
            "embedding_centroid": centroid.tolist(),
            "embedding_spread":   embedding_spread,
            "block_ids":          [inc["block_id"] for inc in inc_subset],
            "llm_context":        llm_context,
        })

    # Sort: anomaly clusters first (by anomaly_frac desc), noise last
    records.sort(key=lambda r: (r["is_noise"], -r["anomaly_frac"]))
    return records


# ══════════════════════════════════════════════════════════════════════════════
# 6.  LOAD HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def load_occ_df(occ_path: Optional[str]) -> Optional[pd.DataFrame]:
    if not occ_path:
        return None
    p = Path(occ_path)
    if not p.exists():
        log.warning("occ file not found at %s — proceeding without labels", p)
        return None
    df = pd.read_csv(p)
    df["BlockId"] = df["BlockId"].astype(str)
    log.info("Loaded occ_df: %d rows", len(df))
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 7.  MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline(
    incidents_path: str,
    out_path:       str,
    model_name:     str,
    occ_path:       Optional[str] = None,
    hdbscan_params: dict          = HDBSCAN_PARAMS,
    batch_size:     int           = EMBEDDING_BATCH_SIZE,
) -> list[dict]:

    log.info("═" * 65)
    log.info("HDFS Cluster Pipeline")
    log.info("  incidents : %s", incidents_path)
    log.info("  model     : %s", model_name)
    log.info("  output    : %s", out_path)
    log.info("═" * 65)

    # 1. Load incidents
    incidents = load_incidents(incidents_path)
    if not incidents:
        log.error("No incidents loaded — exiting.")
        return []

    # 2. Embed
    log.info("Loading embedding model: %s", model_name)
    model = SentenceTransformer(model_name)
    embeddings, block_ids = embed_incidents(incidents, model, batch_size)
    del model  # free memory

    # 3. Labels (post-clustering — no leakage)
    occ_df = load_occ_df(occ_path)
    y_binary, y_type, encoded_to_label = attach_labels(block_ids, occ_df)

    # 4. Cluster
    labels, clusterer = cluster(embeddings, hdbscan_params)

    # 5. Build output records
    log.info("Building cluster records...")
    records = build_cluster_records(
        labels, incidents, embeddings,
        y_binary, y_type, encoded_to_label,
    )

    # 6. Write output
    out_p = Path(out_path)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    with open(out_p, "w") as f:
        json.dump(records, f, indent=2, default=str)

    n_real = sum(1 for r in records if not r["is_noise"])
    log.info(
        "Wrote %d cluster records (%d real + 1 noise) → %s",
        len(records), n_real, out_p,
    )

    # 7. Summary to console
    _print_summary(records)
    return records


def _print_summary(records: list[dict]) -> None:
    real = [r for r in records if not r["is_noise"]]
    noise = next((r for r in records if r["is_noise"]), None)

    print("\n" + "═" * 65)
    print("CLUSTER SUMMARY")
    print("═" * 65)
    print(f"  Total clusters (excl. noise) : {len(real)}")
    if noise:
        print(f"  Noise incidents              : {noise['n_incidents']}")

    anom_clusters = [r for r in real if r["anomaly_frac"] >= 0.90]
    print(f"  Anomaly-dominant clusters    : {len(anom_clusters)}  (≥90% anomaly)")

    print(f"\n  {'CID':>4}  {'N':>6}  {'Anom%':>6}  {'Purity':>6}  Dominant type")
    print(f"  {'─'*4}  {'─'*6}  {'─'*6}  {'─'*6}  {'─'*35}")
    for r in real[:20]:
        print(
            f"  {r['cluster_id']:>4}  {r['n_incidents']:>6}  "
            f"{r['anomaly_frac']:>5.1%}  {r['type_purity']:>6.3f}  "
            f"{r['dominant_type'][:35]}"
        )
    if len(real) > 20:
        print(f"  ... and {len(real) - 20} more clusters")
    print()


# ══════════════════════════════════════════════════════════════════════════════
# 8.  CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description="HDFS clustering pipeline — embeds incidents and clusters for LLM RCA"
    )
    ap.add_argument("--incidents",  required=True,
                    help="Path to incidents.ndjson from consumer.py")
    ap.add_argument("--out",        default="clusters_output.json",
                    help="Output JSON path")
    ap.add_argument("--model",      default="sentence-transformers/all-MiniLM-L6-v2",
                    help="Sentence transformer model name")
    ap.add_argument("--occ",        default=None,
                    help="Path to Event_occurrence_matrix.csv (for ground-truth labels)")
    ap.add_argument("--mcs",        type=int,   default=HDBSCAN_PARAMS["min_cluster_size"],
                    help="HDBSCAN min_cluster_size")
    ap.add_argument("--ms",         type=int,   default=HDBSCAN_PARAMS["min_samples"],
                    help="HDBSCAN min_samples")
    ap.add_argument("--batch-size", type=int,   default=EMBEDDING_BATCH_SIZE,
                    help="Embedding batch size")
    args = ap.parse_args()

    params = {**HDBSCAN_PARAMS,
              "min_cluster_size": args.mcs,
              "min_samples":      args.ms}

    run_pipeline(
        incidents_path=args.incidents,
        out_path=args.out,
        model_name=args.model,
        occ_path=args.occ,
        hdbscan_params=params,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()