"""
lof_inference.py
────────────────────────────────────────────────────────────────────────────────
Inference-time anomaly scoring for cluster_pipeline.py using a fitted
LocalOutlierFactor model.

Loads a trained LOF + StandardScaler + threshold bundle saved by the
training notebook, scores embeddings the pipeline already produced, and
adds an `is_anomaly` boolean column for the UI.

Background
----------
On L2-normalized 768-dim Nomic embeddings of LLM-summarized HDFS log blocks,
LOF (k=30) substantially outperformed the Prodigy-style VAE port:

    Method              ROC-AUC   PR-AUC   F1
    Prodigy VAE          0.87      0.67    0.64
    LOF (k=30)           0.95      0.89    0.82

The VAE port is retained as the documented baseline; LOF is the deployed
detector in cluster_pipeline.py.
"""

from __future__ import annotations

import logging
from pathlib import Path

import joblib
import numpy as np
import pandas as pd


log = logging.getLogger("lof_inference")


def _extract_embedding_matrix(df_embeddings: pd.DataFrame) -> tuple[np.ndarray, list[str]]:
    """Pull the dim_* columns out as a (n, d) float32 matrix."""
    dim_cols = sorted(
        (c for c in df_embeddings.columns if c.startswith("dim_")),
        key=lambda c: int(c.split("_")[1]),
    )
    if not dim_cols:
        raise ValueError("No dim_* columns found in embeddings DataFrame.")
    X = df_embeddings[dim_cols].to_numpy(dtype=np.float32)
    block_ids = df_embeddings["block_id"].astype(str).tolist()
    return X, block_ids, len(dim_cols)


def score_anomalies_with_lof(
    df_embeddings: pd.DataFrame,
    df_clusters:   pd.DataFrame,
    model_path:    str | Path,
    threshold_override: float | None = None,
    anomaly_percentile: float | None = None,
) -> pd.DataFrame:
    """
    Score embeddings with a pre-trained LOF bundle.

    The bundle is a joblib-pickled dict containing:
        - 'lof'       : fitted sklearn.neighbors.LocalOutlierFactor (novelty=True)
        - 'scaler'    : fitted sklearn.preprocessing.StandardScaler
        - 'threshold' : float, F1-optimal anomaly score threshold
        - 'input_dim' : int, expected embedding dimensionality
        - 'metadata'  : dict, training-time hyperparams and metrics

    Parameters
    ----------
    df_embeddings       : block_id + dim_0 ... dim_N  (from cluster_pipeline)
    df_clusters         : block_id + cluster_id       (from cluster_pipeline)
    model_path          : path to the joblib bundle
    threshold_override  : if given, use this absolute LOF score threshold instead
                          of the bundle's baked-in F1-optimal value. Useful when
                          the live distribution has drifted from training.
    anomaly_percentile  : if given (e.g. 95), flag the top (100 - p)% of scores
                          as anomalous. Mutually exclusive with threshold_override.
                          Use this when you know neither the absolute scale nor
                          the true anomaly rate, but want a relative top-N flag.

    Returns
    -------
    df_clusters with two new columns:
        is_anomaly    : bool
        anomaly_score : float (higher = more anomalous; -lof.score_samples)
    """
    model_path = Path(model_path)
    if not model_path.exists():
        raise FileNotFoundError(
            f"LOF model not found at {model_path}. "
            f"Run the training notebook to produce it."
        )

    log.info("Loading LOF bundle from %s", model_path)
    bundle = joblib.load(model_path)

    lof       = bundle["lof"]
    scaler    = bundle["scaler"]
    bundled_threshold = bundle["threshold"]
    expected_dim = bundle["input_dim"]

    X, block_ids, dim = _extract_embedding_matrix(df_embeddings)

    # Guard against an embedding-dim mismatch — easy to silently break this if
    # the embedding model is changed in the pipeline but LOF wasn't retrained.
    if dim != expected_dim:
        raise ValueError(
            f"Embedding dim mismatch: pipeline produced {dim}-dim vectors "
            f"but trained LOF expects {expected_dim}. Retrain LOF on the "
            f"current embedding model."
        )

    X_scaled = scaler.transform(X)
    # LOF.score_samples returns higher values for INLIERS. Negate so higher = more anomalous.
    scores = -lof.score_samples(X_scaled)

    # Decide threshold: percentile > override > bundle
    if threshold_override is not None and anomaly_percentile is not None:
        raise ValueError("Pass only one of threshold_override or anomaly_percentile, not both.")

    if anomaly_percentile is not None:
        if not (0.0 < anomaly_percentile < 100.0):
            raise ValueError("anomaly_percentile must be in (0, 100).")
        threshold = float(np.percentile(scores, anomaly_percentile))
        log.info("Using percentile threshold p%.1f = %.4f "
                 "(bundle baseline = %.4f)",
                 anomaly_percentile, threshold, bundled_threshold)
    elif threshold_override is not None:
        threshold = float(threshold_override)
        log.info("Using threshold override = %.4f (bundle baseline = %.4f)",
                 threshold, bundled_threshold)
    else:
        threshold = bundled_threshold
        log.info("Using bundle threshold = %.4f", threshold)

    # Sanity check: warn if threshold sits outside the live score range —
    # this is the textbook "everything (or nothing) is flagged" symptom.
    s_min, s_max = float(scores.min()), float(scores.max())
    log.info("Score distribution: min=%.3f  median=%.3f  max=%.3f",
             s_min, float(np.median(scores)), s_max)
    if threshold < s_min:
        log.warning("Threshold (%.3f) is BELOW the minimum live score (%.3f) — "
                    "every block will be flagged as anomalous. Consider "
                    "--anomaly-percentile 95 or --anomaly-threshold <higher value>.",
                    threshold, s_min)
    elif threshold > s_max:
        log.warning("Threshold (%.3f) is ABOVE the maximum live score (%.3f) — "
                    "no blocks will be flagged.", threshold, s_max)

    preds = (scores > threshold).astype(bool)

    log.info("Flagged %d / %d blocks as anomalous (%.1f%%)",
             int(preds.sum()), len(preds), 100 * preds.mean())

    score_df = pd.DataFrame({
        "block_id":      block_ids,
        "is_anomaly":    preds,
        "anomaly_score": scores.astype(float),
    })

    out = df_clusters.copy()
    out["block_id"] = out["block_id"].astype(str)
    out = out.merge(score_df, on="block_id", how="left")
    return out