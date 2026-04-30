"""
cluster_pipeline.py
────────────────────────────────────────────────────────────────────────────────
Clustering + anomaly-detection stage in the HDFS RCA pipeline.

Data flow:
    consumer.py  →  incidents.ndjson
                          │
                          ▼
                 cluster_pipeline.py       ← this file
                          │
                          ▼
                 Three outputs:
                   1. embeddings.csv      (block_id + 768-dim embedding)
                   2. clusters.csv        (block_id + cluster_id + is_anomaly + anomaly_score)
                   3. summaries.csv       (block_id + llm_summary)

What this does:
    1. Reads incidents.ndjson produced by consumer.py
    2. Extracts raw log lines per block, sends to gpt-4o-mini for summarization
    3. Embeds summaries with nomic-ai/nomic-embed-text-v1.5
    4. Clusters embeddings with HDBSCAN
    5. Scores embeddings with a pre-trained LOF for anomaly detection
    6. Outputs three CSVs for downstream LLM analysis and the UI

Usage:
    python cluster_pipeline.py \\
        --incidents incidents.ndjson \\
        --out-dir   ./pipeline_output \\
        --lof-model ./training_output/lof_hdfs.joblib \\
        --openai-key sk-... \\
        --model     nomic-ai/nomic-embed-text-v1.5
"""

import argparse
import json
import logging
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
import os
import numpy as np
import pandas as pd
import hdbscan
from openai import OpenAI
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
import concurrent.futures as cf
import threading

from lof_inference import score_anomalies_with_lof

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("cluster_pipeline")


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

HDBSCAN_PARAMS = {
    "min_cluster_size":         50,
    "min_samples":              20,
    "cluster_selection_method": "eom",
    "metric":                   "euclidean",
    "core_dist_n_jobs":         -1,
    "prediction_data":          True,
}

EMBEDDING_BATCH_SIZE = 256

# Defaults for parallel summarization. Tier 1 caps for gpt-4o-mini are 500 RPM
# and 200k TPM; at ~1500 tok/call the binding constraint is TPM, which works
# out to ~130 RPM sustained. 120 RPM with 8 workers stays comfortably under
# both. Override via --max-workers / --max-rpm.
SUMMARIZE_MAX_WORKERS  = 8
SUMMARIZE_MAX_RPM      = 120
SUMMARIZE_TIMEOUT_SECS = 30

# Default LOF bundle location — produced by the training notebook
DEFAULT_LOF_MODEL = "./training_output/lof_hdfs.joblib"

# Default anomaly_label.csv path — used only when --demo-balance is passed
DEFAULT_LABEL_CSV = "./data/hdfs/anomaly_label.csv"

SUMMARIZE_PROMPT = """You are analyzing HDFS (Hadoop Distributed File System) log lines for a single data block.

Summarize what happened to this block. Write as much detail as needed to capture everything notable — a healthy block with routine operations may need only 1-2 sentences, but a block with errors or unusual behavior should get a thorough description.

Focus on:
- What specific errors, exceptions, or warnings appeared (e.g. PacketResponder exceptions, pipeline failures, missing block errors, replication timeouts)
- How many times operations were retried or failed
- Whether the block ended up in an inconsistent state (under-replicated, corrupted, lost)
- Any unusual patterns: repeated failures, cascading errors, or partial completions

For healthy blocks, briefly state what operations completed normally.

Do NOT mention timestamps, IP addresses, or datanode hostnames.
Do NOT use generic phrases like "various operations were performed" or "several errors occurred".
Name the specific error types and outcomes.
Respond with only the summary, no preamble."""


# ══════════════════════════════════════════════════════════════════════════════
# 1.  LOAD INCIDENTS
# ══════════════════════════════════════════════════════════════════════════════

def load_incidents(ndjson_path: str) -> list[dict]:
    """Read incidents.ndjson — one JSON object per line from consumer.py."""
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
# 1b.  DEMO-BALANCE SUBSETTING (optional, opt-in via --demo-balance)
# ══════════════════════════════════════════════════════════════════════════════

def filter_incidents_for_demo(
    incidents:     list[dict],
    label_csv:     str,
    demo_size:     int,
    healthy_frac:  float = 0.60,
    seed:          int   = 42,
) -> list[dict]:
    """
    Filter incidents to a balanced subset using ground-truth labels from
    anomaly_label.csv. Used for producing watchable demo runs without
    summarizing all 6000+ blocks.

    Parameters
    ----------
    incidents     : full list from load_incidents()
    label_csv     : path to anomaly_label.csv (BlockId, Label, Type)
    demo_size     : target total number of incidents (e.g. 150)
    healthy_frac  : fraction that should be healthy ("Success" label)
    seed          : reproducibility for the random sample within each bucket

    Returns
    -------
    A filtered list of incidents (same dict structure, just fewer of them).
    """
    if not (0.0 < healthy_frac < 1.0):
        raise ValueError(f"healthy_frac must be in (0, 1), got {healthy_frac}")

    log.info("Demo-balance: target %d blocks (%.0f%% healthy / %.0f%% anomalous)",
             demo_size, healthy_frac * 100, (1 - healthy_frac) * 100)

    # Load ground-truth labels
    labels = pd.read_csv(label_csv, usecols=["BlockId", "Label"])
    labels["BlockId"] = labels["BlockId"].astype(str)
    label_map = dict(zip(labels["BlockId"], labels["Label"]))
    log.info("  loaded %d labels from %s", len(label_map), label_csv)

    # Bucket incidents by ground-truth label. We only consider incidents whose
    # block_id is in the labels file — anything else is skipped (we can't
    # honestly call it "healthy" or "anomalous" without a label).
    healthy, anomalous, unlabeled = [], [], []
    for inc in incidents:
        bid = str(inc.get("block_id", ""))
        if bid not in label_map:
            unlabeled.append(inc)
        elif label_map[bid] in ("Normal", "Success"):
            healthy.append(inc)
        else:                            # "Fail" or any non-Success label
            anomalous.append(inc)

    log.info("  available pool — healthy: %d, anomalous: %d, unlabeled: %d",
             len(healthy), len(anomalous), len(unlabeled))

    target_healthy = int(round(demo_size * healthy_frac))
    target_anom    = demo_size - target_healthy

    rng = np.random.default_rng(seed)

    # Sample within each bucket, falling back to "use what we've got" if a
    # bucket is short. This keeps the demo runnable on small ingestion pools.
    n_h = min(target_healthy, len(healthy))
    n_a = min(target_anom,    len(anomalous))
    if n_h < target_healthy:
        log.warning("  only %d healthy blocks available (asked for %d)",
                    n_h, target_healthy)
    if n_a < target_anom:
        log.warning("  only %d anomalous blocks available (asked for %d)",
                    n_a, target_anom)

    h_idx = rng.choice(len(healthy),   size=n_h, replace=False) if n_h > 0 else []
    a_idx = rng.choice(len(anomalous), size=n_a, replace=False) if n_a > 0 else []
    picked = [healthy[i] for i in h_idx] + [anomalous[i] for i in a_idx]

    # Shuffle so anomalies don't all appear at the end of the run
    rng.shuffle(picked)

    log.info("  demo set: %d healthy + %d anomalous = %d total",
             n_h, n_a, len(picked))
    return picked


# ══════════════════════════════════════════════════════════════════════════════
# 2.  GROUP RAW LOG LINES BY BLOCK ID
# ══════════════════════════════════════════════════════════════════════════════

def group_logs_by_block(incidents: list[dict]) -> dict[str, list[str]]:
    block_lines: dict[str, list[str]] = {}

    for inc in incidents:
        # Check both group_key and block_id in case the schema changed 
        bid = inc.get("block_id") or inc.get("group_key")
        if not bid:
            continue
        if bid not in block_lines:
            block_lines[bid] = []
        for entry in inc.get("logs", []):
            raw = entry.get("raw_line") or entry.get("message", "")
            if raw.strip():
                block_lines[bid].append(raw.strip())

    log.info("Grouped logs for %d unique blocks", len(block_lines))

    line_counts = [len(v) for v in block_lines.values()]
    if line_counts:
        log.info(
            "Lines per block — min: %d, median: %d, mean: %.1f, max: %d",
            min(line_counts),
            int(np.median(line_counts)),
            np.mean(line_counts),
            max(line_counts),
        )
    return block_lines



# ══════════════════════════════════════════════════════════════════════════════
# 3.  LLM SUMMARIZATION (parallel via ThreadPoolExecutor)
# ══════════════════════════════════════════════════════════════════════════════

class _RateLimiter:
    """
    Sliding-window RPM limiter for a thread pool.

    Each call to acquire() either returns immediately if we're under the cap,
    or sleeps just long enough for the oldest request in the window to roll
    out. Thread-safe.
    """

    def __init__(self, max_rpm: int):
        self.max_rpm = max_rpm
        self.window  = 60.0
        self.lock    = threading.Lock()
        self.calls   = deque()

    def acquire(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                while self.calls and self.calls[0] < now - self.window:
                    self.calls.popleft()
                if len(self.calls) < self.max_rpm:
                    self.calls.append(now)
                    return
                wait = self.window - (now - self.calls[0]) + 0.05
            time.sleep(wait)


def summarize_block(client: OpenAI, block_id: str, lines: list[str]) -> str:
    """Single-block summarization. Used by the parallel worker pool."""
    if len(lines) > 150:
        lines = lines[:60] + ["... [truncated middle section] ..."] + lines[-60:]
    log_text = "\n".join(lines)
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": SUMMARIZE_PROMPT},
            {"role": "user",   "content": f"Block ID: {block_id}\n\nLog lines:\n{log_text}"},
        ],
        max_tokens=400,
        temperature=0.2,
        timeout=SUMMARIZE_TIMEOUT_SECS,
    )
    return response.choices[0].message.content.strip()


def _summarize_one(
    client:    OpenAI,
    block_id:  str,
    lines:     list[str],
    limiter:   _RateLimiter,
) -> tuple[str, str]:
    """Worker function: rate-limit, summarize, return (block_id, summary).

    Returns the sentinel '[SUMMARIZATION_FAILED]' on any exception so the
    caller can detect failures without separate error channels.
    """
    try:
        limiter.acquire()
        return block_id, summarize_block(client, block_id, lines)
    except Exception as e:
        log.warning("Summarization failed for %s: %s: %s",
                    block_id, type(e).__name__, e)
        return block_id, "[SUMMARIZATION_FAILED]"


def summarize_all_blocks(
    client: OpenAI,
    block_lines: dict[str, list[str]],
    max_workers: int = 8,
    retry_attempts: int = 3,
    retry_sleep_s: float = 1.5,
    max_concurrent_requests: int = 8,
) -> dict[str, str]:
    """Summarize all blocks in parallel with request throttling.
    
    Rate limit for OpenAI T1 is 500 Requests per Minute, 10,000 requests per day; max_workers = 8 seems to work

    If RPM is reached, reduce max_workers

    If RPD is reached (occurs if the pipeline is run again after running to full), should wait a day before retrying
    """
    summaries: dict[str, str] = {}
    total = len(block_lines)
    done = 0
    t0 = time.time()
    
    # Semaphore to limit concurrent API requests
    request_semaphore = threading.Semaphore(max_concurrent_requests)

    log.info(
        "Summarizing %d blocks with gpt-4o-mini using %d workers (max %d concurrent requests)...",
        total, max_workers, max_concurrent_requests
    )

    def worker(item: tuple[str, list[str]]) -> tuple[str, str]:
        block_id, lines = item
        
        # Acquire semaphore — only max_concurrent_requests can make API calls at once
        with request_semaphore:
            for attempt in range(1, retry_attempts + 1):
                try:
                    return block_id, summarize_block(client, block_id, lines)
                except Exception as e:
                    if attempt == retry_attempts:
                        log.warning(
                            "Summarization failed for %s after %d attempts: %s",
                            block_id, retry_attempts, e
                        )
                        return block_id, "[SUMMARIZATION_FAILED]"
                    time.sleep(retry_sleep_s * attempt)
        return block_id, "[SUMMARIZATION_FAILED]"

    with cf.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(worker, item) for item in block_lines.items()]
        for fut in cf.as_completed(futures):
            block_id, summary = fut.result()
            summaries[block_id] = summary
            done += 1

            if done % 50 == 0 or done == total:
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0.0
                eta = (total - done) / rate if rate > 0 else 0.0
                log.info("  [%d/%d] %.1f blocks/sec, ETA %.0fs", done, total, rate, eta)

    failed = sum(1 for v in summaries.values() if v == "[SUMMARIZATION_FAILED]")
    log.info("Summarization done in %.0fs — %d succeeded, %d failed",
             time.time() - t0, total - failed, failed)
    return summaries


# ══════════════════════════════════════════════════════════════════════════════
# 4.  EMBED SUMMARIES
# ══════════════════════════════════════════════════════════════════════════════

def embed_summaries(
    block_ids: list[str],
    summaries: dict[str, str],
    model: SentenceTransformer,
    batch_size: int = EMBEDDING_BATCH_SIZE,
) -> tuple[np.ndarray, list[str]]:
    valid_ids = [
        bid for bid in block_ids
        if summaries.get(bid, "[SUMMARIZATION_FAILED]") != "[SUMMARIZATION_FAILED]"
    ]
    texts = [summaries[bid] for bid in valid_ids]
    if not texts:
        log.error("No valid summaries to embed!")
        return np.array([]), []

    log.info("Embedding %d summaries...", len(texts))
    t0 = time.time()
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )
    log.info("Embedding done in %.0fs — shape %s", time.time() - t0, embeddings.shape)
    return embeddings, valid_ids


# ══════════════════════════════════════════════════════════════════════════════
# 5.  CLUSTER
# ══════════════════════════════════════════════════════════════════════════════

def cluster(embeddings: np.ndarray, params: dict = HDBSCAN_PARAMS) -> np.ndarray:
    log.info(
        "Clustering %d blocks with HDBSCAN (mcs=%d, ms=%d, method=%s)...",
        len(embeddings),
        params["min_cluster_size"],
        params["min_samples"],
        params["cluster_selection_method"],
    )
    t0 = time.time()
    clusterer = hdbscan.HDBSCAN(**params)
    labels = clusterer.fit_predict(embeddings)
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    noise_frac = (labels == -1).mean()
    log.info("Clustering done in %.0fs — %d clusters, %.1f%% noise",
             time.time() - t0, n_clusters, noise_frac * 100)
    return labels


# ══════════════════════════════════════════════════════════════════════════════
# 6.  BUILD OUTPUT DATAFRAMES
# ══════════════════════════════════════════════════════════════════════════════

def build_outputs(
    valid_ids: list[str],
    embeddings: np.ndarray,
    cluster_labels: np.ndarray,
    summaries: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    emb_cols = [f"dim_{i}" for i in range(embeddings.shape[1])]
    df_embeddings = pd.DataFrame(embeddings, columns=emb_cols)
    df_embeddings.insert(0, "block_id", valid_ids)

    df_clusters = pd.DataFrame({
        "block_id":   valid_ids,
        "cluster_id": cluster_labels.astype(int),
    })

    df_summaries = pd.DataFrame({
        "block_id": valid_ids,
        "summary":  [summaries[bid] for bid in valid_ids],
    })
    return df_embeddings, df_clusters, df_summaries


# ══════════════════════════════════════════════════════════════════════════════
# 7.  MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline(
    incidents_path: str,
    out_dir: str,
    model_name: str = "nomic-ai/nomic-embed-text-v1.5",
    lof_model_path: str = DEFAULT_LOF_MODEL,
    skip_anomaly_detection: bool = False,
    demo_balance: Optional[int] = None,
    demo_label_csv: str = DEFAULT_LABEL_CSV,
    demo_healthy_frac: float = 0.60,
    summarize_workers: int = SUMMARIZE_MAX_WORKERS,
    summarize_rpm: int = SUMMARIZE_MAX_RPM,
    hdbscan_params: dict = HDBSCAN_PARAMS,
    batch_size: int = EMBEDDING_BATCH_SIZE,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    log.info("═" * 65)
    log.info("HDFS Cluster Pipeline")
    log.info("  incidents : %s", incidents_path)
    log.info("  embedding : %s", model_name)
    log.info("  lof model : %s", lof_model_path if not skip_anomaly_detection else "(disabled)")
    log.info("  output    : %s", out_dir)
    if demo_balance:
        log.info("  demo mode : %d blocks (%.0f%% healthy)",
                 demo_balance, demo_healthy_frac * 100)
    log.info("═" * 65)

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # 1. Load incidents
    incidents = load_incidents(incidents_path)
    if not incidents:
        log.error("No incidents loaded — exiting.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # 1b. Optional: filter to a balanced demo subset using ground-truth labels.
    # This MUST happen before summarization so we don't pay the LLM cost
    # on the full 6000+ blocks.
    if demo_balance is not None:
        incidents = filter_incidents_for_demo(
            incidents,
            label_csv    = demo_label_csv,
            demo_size    = demo_balance,
            healthy_frac = demo_healthy_frac,
        )
        if not incidents:
            log.error("Demo filter produced 0 incidents — exiting.")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # 2. Group raw logs by block
    block_lines = group_logs_by_block(incidents)

    # 3. Summarize with gpt-4o-mini (parallel)
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    summaries = summarize_all_blocks(
        client, block_lines,
        max_workers=summarize_workers,
    )

    # 4. Embed summaries
    log.info("Loading embedding model: %s", model_name)
    emb_model = SentenceTransformer(model_name, trust_remote_code=True)
    block_ids = list(block_lines.keys())
    embeddings, valid_ids = embed_summaries(block_ids, summaries, emb_model, batch_size)
    del emb_model

    if len(valid_ids) == 0:
        log.error("No valid embeddings — exiting.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # 5. Cluster
    labels = cluster(embeddings, hdbscan_params)

    # 6. Build output dataframes
    df_embeddings, df_clusters, df_summaries = build_outputs(
        valid_ids, embeddings, labels, summaries,
    )

    # 6b. Anomaly detection — score embeddings against the trained LOF
    if skip_anomaly_detection:
        log.info("Anomaly detection skipped (--skip-anomaly-detection)")
        df_clusters["is_anomaly"]    = False
        df_clusters["anomaly_score"] = np.nan
    else:
        try:
            df_clusters = score_anomalies_with_lof(
                df_embeddings, df_clusters, lof_model_path,
            )
        except FileNotFoundError as e:
            log.warning("%s", e)
            log.warning("Continuing without anomaly column. "
                        "Pass --skip-anomaly-detection to silence this.")
            df_clusters["is_anomaly"]    = False
            df_clusters["anomaly_score"] = np.nan

    # 7. Save
    emb_path = out_path / "embeddings.csv"
    cls_path = out_path / "clusters.csv"
    sum_path = out_path / "summaries.csv"

    df_embeddings.to_csv(emb_path, index=False)
    df_clusters.to_csv(cls_path, index=False)
    df_summaries.to_csv(sum_path, index=False)

    log.info("Saved outputs:")
    log.info("  embeddings : %s  (%d rows × %d cols)", emb_path, *df_embeddings.shape)
    log.info("  clusters   : %s  (%d rows)", cls_path, len(df_clusters))
    log.info("  summaries  : %s  (%d rows)", sum_path, len(df_summaries))

    # 8. Print summary
    _print_summary(df_clusters, labels)

    return df_embeddings, df_clusters, df_summaries


def _print_summary(df_clusters: pd.DataFrame, labels: np.ndarray) -> None:
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise = int((labels == -1).sum())

    print("\n" + "═" * 65)
    print("PIPELINE SUMMARY")
    print("═" * 65)
    print(f"  Total blocks processed : {len(labels)}")
    print(f"  Clusters found         : {n_clusters}")
    print(f"  Noise (unclustered)    : {n_noise} ({n_noise/len(labels)*100:.1f}%)")

    if "is_anomaly" in df_clusters.columns:
        n_anom = int(df_clusters["is_anomaly"].fillna(False).sum())
        print(f"  Anomalies flagged      : {n_anom} ({n_anom/len(df_clusters)*100:.1f}%)")

    vc = df_clusters["cluster_id"].value_counts().sort_index()
    print(f"\n  {'CID':>6}  {'Count':>6}  {'Anom':>6}")
    print(f"  {'─'*6}  {'─'*6}  {'─'*6}")
    for cid, count in vc.items():
        if "is_anomaly" in df_clusters.columns:
            anom_in_cid = int(
                df_clusters.loc[df_clusters["cluster_id"] == cid, "is_anomaly"]
                           .fillna(False).sum()
            )
        else:
            anom_in_cid = 0
        label = "noise" if cid == -1 else ""
        print(f"  {cid:>6}  {count:>6}  {anom_in_cid:>6}  {label}")
    print()


# ══════════════════════════════════════════════════════════════════════════════
# 8.  CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description="HDFS clustering + anomaly-detection pipeline"
    )
    ap.add_argument("--incidents",   required=True,
                    help="Path to incidents.ndjson from consumer.py")
    ap.add_argument("--out-dir",     default="./pipeline_output",
                    help="Output directory for CSVs")
    ap.add_argument("--model",       default="nomic-ai/nomic-embed-text-v1.5",
                    help="Sentence transformer model name")
    ap.add_argument("--lof-model",   default=DEFAULT_LOF_MODEL,
                    help="Path to trained LOF bundle (.joblib)")
    ap.add_argument("--skip-anomaly-detection", action="store_true",
                    help="Skip the anomaly-detection step (no LOF inference)")
    ap.add_argument("--demo-balance", type=int, default=None,
                    metavar="N",
                    help="Demo mode: subset to N blocks balanced by ground-truth label "
                         "(default split: 60%% healthy, 40%% anomalous). Requires "
                         "--demo-label-csv. Skips this filter if not set.")
    ap.add_argument("--demo-label-csv", default=DEFAULT_LABEL_CSV,
                    help="Path to anomaly_label.csv used for --demo-balance lookup")
    ap.add_argument("--demo-healthy-frac", type=float, default=0.60,
                    help="Fraction of demo set that should be healthy (default 0.60)")
    ap.add_argument("--mcs",         type=int, default=HDBSCAN_PARAMS["min_cluster_size"],
                    help="HDBSCAN min_cluster_size")
    ap.add_argument("--ms",          type=int, default=HDBSCAN_PARAMS["min_samples"],
                    help="HDBSCAN min_samples")
    ap.add_argument("--batch-size",  type=int, default=EMBEDDING_BATCH_SIZE,
                    help="Embedding batch size")
    ap.add_argument("--max-workers", type=int, default=SUMMARIZE_MAX_WORKERS,
                    help="Concurrent threads for LLM summarization (default 8). "
                         "Higher = faster, but watch TPM/RPM caps for your tier.")
    ap.add_argument("--max-rpm",     type=int, default=SUMMARIZE_MAX_RPM,
                    help="Sliding-window RPM cap for summarization (default 120). "
                         "Set below your tier's effective RPM ceiling. For Tier 1 "
                         "gpt-4o-mini, ~120 keeps you under the 200k TPM limit.")
    args = ap.parse_args()

    params = {
        **HDBSCAN_PARAMS,
        "min_cluster_size": args.mcs,
        "min_samples":      args.ms,
    }

    run_pipeline(
        incidents_path=args.incidents,
        out_dir=args.out_dir,
        model_name=args.model,
        lof_model_path=args.lof_model,
        skip_anomaly_detection=args.skip_anomaly_detection,
        demo_balance=args.demo_balance,
        demo_label_csv=args.demo_label_csv,
        demo_healthy_frac=args.demo_healthy_frac,
        summarize_workers=args.max_workers,
        summarize_rpm=args.max_rpm,
        hdbscan_params=params,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()