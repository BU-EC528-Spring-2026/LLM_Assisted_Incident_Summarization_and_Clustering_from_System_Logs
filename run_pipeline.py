"""
run_pipeline.py
────────────────────────────────────────────────────────────────────────────────
End-to-end orchestrator for the HDFS Incident RCA pipeline.

Stages (in order):
    1. Ingestion           → produce incidents.ndjson
    2. Block summarization → gpt-4o-mini per block
    3. Embedding           → nomic-embed-text-v1.5
    4. Clustering          → HDBSCAN
       (steps 2-4 are run together by src.cluster_pipeline.run_pipeline)
    5. Cluster summaries   → AnythingLLM (Summarizationllm)
    6. Dashboard           → streamlit run app.py

────────────────────────────────────────────────────────────────────────────────
Examples
────────────────────────────────────────────────────────────────────────────────
# Fastest end-to-end run using the parser ingestion path on a raw HDFS log
python run_pipeline.py --ingest parser --raw-log ingestion/data/HDFS_2k.log --out-dir pipeline_output

# Reuse an incidents.ndjson that's already on disk
python run_pipeline.py --ingest existing --incidents ingestion/incidents.ndjson --out-dir pipeline_output

# Use the Fluent Bit + consumer.py pipeline (requires fluent-bit installed)
python run_pipeline.py --ingest fluentbit --dataset hdfs --out-dir pipeline_output

# Skip optional stages
python run_pipeline.py --ingest existing --incidents ingestion/incidents.ndjson --skip-cluster-summary --skip-dashboard
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

# Make `src` importable when running from repo root
REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  [run_pipeline] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("run_pipeline")


# ══════════════════════════════════════════════════════════════════════════════
# Stage 1 — Ingestion
# ══════════════════════════════════════════════════════════════════════════════

def ingest_existing(incidents_path: Path) -> Path:
    if not incidents_path.exists():
        raise FileNotFoundError(f"--incidents file not found: {incidents_path}")
    log.info("Using existing incidents file: %s", incidents_path)
    return incidents_path


def ingest_parser(raw_log: Path, out_ndjson: Path, time_window_minutes: int = 5) -> Path:
    """
    Parse a raw HDFS log file directly with hdfs_parser + incident_grouper and
    write incidents.ndjson in the schema expected by cluster_pipeline.

    This avoids the Fluent Bit + HTTP consumer dance and is the simplest fully-
    automated ingestion path for HDFS logs.
    """
    from hdfs_parser import HDFSLogParser
    from incident_grouper import IncidentGrouper

    if not raw_log.exists():
        raise FileNotFoundError(f"--raw-log file not found: {raw_log}")

    log.info("Parsing raw HDFS log: %s", raw_log)
    parser = HDFSLogParser()
    parsed = parser.parse_file(str(raw_log))
    log.info("Parsed %d log lines", len(parsed))

    grouper = IncidentGrouper(time_window_minutes=time_window_minutes)
    incidents = grouper.group_incidents(parsed)

    out_ndjson.parent.mkdir(parents=True, exist_ok=True)
    with open(out_ndjson, "w") as f:
        for inc in incidents:
            f.write(json.dumps(inc) + "\n")
    log.info("Wrote %d incidents → %s", len(incidents), out_ndjson)
    return out_ndjson


def ingest_fluentbit(
    dataset: str,
    out_ndjson: Path,
    fluent_bit_conf: Path,
    consumer_path: Path,
    window_minutes: int = 5,
    fluentbit_cmd: str = "fluent-bit",
) -> Path:
    """
    Run consumer.py + fluent-bit as subprocesses to produce incidents.ndjson.

    Flow:
      1. Start consumer.py (HTTP server + grouper) in the background
      2. Start fluent-bit, wait for it to finish reading the file
      3. SIGINT the consumer so it flushes incidents.ndjson and exits
    """
    if not fluent_bit_conf.exists():
        raise FileNotFoundError(f"fluent-bit config not found: {fluent_bit_conf}")
    if not consumer_path.exists():
        raise FileNotFoundError(f"consumer.py not found: {consumer_path}")

    out_ndjson = out_ndjson.resolve()
    out_ndjson.parent.mkdir(parents=True, exist_ok=True)

    log.info("Starting consumer.py (dataset=%s) → %s", dataset, out_ndjson)
    consumer_proc = subprocess.Popen(
        [
            sys.executable, str(consumer_path),
            "--dataset", dataset,
            "--window", str(window_minutes),
            "--out", str(out_ndjson),
        ],
        cwd=str(consumer_path.parent),
    )

    # Give the HTTP server a moment to bind
    time.sleep(2.0)

    log.info("Starting fluent-bit with %s", fluent_bit_conf)
    try:
        fb_proc = subprocess.Popen(
            [fluentbit_cmd, "-c", str(fluent_bit_conf)],
            cwd=str(fluent_bit_conf.parent),
        )
    except FileNotFoundError as e:
        consumer_proc.send_signal(signal.SIGINT)
        consumer_proc.wait(timeout=15)
        raise RuntimeError(
            "fluent-bit binary not found on PATH. Install with `brew install fluent-bit` "
            "or use --ingest parser instead."
        ) from e

    # Give fluent-bit time to read the file and send batches to consumer
    log.info("Waiting for fluent-bit to read data...")
    time.sleep(5.0)
    
    # Flush consumer without waiting for fluent-bit to exit
    log.info("Flushing consumer...")
    time.sleep(2.0)
    consumer_proc.send_signal(signal.SIGINT)
    try:
        consumer_proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        log.warning("Consumer did not exit cleanly — terminating")
        consumer_proc.terminate()
        consumer_proc.wait(timeout=10)
    
    # Check fluent-bit status but don't wait for it
    if fb_proc.poll() is not None:
        log.info("fluent-bit exited (rc=%s)", fb_proc.returncode)
    else:
        log.info("fluent-bit still running in background")

    if not out_ndjson.exists():
        raise RuntimeError(f"Ingestion produced no output at {out_ndjson}")
    log.info("Ingestion complete → %s", out_ndjson)
    return out_ndjson


# ══════════════════════════════════════════════════════════════════════════════
# Stages 2-4 — Summarize, Embed, Cluster
# ══════════════════════════════════════════════════════════════════════════════

def run_cluster_pipeline(incidents_path: Path, out_dir: Path, model_name: str,
                         mcs: int, ms: int, batch_size: int,
                         lof_model_path: Path, skip_anomaly_detection: bool) -> None:
    from cluster_pipeline import run_pipeline, HDBSCAN_PARAMS

    params = {**HDBSCAN_PARAMS, "min_cluster_size": mcs, "min_samples": ms}
    run_pipeline(
        incidents_path=str(incidents_path),
        out_dir=str(out_dir),
        model_name=model_name,
        lof_model_path=str(lof_model_path),
        skip_anomaly_detection=skip_anomaly_detection,
        hdbscan_params=params,
        batch_size=batch_size,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Stage 5 — Cluster Summarization (AnythingLLM)
# ══════════════════════════════════════════════════════════════════════════════

def run_cluster_summaries(out_dir: Path) -> None:
    """
    Calls Summarizationllm helpers to produce cluster_summaries.csv inside out_dir.
    Requires a running AnythingLLM instance (default http://localhost:3001) and
    SUMM_API_KEY in the environment / .env.
    """
    import csv
    from Summarizationllm import load_clusters, load_summaries, analyze_cluster

    clusters_csv = out_dir / "clusters.csv"
    summaries_csv = out_dir / "summaries.csv"
    output_csv = out_dir / "cluster_summaries.csv"

    if not clusters_csv.exists() or not summaries_csv.exists():
        raise FileNotFoundError(
            f"Expected {clusters_csv} and {summaries_csv} from the cluster stage."
        )

    clusters = load_clusters(str(clusters_csv))
    summaries = load_summaries(str(summaries_csv))
    log.info("Loaded %d clusters, %d block summaries", len(clusters), len(summaries))

    with open(output_csv, "w", newline="", encoding="utf-8") as out_file:
        writer = csv.writer(out_file)
        writer.writerow(["cluster_id", "cluster_summary"])

        for cluster_id, block_ids in clusters.items():
            if str(cluster_id) == "-1":
                log.info("Skipping noise cluster (-1)")
                continue
            log.info("Summarizing cluster %s (%d blocks)...", cluster_id, len(block_ids))
            result = analyze_cluster(cluster_id, block_ids, summaries)
            writer.writerow([cluster_id, result if result is not None else ""])

    log.info("Cluster summaries → %s", output_csv)


# ══════════════════════════════════════════════════════════════════════════════
# Stage 6 — Dashboard
# ══════════════════════════════════════════════════════════════════════════════

def launch_dashboard(app_path: Path, out_dir: Path, port: int = 8501) -> None:
    if not app_path.exists():
        raise FileNotFoundError(f"app.py not found: {app_path}")
    log.info("Launching Streamlit dashboard on port %d (Ctrl-C to stop)...", port)
    env = os.environ.copy()
    # Surface the pipeline output dir to the dashboard via env var (sidebar default
    # still controls; this is informational).
    env["PIPELINE_OUTPUT_DIR"] = str(out_dir)
    cmd = [
        sys.executable, "-m", "streamlit", "run", str(app_path),
        "--server.port", str(port),
    ]
    subprocess.run(cmd, env=env, cwd=str(app_path.parent), check=False)


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="End-to-end pipeline runner: ingest → summarize → embed → cluster → cluster-summary → dashboard",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Ingestion
    p.add_argument("--ingest", choices=["existing", "parser", "fluentbit"], default="existing",
                   help="Ingestion mode (default: existing)")
    p.add_argument("--incidents", type=Path,
                   default=REPO_ROOT / "ingestion" / "incidents.ndjson",
                   help="Path to incidents.ndjson (input for 'existing', output for others)")
    p.add_argument("--raw-log", type=Path,
                   help="Raw HDFS log file (required for --ingest parser)")
    p.add_argument("--dataset", default="hdfs", choices=["hdfs", "thunderbird", "zookeeper"],
                   help="Dataset for --ingest fluentbit")
    p.add_argument("--window", type=int, default=5,
                   help="Incident time-window in minutes")
    p.add_argument("--fluent-bit-conf", type=Path,
                   default=REPO_ROOT / "ingestion" / "fluent-bit.conf")
    p.add_argument("--consumer", type=Path,
                   default=REPO_ROOT / "ingestion" / "consumer.py")
    p.add_argument("--fluentbit-cmd", default="fluent-bit",
                   help="fluent-bit executable name/path")

    # Cluster pipeline
    p.add_argument("--out-dir", type=Path, default=REPO_ROOT / "pipeline_output")
    p.add_argument("--model", default="nomic-ai/nomic-embed-text-v1.5")
    p.add_argument("--mcs", type=int, default=50, help="HDBSCAN min_cluster_size")
    p.add_argument("--ms", type=int, default=20, help="HDBSCAN min_samples")
    p.add_argument("--batch-size", type=int, default=256)

    # Anomaly detection (LOF)
    p.add_argument("--lof-model", type=Path,
                   default=REPO_ROOT / "src" / "lof_hdfs.joblib",
                   help="Path to trained LOF bundle (.joblib) used by lof_inference")
    p.add_argument("--skip-anomaly", action="store_true",
                   help="Skip LOF anomaly scoring inside the cluster pipeline")

    # Skip flags
    p.add_argument("--skip-ingest", action="store_true",
                   help="Skip ingestion entirely (assumes --incidents already exists)")
    p.add_argument("--skip-cluster", action="store_true",
                   help="Skip stages 2-4 (use existing CSVs in --out-dir)")
    p.add_argument("--skip-cluster-summary", action="store_true",
                   help="Skip AnythingLLM cluster summarization")
    p.add_argument("--skip-dashboard", action="store_true",
                   help="Don't launch Streamlit at the end")

    # Dashboard
    p.add_argument("--app", type=Path, default=REPO_ROOT / "app.py")
    p.add_argument("--port", type=int, default=8501)

    return p.parse_args()


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    t_start = time.time()
    log.info("═" * 70)
    log.info("Starting full pipeline")
    log.info("  out_dir : %s", args.out_dir)
    log.info("  ingest  : %s", "SKIP" if args.skip_ingest else args.ingest)
    log.info("═" * 70)

    # 1. Ingestion
    if args.skip_ingest:
        incidents_path = ingest_existing(args.incidents)
    elif args.ingest == "existing":
        incidents_path = ingest_existing(args.incidents)
    elif args.ingest == "parser":
        if not args.raw_log:
            log.error("--raw-log is required for --ingest parser")
            return 2
        incidents_path = ingest_parser(args.raw_log, args.incidents, args.window)
    elif args.ingest == "fluentbit":
        incidents_path = ingest_fluentbit(
            dataset=args.dataset,
            out_ndjson=args.incidents,
            fluent_bit_conf=args.fluent_bit_conf,
            consumer_path=args.consumer,
            window_minutes=args.window,
            fluentbit_cmd=args.fluentbit_cmd,
        )
    else:
        log.error("Unknown ingest mode: %s", args.ingest)
        return 2

    # 2-4. Summarize → Embed → Cluster
    if args.skip_cluster:
        log.info("Skipping cluster pipeline (stages 2-4)")
    else:
        if not os.getenv("OPENAI_API_KEY"):
            log.warning("OPENAI_API_KEY is not set — block summarization will fail.")
        if not args.skip_anomaly and not args.lof_model.exists():
            log.warning("LOF model not found at %s — disabling anomaly detection. "
                        "Train it via the LOF notebook or pass --lof-model <path>.",
                        args.lof_model)
            args.skip_anomaly = True
        run_cluster_pipeline(
            incidents_path=incidents_path,
            out_dir=args.out_dir,
            model_name=args.model,
            mcs=args.mcs,
            ms=args.ms,
            batch_size=args.batch_size,
            lof_model_path=args.lof_model,
            skip_anomaly_detection=args.skip_anomaly,
        )

    # 5. Cluster summaries
    if args.skip_cluster_summary:
        log.info("Skipping cluster summarization (stage 5)")
    else:
        try:
            run_cluster_summaries(args.out_dir)
        except Exception as e:
            log.error("Cluster summarization failed: %s", e)
            log.warning("Continuing without cluster_summaries.csv. "
                        "Check that AnythingLLM is running and SUMM_API_KEY is set.")

    log.info("Pipeline finished in %.1fs", time.time() - t_start)

    # 6. Dashboard
    if args.skip_dashboard:
        log.info("Skipping dashboard. Run manually: streamlit run %s", args.app)
    else:
        launch_dashboard(args.app, args.out_dir, args.port)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
