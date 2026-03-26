"""
embed_export.py
───────────────────────────────────────────────────────────────────────────────
Embed HDFS incidents from consumer.py output and export per-block
mean-pooled embeddings to CSV.

Reads incidents.ndjson (one incident JSON per line, as written by consumer.py).
Each incident's log messages are concatenated and embedded, then mean-pooled
into a single vector per BlockId.

Output CSV: BlockId, dim_0, dim_1, ..., dim_N

Usage:
    python embed_export.py --incidents incidents.ndjson
                           --out       ../results/embeddings
"""

import argparse
import gc
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer

try:
    import torch
    CUDA_AVAILABLE = torch.cuda.is_available()
except ImportError:
    CUDA_AVAILABLE = False

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


# ── Load incidents ─────────────────────────────────────────────────────────────

def load_incidents(ndjson_path: str) -> pd.DataFrame:
    """
    Read incidents.ndjson from consumer.py.
    Each line is one incident with block_id, logs[], anomaly_label, etc.
    Returns a DataFrame with one row per incident: BlockId + concatenated text.
    """
    print(f"Loading incidents from {ndjson_path}...")
    t0 = time.time()
    rows = []

    with open(ndjson_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                inc = json.loads(line)
            except json.JSONDecodeError:
                continue

            block_id = inc.get("block_id", "")
            if not block_id:
                continue

            # Concatenate all log messages in this incident into one text
            messages = [
                log.get("message", "")
                for log in inc.get("logs", [])
                if log.get("message", "").strip()
            ]
            text = " | ".join(messages) if messages else ""
            if not text:
                continue

            rows.append({
                "BlockId":       block_id,
                "text":          text,
                "num_logs":      inc.get("num_logs", len(messages)),
                "severity":      inc.get("severity", ""),
                "anomaly_label": inc.get("anomaly_label", "Unknown"),
            })

    df = pd.DataFrame(rows)
    print(f"  Loaded {len(df):,} incidents for {df['BlockId'].nunique():,} unique blocks "
          f"({time.time()-t0:.1f}s)")
    print(f"  Anomaly breakdown:\n{df['anomaly_label'].value_counts().to_string()}")
    return df


# ── Embed and export ───────────────────────────────────────────────────────────

def embed_and_export(
    incidents_df: pd.DataFrame,
    model_name:   str,
    out_dir:      Path,
    batch_size:   int,
) -> Path:
    safe_name = model_name.replace("/", "_")
    out_path  = out_dir / f"embeddings_{safe_name}.csv"

    if out_path.exists():
        print(f"Skipping {model_name} — {out_path} already exists")
        return out_path

    device = "cuda" if CUDA_AVAILABLE else "cpu"
    print(f"\nLoading {model_name} on {device}...")
    model = SentenceTransformer(model_name, device=device)

    # Group by BlockId — multiple incidents may share the same block_id,
    # collect all their texts and mean-pool at the block level
    groups    = incidents_df.groupby("BlockId")["text"].apply(list)
    block_ids = list(groups.index)

    all_texts:   list[str] = []
    text_to_blk: list[int] = []
    for blk_idx, texts in enumerate(groups):
        all_texts.extend(texts)
        text_to_blk.extend([blk_idx] * len(texts))
    text_to_blk = np.array(text_to_blk)

    print(f"  Encoding {len(all_texts):,} incident texts for {len(block_ids):,} blocks...")
    t0 = time.time()
    embeddings = model.encode(
        all_texts,
        batch_size=batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
        device=device,
    )
    print(f"  Encoding done ({time.time()-t0:.0f}s) — shape {embeddings.shape}")

    # Mean-pool per block
    d          = embeddings.shape[1]
    block_embs = np.zeros((len(block_ids), d), dtype=np.float32)
    counts     = np.zeros(len(block_ids),     dtype=np.int32)
    np.add.at(block_embs, text_to_blk, embeddings)
    np.add.at(counts,     text_to_blk, 1)
    block_embs /= np.maximum(counts, 1)[:, None]

    del embeddings, all_texts
    gc.collect()
    if CUDA_AVAILABLE:
        torch.cuda.empty_cache()

    # Save — BlockId + dim_0..dim_N (matches cluster_eval.py expectations)
    dim_cols = [f"dim_{i}" for i in range(d)]
    out_df   = pd.DataFrame(block_embs, columns=dim_cols)
    out_df.insert(0, "BlockId", block_ids)
    out_df.to_csv(out_path, index=False)

    size_mb = out_path.stat().st_size / 1e6
    print(f"  Saved -> {out_path}  ({out_df.shape[0]:,} rows x {d} dims, {size_mb:.0f} MB)")

    del model, block_embs, out_df
    gc.collect()
    if CUDA_AVAILABLE:
        torch.cuda.empty_cache()
    print("  Memory cleared.")

    return out_path


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Export per-block sentence embeddings from consumer.py incidents"
    )
    ap.add_argument("--incidents", required=True,
                    help="Path to incidents.ndjson from consumer.py")
    ap.add_argument("--out",       default="../results/embeddings",
                    help="Output directory for embedding CSVs")
    ap.add_argument("--batch",     type=int, default=256,
                    help="Encoding batch size (increase to 512+ if you have a GPU)")
    ap.add_argument("--model",     default=MODEL_NAME,
                    help=f"Model to use (default: {MODEL_NAME})")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Load incidents
    incidents_df = load_incidents(args.incidents)
    if incidents_df.empty:
        print("ERROR: No incidents loaded. Check --incidents path.")
        return

    # 2. Embed
    out_path = embed_and_export(incidents_df, args.model, out_dir, args.batch)

    print("\nDone.")
    print(f"Embedding CSV: {out_path.resolve()}")
    print("Pass this to cluster_eval.py as --embeddings")


if __name__ == "__main__":
    main()