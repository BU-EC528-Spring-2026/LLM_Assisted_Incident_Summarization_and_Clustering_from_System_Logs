# LLM-Assisted Incident Summarization and Clustering from System Logs

A pipeline for clustering system log incidents and generating AI-powered summaries using Large Language Models. This tool helps Site Reliability Engineers (SREs) quickly understand production incidents by automatically grouping related failures, flagging anomalies, and generating concise summaries.

## Project Overview

This project implements an end-to-end pipeline that:
1. **Ingests** system logs using Fluent Bit, the built-in parser, or pre-existing NDJSON (supports HDFS, Thunderbird, Zookeeper)
2. **Groups** log messages into incidents by block ID and time window
3. **Summarizes** each block's logs using an LLM (swappable between OpenAI, Ollama, Mistral)
4. **Embeds** the summary text using sentence transformers (nomic-embed-text-v1.5)
5. **Clusters** embeddings with HDBSCAN
6. **Detects anomalies** with a pre-trained Local Outlier Factor (LOF) model
7. **Summarizes clusters** at a higher level via AnythingLLM
8. **Visualizes** clusters and summaries in a Streamlit dashboard
9. **Evaluates** LLM summarization quality via schema compliance and LLM-as-judge


## System Architecture

**Ingestion** (three options, choose one):

    Option A: HDFS.log → Fluent Bit (HTTP, port 9880) → consumer.py → incidents.ndjson
    Option B: HDFS.log → hdfs_parser.py + incident_grouper.py     → incidents.ndjson
    Option C: existing  incidents.ndjson on disk

**Pipeline:**

    incidents.ndjson → cluster_pipeline.py → pipeline_output/
                                                ├── embeddings.csv
                                                ├── clusters.csv         ← includes is_anomaly + anomaly_score
                                                └── summaries.csv
                                  +
                          (LOF model loaded for inference scoring)
                                  ↓
                       Summarizationllm.py → cluster_summaries.csv

**Dashboard:**

    streamlit run app.py    (reads everything from pipeline_output/)

The orchestrator `run_pipeline.py` runs all stages end-to-end with sensible defaults.


## What Changed Since Milestone 3

- **End-to-end orchestrator (`run_pipeline.py`)**: A single command now runs ingestion → summarization → embedding → clustering → anomaly detection → cluster-level summary → dashboard launch. Each stage can be skipped independently with `--skip-*` flags.
- **LOF anomaly detection**: We trained a Local Outlier Factor model on a stratified HDFS sample with nomic embeddings of LLM summaries. On the held-out test set it achieves ROC-AUC 0.95, PR-AUC 0.89, F1 0.82 — comfortably outperforming our PyTorch port of Prodigy's VAE (ROC-AUC 0.87) and One-Class SVM (ROC-AUC 0.90). LOF inference is now a step inside `cluster_pipeline.py`, adding `is_anomaly` and `anomaly_score` columns to `clusters.csv`.
- **Three ingestion modes**: Existing-file, in-process parser, or Fluent Bit + consumer. The parser mode is the simplest path for HDFS — no Fluent Bit required.
- **Parallel LLM summarization**: A bounded thread pool with sliding-window RPM limiting saturates Tier 1 OpenAI rate limits. ~7× speedup over the previous serial version with no zombie-thread risk.
- **Demo mode for video/walkthrough runs**: `--demo-balance N` filters incidents to a balanced N-block subset (default 60% healthy / 40% anomalous) using ground-truth labels, so you can demo without paying for thousands of LLM calls.
- **UI connected to pipeline**: The Streamlit dashboard reads directly from `pipeline_output/`, including the new `is_anomaly` column for color-coding the embedding scatter plot.


## Prerequisites

- **Python 3.11** (required)
- **Fluent Bit** (only required for the `--ingest fluentbit` path)
- **Git**
- At least 8GB RAM recommended (for embedding generation)
- **Ollama** (optional — only needed if using local Llama 3.1)
- **AnythingLLM** running locally (optional — only required for cluster-level summaries via stage 5)


## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/BU-EC528-Spring-2026/LLM_Assisted_Incident_Summarization_and_Clustering_from_System_Logs.git
cd LLM_Assisted_Incident_Summarization_and_Clustering_from_System_Logs

python3.11 -m venv env

# Activate on Linux/Mac
source env/bin/activate

# Activate on Windows
env\Scripts\activate

pip install -r requirements.txt
pip install jsonschema
```


### 2. Download the Dataset

Download the HDFS logs from https://github.com/logpai/loghub (scroll to the README, click the download link on the HDFS_v1 row).

You need:
- `HDFS.log` (raw log file — or `HDFS_2k.log` for a smaller test run)
- `Event_occurrence_matrix.csv` (ground truth labels)
- `anomaly_label.csv`

Place them in the `data/` directory:
```
data/
├── HDFS.log                    # Full raw HDFS logs (1.47 GB)
├── HDFS_2k.log                 # Smaller 2000-line sample (for testing)
├── Event_occurrence_matrix.csv # Ground truth event occurrence per block
└── anomaly_label.csv           # Normal/Anomaly label per block
```


### 3. Configure API Keys

Create a `.env` file in the project root:

```
OPENAI_API_KEY=your-openai-key-here
MISTRAL_API_KEY=your-mistral-key-here
SUMM_API_KEY=your-anythingllm-api-key-here
```

`MISTRAL_API_KEY` is only needed if using the Mistral provider. `SUMM_API_KEY` is only needed for the cluster-level summarization stage. The OpenAI key is needed for the main pipeline (GPT-4o-mini summarization) and for LLM-as-judge evaluation.


### 4. (Optional) Train the LOF Anomaly Detection Model

The pipeline includes anomaly detection via a pre-trained LOF model. The bundle (`lof_hdfs.joblib`) should already be in `src/`. If you need to retrain it (e.g. on a different dataset or with different hyperparameters), open the LOF training notebook in `notebooks/` and run end-to-end. Training takes a few minutes; the resulting `.joblib` is ~30MB.

If the LOF bundle is missing, the pipeline will warn and continue with anomaly detection disabled (`is_anomaly` column set to `False` for all rows).


## Quick Start: End-to-End with `run_pipeline.py`

The fastest way to run the whole thing on an HDFS log:

```bash
python run_pipeline.py \
    --ingest parser \
    --raw-log data/HDFS_2k.log \
    --out-dir pipeline_output
```

This runs ingestion → summarization → embedding → clustering → LOF anomaly scoring → cluster-level summary → launches the Streamlit dashboard, all in one command.

### Common variations

```bash
# Reuse an incidents.ndjson that's already on disk
python run_pipeline.py --ingest existing --incidents ingestion/incidents.ndjson

# Use the Fluent Bit + consumer.py ingestion path (requires fluent-bit installed)
python run_pipeline.py --ingest fluentbit --dataset hdfs

# Skip optional stages (no AnythingLLM, no dashboard launch)
python run_pipeline.py --ingest existing \
    --incidents ingestion/incidents.ndjson \
    --skip-cluster-summary --skip-dashboard

# Demo run — for finer-grained demo control (e.g. --demo-balance 150),
# call cluster_pipeline.py directly. See "Manual Step-by-Step" below.
```

### Available `run_pipeline.py` flags

| Flag | Default | Purpose |
|------|---------|---------|
| `--ingest {existing,parser,fluentbit}` | `existing` | Ingestion mode |
| `--incidents PATH` | `ingestion/incidents.ndjson` | Path to NDJSON (input or output) |
| `--raw-log PATH` | — | Required for `--ingest parser` |
| `--dataset {hdfs,thunderbird,zookeeper}` | `hdfs` | Dataset for `--ingest fluentbit` |
| `--out-dir PATH` | `pipeline_output/` | Where CSVs land |
| `--lof-model PATH` | `src/lof_hdfs.joblib` | LOF bundle for anomaly scoring |
| `--mcs N` / `--ms N` | `50` / `20` | HDBSCAN parameters |
| `--skip-anomaly` | off | Skip LOF inference |
| `--skip-cluster-summary` | off | Skip AnythingLLM stage |
| `--skip-dashboard` | off | Don't launch Streamlit |

For finer-grained control over individual stages (especially demo-balance subsetting and parallel summarization tuning), run `cluster_pipeline.py` directly — see "Manual Step-by-Step" below.


## Manual Step-by-Step (for finer control)

### Step 1: Start the Ingestion Consumer

In one terminal, start the HTTP consumer that receives logs from Fluent Bit:

```bash
python ingestion/consumer.py --dataset hdfs --out ingestion/incidents.ndjson
```

Arguments:
- `--dataset`: `hdfs`, `thunderbird`, or `zookeeper` (default: `hdfs`)
- `--port`: HTTP port (default: `9880`)
- `--window`: Incident grouping time window in minutes (default: `5`)
- `--data`: Directory containing `anomaly_label.csv` (default: `./data`)
- `--out`: Output file path (default: `incidents.ndjson`)


### Step 2: Start Fluent Bit

```bash
cd ingestion
/opt/fluent-bit/bin/fluent-bit -c fluent-bit.conf
```

Adjust the path based on your installation. On many systems it's just `fluent-bit` if it's in your PATH.


### Step 3: Stop the Consumer

After Fluent Bit finishes tailing the log file, press **Ctrl+C** in the consumer terminal. This triggers a flush that groups all buffered entries into incidents and writes them to `incidents.ndjson`.

> **Tip**: If you want to skip the Fluent Bit dance entirely, use the in-process parser:
> ```bash
> python -c "
> from src.hdfs_parser import HDFSLogParser
> from src.incident_grouper import IncidentGrouper
> import json
> parsed = HDFSLogParser().parse_file('data/HDFS_2k.log')
> incidents = IncidentGrouper(time_window_minutes=5).group_incidents(parsed)
> with open('ingestion/incidents.ndjson', 'w') as f:
>     for inc in incidents: f.write(json.dumps(inc) + '\n')
> "
> ```
> or just use `python run_pipeline.py --ingest parser --raw-log data/HDFS_2k.log`.


### Step 4: Run the Clustering Pipeline

```bash
python src/cluster_pipeline.py \
    --incidents ingestion/incidents.ndjson \
    --out-dir ./pipeline_output \
    --lof-model src/lof_hdfs.joblib
```

Arguments:
- `--incidents`: Path to `incidents.ndjson` from Step 3 (required)
- `--out-dir`: Output directory (default: `./pipeline_output`)
- `--model`: Sentence transformer model (default: `nomic-ai/nomic-embed-text-v1.5`)
- `--lof-model`: Path to trained LOF bundle (default: `./training_output/lof_hdfs.joblib`)
- `--skip-anomaly-detection`: Skip LOF inference entirely
- `--demo-balance N`: Subset to N blocks balanced by ground-truth label (great for demo videos — caps LLM cost)
- `--demo-label-csv PATH`: Path to anomaly_label.csv used by `--demo-balance`
- `--demo-healthy-frac F`: Healthy fraction in demo subset (default 0.60)
- `--mcs`: HDBSCAN min_cluster_size (default: `50`)
- `--ms`: HDBSCAN min_samples (default: `20`)
- `--max-workers`: Concurrent threads for LLM summarization (default: `8`)
- `--max-rpm`: Sliding-window RPM cap (default: `120`)
- `--batch-size`: Embedding batch size (default: `256`)

**Note on cost**: This step makes one OpenAI API call per unique block. With the full HDFS dataset (~575k blocks) it would take ~50 minutes and ~$15 in API credits at Tier 1. For testing or demos, use `--demo-balance 150` to cap at ~150 blocks (~$0.50, ~1 minute).

**Note on rate limits**: Defaults of `--max-workers 8 --max-rpm 120` are calibrated for OpenAI Tier 1 caps (500 RPM, 200k TPM). If you hit 429s, lower `--max-rpm`. If you're on a higher tier, raise both.

Output:
```
pipeline_output/
├── embeddings.csv    # block_id + dim_0..dim_767
├── clusters.csv      # block_id + cluster_id + is_anomaly + anomaly_score
└── summaries.csv     # block_id + summary text
```


### Step 5: Generate Cluster-Level Summaries

After clustering, summarize each cluster as a whole using AnythingLLM:

```bash
python src/Summarizationllm.py
```

Reads `pipeline_output/clusters.csv` and `pipeline_output/summaries.csv`, sends each cluster's set of summaries to AnythingLLM with a prompt that asks for a structured JSON summary, and writes `pipeline_output/cluster_summaries.csv`.

Requires:
- AnythingLLM running locally (default `http://localhost:3001`)
- `SUMM_API_KEY` set in `.env`

(If you used `run_pipeline.py`, this step ran automatically. Skip it manually with `--skip-cluster-summary` if AnythingLLM isn't available.)


### Step 6: Launch the Dashboard

```bash
streamlit run app.py
```

The dashboard reads from `./pipeline_output/` by default (configurable in the sidebar). It provides:
- **2D Visualization**: PCA projection of block embeddings, color-coded by anomaly status, click-to-inspect for individual block summaries
- **Cluster Explorer**: Browse summaries grouped by cluster, with the cluster-level analysis from Stage 5 if `cluster_summaries.csv` is present
- **Raw Data**: Full merged dataset table

A sampling control in the sidebar limits points per cluster so the UI stays responsive on large datasets.


### Step 7: Generate Cluster JSON for Evaluation (Optional)

If you want to run LLM evaluation against multiple providers, you'll need a `clusters_output.json` produced by `llmtest.py`:

```bash
python src/llmtest.py src/clusters_output.json --provider openai
python src/llmtest.py src/clusters_output.json --provider ollama
python src/llmtest.py src/clusters_output.json --provider mistral
```

Output per cluster:
```json
{
  "title": "Write failure affecting DataNode block replication",
  "affected_components": ["DataNode", "PacketResponder"],
  "root_cause_hypothesis": "IOException during write stream caused incomplete replication",
  "severity": "medium"
}
```


### Step 8: Evaluate LLM Summarization Quality (Optional)

```bash
python src/llm_eval.py openai src/clusters_output.json --samples 20
python src/llm_eval.py ollama src/clusters_output.json --samples 20
python src/llm_eval.py mistral src/clusters_output.json --samples 20
```

This runs two evaluation methods per provider:
1. **Schema compliance** — checks JSON output has all required fields with correct types
2. **LLM-as-judge** — sends original logs and generated summary to GPT-4o, which scores faithfulness/completeness/conciseness on a 1-5 scale

Sample output:
```
============================================================
Results: openai
============================================================
Schema compliance: 20/20 (100%)
Avg faithfulness:  4.65
Avg completeness:  4.30
Avg conciseness:   4.50
```

The judge always uses GPT-4o regardless of which provider is being evaluated, so a working `OPENAI_API_KEY` is required even when evaluating Ollama or Mistral.


## Project Structure

```
.
├── run_pipeline.py             # ★ End-to-end orchestrator (all stages)
├── app.py                      # Streamlit dashboard
│
├── ingestion/                  # Log ingestion pipeline
│   ├── consumer.py             # HTTP consumer — receives logs, groups into incidents
│   ├── fluent-bit.conf         # Fluent Bit config (HTTP output to port 9880)
│   ├── parsers.conf            # Log parsing rules (hdfs, thunderbird, zookeeper)
│   ├── incidents.ndjson        # Output from consumer (one incident per line)
│   └── INGESTION_README.md     # Detailed ingestion documentation
│
├── src/                        # Core processing pipeline
│   ├── cluster_pipeline.py     # Main pipeline: summarize → embed → cluster → LOF
│   ├── lof_inference.py        # Loads LOF bundle, scores embeddings at inference
│   ├── lof_hdfs.joblib         # Pre-trained LOF model bundle
│   ├── Summarizationllm.py     # Cluster-level summary via AnythingLLM
│   ├── llm_provider.py         # Swappable LLM interface (OpenAI, Ollama, Mistral)
│   ├── llmtest.py              # Cluster-level LLM summarization (uses llm_provider)
│   ├── llm_eval.py             # LLM evaluation: schema compliance + LLM-as-judge
│   ├── cluster_eval.py         # Cluster quality evaluation (RCA score, purity, recall)
│   ├── embed_export.py         # Export per-block embeddings to CSV
│   ├── embeddings_evaluator.py # Embedding model benchmarking (retrieval + clustering)
│   ├── hdfs_parser.py          # HDFS log parser
│   ├── incident_grouper.py     # Incident grouping logic (used by --ingest parser)
│   └── run_embedding_evaluation.py
│
├── notebooks/                  # Jupyter notebooks for exploration & training
│   ├── 01_data_exploration.ipynb
│   ├── 02_clustering_experiments.ipynb
│   ├── 02_embedding_evaluation.ipynb
│   ├── 03_train_prodigy_vae_hdfs.ipynb     # Anomaly detection training (LOF + VAE comparison)
│   └── 03_embedding_export.ipynb
│
├── pipeline_output/            # Output from cluster_pipeline.py
│   ├── embeddings.csv          # block_id + dim_0..dim_767
│   ├── clusters.csv            # block_id + cluster_id + is_anomaly + anomaly_score
│   ├── summaries.csv           # block_id + summary text
│   └── cluster_summaries.csv   # cluster_id + cluster-level summary
│
├── data/                       # Log datasets and ground truth
│   ├── HDFS.log
│   ├── HDFS_2k.log
│   ├── Event_occurrence_matrix.csv
│   └── anomaly_label.csv
│
├── requirements.txt
├── .env                        # API keys (not committed)
└── README.md
```


## Troubleshooting

### Consumer Doesn't Receive Logs

- Verify Fluent Bit is running and `Path` in `fluent-bit.conf` points to an existing log file
- Check that `consumer.py` is running **before** starting Fluent Bit
- Confirm the port matches — `consumer.py` defaults to `9880`, and `fluent-bit.conf` must use the same port
- Ensure nothing else is bound to port 9880
- **If Fluent Bit is more trouble than it's worth**: switch to `--ingest parser` in `run_pipeline.py`. Same end result, no Fluent Bit dependency.

### Every Block Flagged as Anomalous

This usually means a mismatch between the LOF training distribution and the inference embeddings:
- **The LOF bundle was trained on different summaries**: confirm everyone is using the same `lof_hdfs.joblib`. Don't retrain per-machine — copy the bundle directly.
- **Different LLM provider or prompt than at training time**: LOF is sensitive to embedding distribution. If you switched providers (OpenAI ↔ Ollama ↔ Mistral) or changed `SUMMARIZE_PROMPT`, the embeddings shift and everything looks novel to the model. Retrain LOF with the new setup, or revert to the original.
- **Empty or failed summaries**: check `summaries.csv` for `[SUMMARIZATION_FAILED]` entries. If most are failed (often due to an expired API key or quota), all blocks end up with identical embeddings and LOF flags everything.

### No Anomalies Detected

- Confirm LOF bundle exists at `--lof-model` path (default `src/lof_hdfs.joblib`)
- Check the pipeline log for "LOF model not found" — if you see it, the pipeline silently fell back to `is_anomaly=False` for everyone
- Run with `--skip-anomaly-detection` to confirm anomaly detection isn't masking another issue

### LLM Summaries Not Generated

- Ensure `OPENAI_API_KEY` is set in `.env`
- Check internet connectivity
- If you hit rate limits (Tier 1: 500 RPM, 10,000 RPD, 200k TPM), lower `--max-rpm` or `--max-workers`
- If you exhausted RPD, you must wait 24 hours — checkpoint files prevent loss of in-progress work

### Cluster-Level Summarization Fails

- Confirm AnythingLLM is running locally (default `http://localhost:3001`)
- Check `SUMM_API_KEY` is set in `.env`
- If you don't have AnythingLLM, pass `--skip-cluster-summary` to `run_pipeline.py` — the dashboard works fine without it (the Cluster Explorer tab just won't show overall analyses)

### Ollama Provider Not Responding

- Make sure Ollama is running: `ollama serve`
- Verify the model is pulled: `ollama pull llama3.1`
- Check that port 11434 is accessible on localhost

### Dashboard Won't Load

- Make sure `streamlit` is installed: `pip install streamlit`
- Confirm `pipeline_output/clusters.csv` exists — the dashboard won't render without it
- If the UI is slow, lower "Max samples per cluster" in the sidebar

### Memory Issues During Embedding

- Reduce batch size: `--batch-size 64`
- Use a smaller embedding model: `--model sentence-transformers/all-MiniLM-L6-v2` (will require retraining LOF since input dim changes)
- Process a smaller log file first (`HDFS_2k.log`)

### LLM Eval Errors

- Install jsonschema: `pip install jsonschema`
- The judge always uses GPT-4o, so a working `OPENAI_API_KEY` is required even when evaluating other providers
- If a provider fails on a cluster, the eval logs the error and skips to the next one


## Acknowledgments

- Dataset: LogHub — HDFS logs
- Embeddings: Sentence Transformers (nomic-ai/nomic-embed-text-v1.5)
- Clustering: HDBSCAN
- Anomaly detection: scikit-learn LocalOutlierFactor (compared against PyTorch port of peaclab/Prodigy)
- LLM: OpenAI GPT-4o-mini, Meta Llama 3.1, Mistral, AnythingLLM
