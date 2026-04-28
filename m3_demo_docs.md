# LLM-Assisted Incident Summarization and Clustering from System Logs

A pipeline for clustering system log incidents and generating AI-powered summaries using Large Language Models. This tool helps Site Reliability Engineers (SREs) quickly understand production incidents by automatically grouping related failures and generating concise summaries.

## Project Overview

This project implements an end-to-end pipeline that:
1. **Ingests** system logs using Fluent Bit (supports HDFS, Thunderbird, Zookeeper)
2. **Groups** log messages into incidents by block ID and time window
3. **Summarizes** each block's logs using an LLM (swappable between OpenAI, Ollama, Mistral)
4. **Embeds** the summary text using sentence transformers (nomic-embed-text-v1.5)
5. **Clusters** embeddings with HDBSCAN
6. **Visualizes** clusters and summaries in a Streamlit dashboard
7. **Evaluates** LLM summarization quality via schema compliance and LLM-as-judge


## System Architecture

**Ingestion:**

    HDFS.log → Fluent Bit (HTTP, port 9880) → consumer.py → incidents.ndjson

**Pipeline:**

    incidents.ndjson → cluster_pipeline.py → pipeline_output/
                                                ├── embeddings.csv
                                                ├── clusters.csv
                                                └── summaries.csv

**Downstream (all read from pipeline_output/):**

    llmtest.py        → Cluster-level summaries (JSON per cluster)
    llm_eval.py       → Evaluation scores (schema compliance + LLM-as-judge)
    app.py (Streamlit) → Dashboard UI


## What Changed Since Milestone 2

- **LLM summarization before embedding**: Previously, raw log lines were embedded directly using mean pooling per incident, which produced mostly meaningless embeddings (cosine similarity >0.95 across all sets). Now, each block's logs are summarized by GPT-4o-mini first, and the summary text is embedded instead. This nearly doubled the intra-type vs inter-type similarity gap and increased k-NN purity by ~20% to roughly 79%.
- **Swappable LLM providers**: The summarization step is abstracted behind `llm_provider.py`, allowing OpenAI, Ollama (Llama 3.1 local), and Mistral to be swapped with a single CLI argument.
- **LLM evaluation before fine-tuning**: `llm_eval.py` measures summarization quality across providers using JSON schema compliance and GPT-4o as a judge scoring faithfulness, completeness, and conciseness.
- **cluster_pipeline.py rewrite**: The pipeline now handles summarization, embedding, and clustering in a single run, outputting three CSVs (embeddings, clusters, summaries) to `pipeline_output/`.
- **Multi-dataset ingestion**: `consumer.py` now supports HDFS, Thunderbird, and Zookeeper log formats via a `--dataset` flag. Fluent Bit sends logs over HTTP to port 9880.
- **UI connected to pipeline**: The Streamlit dashboard now reads directly from `pipeline_output/` instead of computing its own embeddings and clusters.


## Prerequisites

- **Python 3.11** (required)
- **Fluent Bit** (for log ingestion)
- **Git**
- At least 8GB RAM recommended (for embedding generation)
- **Ollama** (optional — only needed if using local Llama 3.1)


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


### 3. Update Fluent Bit Config

Edit `ingestion/fluent-bit.conf` and update the `Path` to point at your log file:

```ini
[INPUT]
    Name             tail
    Path             data/HDFS_2k.log    # <-- Update this to your log file path
    Parser           hdfs
    Tag              logs.raw
    Read_from_Head   On
    Refresh_Interval 2

[OUTPUT]
    Name             http
    Match            logs.raw
    Host             127.0.0.1
    Port             9880
    URI              /ingest
    Format           json_lines
```

If using Thunderbird or Zookeeper, also change `Parser` to match (`thunderbird` or `zookeeper`) and update the `Path` accordingly. The parser definitions live in `ingestion/parsers.conf`.


### 4. Configure API Keys

Create a `.env` file in the project root:

```
OPENAI_API_KEY=your-openai-key-here
MISTRAL_API_KEY=your-mistral-key-here
```

`MISTRAL_API_KEY` is only required if you plan to use the Mistral provider. The OpenAI key is needed for the main pipeline (GPT-4o-mini summarization) and for LLM-as-judge evaluation.


## Usage

### Step 1: Start the Ingestion Consumer

In one terminal, start the HTTP consumer that receives logs from Fluent Bit:

```bash
python ingestion/consumer.py --dataset hdfs --out ingestion/incidents.ndjson
```

Arguments:
- `--dataset`: Log format — `hdfs`, `thunderbird`, or `zookeeper` (default: `hdfs`)
- `--port`: HTTP port (default: `9880`)
- `--window`: Incident grouping time window in minutes (default: `5`)
- `--data`: Directory containing `anomaly_label.csv` (default: `./data`)
- `--out`: Output file path (default: `incidents.ndjson`)

The consumer will start an HTTP server on port 9880 and wait for Fluent Bit to send logs.


### Step 2: Start Fluent Bit

In a second terminal, navigate to the ingestion folder and start Fluent Bit:

```bash
cd ingestion
/opt/fluent-bit/bin/fluent-bit -c fluent-bit.conf
```

Adjust the path to `fluent-bit` based on your installation. On some systems it may be `fluent-bit` directly if it's in your PATH.

You should see the consumer terminal logging incoming entries.


### Step 3: Stop the Consumer

After Fluent Bit finishes tailing the log file (or when you want to stop), press **Ctrl+C** in the consumer terminal. This triggers a flush that groups all buffered entries into incidents and writes them to `incidents.ndjson`.


### Step 4: Run the Clustering Pipeline

This step summarizes each block's logs with GPT-4o-mini, embeds the summaries, and clusters them with HDBSCAN:

```bash
python src/cluster_pipeline.py \
    --incidents ingestion/incidents.ndjson \
    --out-dir ./pipeline_output
```

Arguments:
- `--incidents`: Path to `incidents.ndjson` from Step 3 (required)
- `--out-dir`: Output directory (default: `./pipeline_output`)
- `--model`: Sentence transformer model (default: `nomic-ai/nomic-embed-text-v1.5`)
- `--mcs`: HDBSCAN min_cluster_size (default: `50`)
- `--ms`: HDBSCAN min_samples (default: `20`)
- `--batch-size`: Embedding batch size (default: `256`)

**Note**: This step makes OpenAI API calls for every unique block. With the full HDFS dataset (~575k blocks) this will take a long time and use significant API credits. For testing, use `HDFS_2k.log` which produces ~1,994 incidents.

Output:
```
pipeline_output/
├── embeddings.csv    # block_id + 768-dim embedding vectors
├── clusters.csv      # block_id + cluster_id (-1 = noise)
└── summaries.csv     # block_id + LLM-generated summary text
```


### Step 5: Launch the Dashboard

```bash
streamlit run app.py
```

The dashboard reads from `./pipeline_output/` by default (configurable in the sidebar). It provides:
- **2D Visualization**: PCA projection of block embeddings colored by cluster, with click-to-inspect for individual block summaries
- **Cluster Explorer**: Browse summaries grouped by cluster, with an overall cluster analysis if `cluster_summaries.csv` is present
- **Raw Data**: Full merged dataset table

A sampling control in the sidebar limits points per cluster so the UI stays responsive on large datasets.


### Step 6: Generate Cluster-Level Summaries (Optional)

If you have a `clusters_output.json` from a previous run or want to summarize at the cluster level (rather than per-block), run:

```bash
# Using OpenAI (default)
python src/llmtest.py src/clusters_output.json --provider openai

# Using local Llama 3.1 via Ollama
python src/llmtest.py src/clusters_output.json --provider ollama

# Using Mistral
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


### Step 7: Evaluate LLM Summarization Quality (Optional)

Compare how different LLM providers perform on the same data before committing to fine-tuning:

```bash
python src/llm_eval.py openai src/clusters_output.json --samples 20
python src/llm_eval.py ollama src/clusters_output.json --samples 20
python src/llm_eval.py mistral src/clusters_output.json --samples 20
```

Arguments:
- First positional: provider name (`openai`, `ollama`, or `mistral`)
- Second positional: path to clusters JSON
- `--samples`: Number of clusters to evaluate (default: `20`)

This runs two evaluation methods per provider:
1. **Schema compliance** — checks that the JSON output has all required fields (`title`, `affected_components`, `root_cause_hypothesis`, `severity`) with correct types
2. **LLM-as-judge** — sends the original logs and the generated summary to GPT-4o, which scores on faithfulness (1–5), completeness (1–5), and conciseness (1–5)

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

**Note**: The judge always uses GPT-4o via the OpenAI API regardless of which provider is being evaluated, so a working `OPENAI_API_KEY` is required even when evaluating Ollama or Mistral.


## Project Structure

```
.
├── ingestion/                  # Log ingestion pipeline
│   ├── consumer.py            # HTTP consumer — receives logs, groups into incidents
│   ├── fluent-bit.conf        # Fluent Bit config (HTTP output to port 9880)
│   ├── parsers.conf           # Log parsing rules (hdfs, thunderbird, zookeeper)
│   ├── incidents.ndjson       # Output from consumer (one incident per line)
│   └── INGESTION_README.md    # Detailed ingestion documentation
│
├── src/                       # Core processing pipeline
│   ├── cluster_pipeline.py    # Main pipeline: summarize → embed → cluster
│   ├── llm_provider.py        # Swappable LLM interface (OpenAI, Ollama, Mistral)
│   ├── llmtest.py             # Cluster-level LLM summarization (uses llm_provider)
│   ├── llm_eval.py            # LLM evaluation: schema compliance + LLM-as-judge
│   ├── cluster_eval.py        # Cluster quality evaluation (RCA score, purity, recall)
│   ├── embed_export.py        # Export per-block embeddings to CSV
│   ├── embeddings_evaluator.py # Embedding model benchmarking (retrieval + clustering)
│   ├── hdfs_parser.py         # HDFS log parser
│   ├── incident_grouper.py    # Incident grouping logic (standalone, used in notebooks)
│   └── run_embedding_evaluation.py  # Embedding evaluation entrypoint
│
├── notebooks/                 # Jupyter notebooks for exploration
│   ├── 01_data_exploration.ipynb
│   ├── 02_clustering_experiments.ipynb
│   ├── 02_clustering_experiments_v2.ipynb
│   ├── 02_embedding_evaluation.ipynb
│   └── 03_embedding_export.ipynb
│
├── pipeline_output/           # Output from cluster_pipeline.py
│   ├── embeddings.csv         # block_id + dim_0..dim_767
│   ├── clusters.csv           # block_id + cluster_id
│   ├── summaries.csv          # block_id + summary text
│   └── cluster_summaries.csv  # (optional) cluster_id + overall cluster analysis
│
├── data/                      # Log datasets and ground truth
│   ├── HDFS.log               # Full raw HDFS logs
│   ├── HDFS_2k.log            # 2000-line sample for quick testing
│   ├── Event_occurrence_matrix.csv
│   └── anomaly_label.csv
│
├── app.py                    # Streamlit dashboard
├── requirements.txt          # Python dependencies
├── test_parser.py           # Parser testing utility
├── .env                     # API keys (not committed)
└── README.md
```


## Troubleshooting

### Consumer Doesn't Receive Logs

- Verify Fluent Bit is running and `Path` in `fluent-bit.conf` points to an existing log file
- Check that `consumer.py` is running **before** starting Fluent Bit
- Confirm the port matches — `consumer.py` defaults to `9880`, and `fluent-bit.conf` must use the same port in the `[OUTPUT]` section
- Ensure nothing else is bound to port 9880

### No Anomalies Detected in Clusters

- Make sure `anomaly_label.csv` is in the `data/` directory (or wherever `--data` points)
- Verify that BlockIds in `incidents.ndjson` match those in `anomaly_label.csv`
- If using the 2k sample, most blocks may be normal — try the full HDFS.log for anomaly coverage

### LLM Summaries Not Generated

- Ensure `OPENAI_API_KEY` is set in `.env`
- Check internet connectivity
- If you hit rate limits (OpenAI Tier 1: 500 RPM, 10,000 RPD), reduce `max_workers` in `cluster_pipeline.py` or wait before retrying
- If the pipeline was already run once against the daily limit, wait 24 hours before running again

### Ollama Provider Not Responding

- Make sure Ollama is running: `ollama serve`
- Verify the model is pulled: `ollama pull llama3.1`
- Check that port 11434 is accessible on localhost

### Dashboard Won't Load

- Make sure `streamlit` is installed: `pip install streamlit`
- Confirm that `pipeline_output/clusters.csv` exists — the dashboard won't render without it
- If the UI is slow, lower "Max samples per cluster" in the sidebar

### Memory Issues During Embedding

- Reduce batch size: `--batch-size 64`
- Use a smaller embedding model: `--model sentence-transformers/all-MiniLM-L6-v2`
- Process a smaller log file first (`HDFS_2k.log`)

### LLM Eval Errors

- Install jsonschema: `pip install jsonschema`
- The judge always uses GPT-4o, so a working `OPENAI_API_KEY` is required even when evaluating other providers
- If a provider fails on a cluster, the eval logs the error and skips to the next one


## Acknowledgments

- Dataset: LogHub — HDFS logs
- Embeddings: Sentence Transformers (nomic-ai/nomic-embed-text-v1.5)
- Clustering: HDBSCAN
- LLM: OpenAI GPT-4o-mini, Meta Llama 3.1, Mistral
