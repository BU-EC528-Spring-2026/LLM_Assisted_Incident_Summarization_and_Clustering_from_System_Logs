# LLM-Assisted Incident Summarization and Clustering from System Logs

A pipeline for clustering system log incidents and generating AI-powered summaries using Large Language Models. This tool helps Site Reliability Engineers (SREs) quickly understand production incidents by automatically grouping related failures and generating concise summaries.

## Project Overview

This project implements an end-to-end pipeline that:
1. **Ingests** HDFS logs using Fluent Bit
2. **Groups** log messages into incident windows
3. **Clusters** similar incidents using sentence transformers and HDBSCAN
4. **Summarizes** clusters using GPT-4o-mini to generate incident reports

## System Architecture



HDFS.log → Fluent Bit → consumer.py → incidents.ndjson → cluster_pipeline.py → clusters_output.json → llmtest.py → Incident Summaries
↑ ↑
Event_occurrence_matrix.csv |
(ground truth) |


## Prerequisites

- **Python 3.11** (required - project uses specific features)
- **Fluent Bit** (for log ingestion)
- **Git** (for downloading datasets)
- At least 8GB RAM recommended (for embedding generation)

## Installation

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd LLM_Assisted_Incident_Summarization_and_Clustering_from_System_Logs

# Create virtual environment with Python 3.11
python3.11 -m venv env

# Activate on Linux/Mac
source env/bin/activate

# Activate on Windows
env\Scripts\activate 

pip install -r requirements.txt

# Create data directory if it doesn't exist
mkdir -p data

# Download and extract HDFS logs
# You'll need:
# - HDFS.log (raw log file)
# - Event_occurrence_matrix.csv (ground truth labels)

data/
├── HDFS.log                    # Raw HDFS logs
|-- preprocessed/
    ├── Event_occurrence_matrix.csv # Ground truth labels
    ├── anomaly_label.csv

[INPUT]
    Name tail
    Path /path/to/your/HDFS.log   # Update this path
    # ... other settings

[OUTPUT]
    Name  forward
    Match *
    Host  127.0.0.1
    Port  24224
```


### 2. Update Parser Configuration

Edit ingestion/parsers.conf to match your HDFS log format if needed. The default parser should work with standard HDFS logs.



### 3. Configure API Key

Create a .env file in the project root or hardcode it in llmtest.py


`OPENAI_API_KEY=your-api-key-here`



## Usage

### Step 1: Start the Consumer

In one terminal, start the consumer that will receive logs from Fluent Bit:


`python ingestion/consumer.py`


This will create ingestion/incidents.ndjson as it receives logs.

### Step 2: Start Fluent Bit

In another terminal, navigate to the ingestion folder and start Fluent Bit:


`cd ingestion
/opt/fluent-bit/bin/fluent-bit -c fluent-bit.conf`



Note: Adjust the path to fluent-bit based on your installation. On some systems, it might be fluent-bit directly if in PATH.

### Step 3: Stop the Consumer

After enough logs have been processed (or when you want to stop), press Ctrl+C in the consumer terminal to stop it. The incidents.ndjson file will contain all processed incidents.




### Step 4: Run Clustering Pipeline

Now cluster the incidents:



`python src/cluster_pipeline.py \
    --incidents ingestion/incidents.ndjson \
    --occ ../data/Event_occurrence_matrix.csv \
    --out src/clusters_output.json`



Arguments:
-  --incidents: Path to incidents.ndjson (required)
-  --occ: Path to Event_occurrence_matrix.csv (optional, but 
recommended)
-  --out: Output path for cluster results (default: clusters_output.json)
-  --model: Sentence transformer model (default: sentence-transformers/all-MiniLM-L6-v2)
-  --mcs: HDBSCAN min_cluster_size (default: 648)
-  --ms: HDBSCAN min_samples (default: 20)



### Step 5: Generate LLM Summaries

Finally, generate AI-powered incident summaries:





`python src/llmtest.py src/clusters_output.json`





This will output structured JSON summaries for each cluster:




``` {
  "title": "Database connection timeout affecting API service",
  "affected_components": ["Database", "API"],
  "root_cause_hypothesis": "Connection pool exhaustion due to slow queries",
  "severity": "high"
}

.
├── ingestion/                  # Log ingestion pipeline
│   ├── consumer.py            # Receives logs and groups into incidents
│   ├── fluent-bit.conf        # Fluent Bit configuration
│   ├── incidents.ndjson       # Output from consumer (incident data)
│   ├── parsers.conf           # Log parsing rules
│   └── INGESTION_README.md    # Detailed ingestion documentation
│
├── src/                       # Core processing pipeline
│   ├── cluster_pipeline.py    # Clustering pipeline
│   ├── llmtest.py            # LLM summarization script
│   ├── cluster_eval.py       # Cluster evaluation metrics
│   ├── embed_export.py       # Export embeddings for analysis
│   ├── hdfs_parser.py        # HDFS log parser
│   ├── incident_grouper.py   # Incident grouping logic
│   └── run_embedding_evaluation.py  # Embedding evaluation
│
├── notebooks/                 # Jupyter notebooks for exploration
│   ├── 01_data_exploration.ipynb
│   ├── 02_clustering_experiments.ipynb
│   ├── 02_clustering_experiments_v2.ipynb
│   ├── 02_embedding_evaluation.ipynb
│   └── 03_embedding_export.ipynb
│
├── results/                   # Experiment results and visualizations
│   ├── embeddings/           # Saved embeddings for analysis
│   ├── *.png                 # Cluster visualizations
│   └── *_results.csv         # Evaluation metrics
│
├── app.py                    # Main application entry point
├── requirements.txt          # Python dependencies
├── test_parser.py           # Parser testing utility
└── README.md                # This file
```


## Troubleshooting

### Consumer Doesn't Receive Logs

Verify Fluent Bit is running and paths in fluent-bit.conf are correct
Check that consumer.py is running before starting Fluent Bit
Ensure port 24224 is not blocked by firewall

### No Anomalies Detected in Clusters

Make sure you're providing the --occ parameter with correct path
Verify BlockIds in incidents.ndjson match those in Event_occurrence_matrix.csv
Check that the occurrence matrix has the expected format (BlockId, Label, Type columns)

### LLM Summaries Not Generated

Ensure OpenAI API key is set (either in .env or hardcoded for testing)
Check internet connection for API access
Verify cluster file contains data (non-empty clusters)

### Memory Issues During Embedding

Reduce batch size by adding --batch-size 64 to cluster_pipeline.py
Use a smaller model (e.g., --model all-MiniLM-L6-v2 is already lightweight)
Process incidents in smaller chunks


## Acknowledgments

Dataset: LogHub - HDFS logs
Embeddings: Sentence Transformers
Clustering: HDBSCAN
LLM: OpenAI GPT-4o-mini