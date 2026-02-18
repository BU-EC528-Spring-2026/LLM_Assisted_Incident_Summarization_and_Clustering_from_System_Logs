# EC528 Log Incident Clustering Project

## Quick Start (Week 5 - Feb 18-22)

### 1. Setup Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Download HDFS Dataset

**Official LogHub HDFS Dataset:**
- **Primary Source:** https://zenodo.org/record/3227177/files/HDFS_1.tar.gz
- **GitHub Mirror:** https://github.com/logpai/loghub/tree/master/HDFS

**Dataset Details:**
- Size: ~1.5 GB compressed, ~11M log lines
- Format: Plain text, one log per line
- Structure: `DATE TIME THREAD_ID LEVEL COMPONENT: MESSAGE`
- Contains labeled anomalies (blocks marked as corrupt)

**Download Instructions:**
```bash
# Create data directory
mkdir -p data

# Download from Zenodo (recommended)
cd data
wget https://zenodo.org/record/3227177/files/HDFS_1.tar.gz
tar -xzf HDFS_1.tar.gz

# The extracted file will be named HDFS.log
```

### 3. Parse HDFS Logs

```bash
# Parse first 1,000 logs (for testing)
cd src
python hdfs_parser.py ../data/HDFS.log 1000

# This creates: ../data/parsed_logs_1000.json
```

**Expected output:**
```
Parsing Summary:
  Total lines processed: 1000
  Successfully parsed: 950
  Failed to parse: 50
  Success rate: 95.0%

Saved 950 logs to ../data/parsed_logs_1000.json
```

### 4. Group Into Incidents

```bash
# Group parsed logs into incidents (5-minute window)
python incident_grouper.py ../data/parsed_logs_1000.json 5

# This creates: ../data/incidents_50.json (approximate)
```

**Expected output:**
```
Grouping 750 logs with block IDs into incidents...
Found 150 unique block IDs
Created 50 incidents from 750 logs
Average logs per incident: 15.0

Saved 50 incidents to ../data/incidents_50.json
```

### 5. Explore Data

```bash
# Open Jupyter notebook
cd ../notebooks
jupyter notebook 01_data_exploration.ipynb
```

Run all cells to see:
- Log structure and statistics
- Incident size distribution
- Failure type analysis
- Sample incidents

---

## Project Structure

```
.
├── data/                      # Raw and processed data
│   ├── HDFS.log              # Raw HDFS logs (download separately)
│   ├── parsed_logs_1000.json # Parsed logs (output)
│   └── incidents_50.json     # Grouped incidents (output)
├── src/                       # Source code
│   ├── hdfs_parser.py        # Log parser
│   └── incident_grouper.py   # Incident grouping logic
├── notebooks/                 # Jupyter notebooks
│   └── 01_data_exploration.ipynb
├── outputs/                   # Final outputs for demos
└── requirements.txt           # Python dependencies
```

---

## Tasks (Week 5 - Due Feb 22)

**Parth's Tasks:**
- [x] Set up GitHub repo structure
- [x] Download HDFS dataset
- [ ] Run parser on 1,000 logs
- [ ] Group into ~50-100 incidents
- [ ] Run data exploration notebook
- [ ] **By Feb 22:** Provide Guillermo with 100 incidents in JSON

**Usage for Full Dataset (later):**
```bash
# Parse 10,000 logs (Week 6)
python hdfs_parser.py ../data/HDFS.log 10000

# Group into incidents
python incident_grouper.py ../data/parsed_logs_10000.json 5
```

---

## Troubleshooting

**Issue: Parse failures (success rate < 90%)**
- HDFS logs may have irregular lines (empty, malformed)
- Parser skips unparseable lines and continues
- Check warning messages for patterns

**Issue: Too many/too few incidents**
- Adjust time window: `python incident_grouper.py ... 3` (3 minutes)
- Larger window = fewer, larger incidents
- Smaller window = more, smaller incidents

**Issue: No block IDs found**
- Some logs don't contain block IDs (e.g., system status logs)
- Grouper only processes logs with block IDs
- Expected: ~70-80% of logs have block IDs

---

## Next Steps (Week 6)

1. Philip: Set up LLM (OpenAI POC → vLLM transition)
2. Generate summaries for 50-100 incidents
3. Prepare Milestone Demo I (Feb 25)

---

## Dataset References

- **LogHub:** https://github.com/logpai/loghub
- **HDFS Paper:** "Mining Console Logs for Large-Scale System Problem Detection"
- **Zenodo Archive:** https://zenodo.org/record/3227177

---

## Questions?

Contact team:
- Parth (Data Engineering)
- Guillermo (Project Management)
- Philip (LLM)
- Connor (Clustering)
- Beau (Full-Stack)
