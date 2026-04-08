# Ingestion Service — Multi-Dataset Log Pipeline

**Owner:** Memo  
**Status:** ✅ Working — tested with HDFS_2k.log. Thunderbird and Zookeeper parsers implemented, pending end-to-end test with their datasets.  
**Output:** `incidents.ndjson` — ready for summarisation step

---

## What This Does

First stage of the pipeline. Takes raw log files and produces structured incident objects
for the LLM summariser.

```
log file  ──►  Fluent Bit (tail + parse)  ──►  consumer.py  ──►  incidents.ndjson
```

Fluent Bit watches the log file, extracts fields via regex, and POSTs JSON batches to the
Python consumer. The consumer groups log lines into incidents using a dataset-specific
strategy and writes one JSON object per incident to `incidents.ndjson`.

---

## Supported Datasets

| Dataset     | Group By   | Log Format                                              |
|-------------|------------|---------------------------------------------------------|
| HDFS        | block_id   | `DATE TIME TID LEVEL COMPONENT: MESSAGE`                |
| Thunderbird | node       | `LABEL UNIX_TS DATE NODE ... HOSTNAME COMPONENT: MSG`   |
| Zookeeper   | time window| `DATE TIME,MS - LEVEL [THREAD:COMPONENT@LINE] - MSG`    |

Adding a new dataset requires only adding one entry to the `DATASETS` registry in
`consumer.py` — a `parse()` function and a `group_key()` function.

---

## Folder Structure

```
ingestion/
├── consumer.py          ← multi-dataset Python consumer
├── fluent-bit.conf      ← Fluent Bit config (update Path + Parser to switch datasets)
├── parsers.conf         ← regex parsers for HDFS, Thunderbird, Zookeeper
├── data/
│   └── HDFS_2k.log      ← drop any supported log file here
└── incidents.ndjson     ← output (generated on run)
```

---

## Setup

### 1. Install Fluent Bit
```bash
brew install fluent-bit
```

### 2. No Python dependencies needed
`consumer.py` uses only the standard library.

### 3. Update absolute paths in fluent-bit.conf
```ini
Parsers_File /YOUR/PATH/TO/ingestion/parsers.conf
Path         /YOUR/PATH/TO/ingestion/data/HDFS_2k.log
```
Run `pwd` inside the `ingestion/` folder to get your absolute path.

---

## Running

**Terminal 1 — start the consumer first:**
```bash
cd /path/to/ingestion
python consumer.py --dataset hdfs --out incidents.ndjson
```

**Terminal 2 — start Fluent Bit:**
```bash
cd /path/to/ingestion
fluent-bit -c fluent-bit.conf
```

Once Fluent Bit finishes reading the file, hit **Ctrl-C in Terminal 1** to flush and write
`incidents.ndjson`.

---

## Switching Datasets

Two changes needed — one in `fluent-bit.conf`, one in the run command.

**Thunderbird:**
```ini
# fluent-bit.conf
Path    /path/to/Thunderbird.log
Parser  thunderbird
```
```bash
python consumer.py --dataset thunderbird --out incidents.ndjson
```

**Zookeeper:**
```ini
# fluent-bit.conf
Path    /path/to/Zookeeper.log
Parser  zookeeper
```
```bash
python consumer.py --dataset zookeeper --out incidents.ndjson
```

---

## Optional Flags

```bash
python consumer.py --dataset hdfs --window 10 --out incidents.ndjson  # 10-min grouping window
python consumer.py --dataset hdfs --port 9881                          # change port
python consumer.py --dataset hdfs --data ./data                        # path to anomaly_label.csv
```

---

## Output Format

Each line of `incidents.ndjson` is one incident:

```json
{
  "incident_id": 1,
  "group_key": "blk_-1608999687919862906",
  "start_time": "2008-11-09T20:35:18",
  "end_time": "2008-11-09T20:35:35",
  "duration_seconds": 17.0,
  "num_logs": 12,
  "severity": "INFO",
  "components": ["dfs.DataNode$DataXceiver", "dfs.DataNode$PacketResponder"],
  "anomaly_label": "Unknown",
  "logs": [ ... ]
}
```

`group_key` is dataset-specific: block_id for HDFS, node for Thunderbird, "zookeeper" for ZK.  
`logs` array is passed directly to the LLM summariser.

---

## Integration with Summarisation Step

```python
import json

with open("incidents.ndjson") as f:
    for line in f:
        incident = json.loads(line)
        logs_text = "\n".join(r["message"] for r in incident["logs"])
        # pass logs_text to LLM prompt
```

---

## Results on HDFS_2k.log

```
Lines ingested:        2,000
Incidents produced:    1,994
Severity:              1,914 INFO, 80 WARN
Avg logs/incident:     1.0
```

> With the full HDFS.log (1.47GB), the same block IDs appear across many lines producing
> deeply grouped incidents — consistent with Milestone 1 results (e.g. 191 logs per incident).

---

## Known Limitations

**Timestamp grouping** — Fluent Bit v4 strips the `time` field from payloads when using it
as the record timestamp. Fixed in this version by adding `json_date_key` + `json_date_format
iso8601` to the `[OUTPUT]` block in `fluent-bit.conf`.

**Thunderbird / Zookeeper** — parsers and grouping logic are implemented and tested against
the log format spec. End-to-end ingestion with real dataset files is pending.

**Anomaly labels** — `anomaly_label.csv` is not available in the public LogHub GitHub repo.
All incidents labeled `"Unknown"` until the file is sourced. Drop it into `data/` and it
will be picked up automatically.
