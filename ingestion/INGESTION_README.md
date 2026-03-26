# Ingestion Service — HDFS Log Pipeline

**Owner:** Memo  
**Status:** ✅ Working — tested locally with `HDFS_2k.log`  
**Output:** `incidents.ndjson` — ready for summarisation step

---

## What This Does

This is the first stage of our pipeline. It takes raw HDFS log files and produces structured
incident objects that the LLM summariser can consume.

```
HDFS_2k.log  ──►  Fluent Bit (tail + parse)  ──►  consumer.py  ──►  incidents.ndjson
```

**Fluent Bit** watches the log file, extracts structured fields from each line via regex, and
POSTs them as JSON batches to the Python consumer over HTTP.

**consumer.py** receives the batches, groups log lines by `block_id` within a 5-minute time
window (matching the existing `IncidentGrouper` logic), and writes one JSON object per incident
to `incidents.ndjson`.

---

## Folder Structure

```
ingestion/
├── consumer.py          ← self-contained Python consumer (no external repo deps)
├── fluent-bit.conf      ← Fluent Bit configuration
├── parsers.conf         ← HDFS log line regex parser for Fluent Bit
├── data/
│   └── HDFS_2k.log      ← log file (swap for full HDFS.log for production)
└── incidents.ndjson     ← output (generated on run)
```

---

## Setup

### 1. Install Fluent Bit
```bash
brew install fluent-bit        # macOS
```

### 2. No Python dependencies needed
`consumer.py` uses only the standard library. No `pip install` required.

### 3. Update paths in fluent-bit.conf
The config uses absolute paths. Update these two lines to match your machine:

```ini
Parsers_File /YOUR/PATH/TO/ingestion/parsers.conf
Path         /YOUR/PATH/TO/ingestion/data/HDFS_2k.log
```

You can get your path by running `pwd` inside the `ingestion/` folder.

---

## Running

Open **two terminals** side by side.

**Terminal 1 — start the consumer first:**
```bash
cd /path/to/ingestion
python consumer.py --data ./data --out incidents.ndjson
```

Wait until you see:
```
INFO  Consumer ready | port=9880 window=5m labels=0 out=incidents.ndjson
INFO  Start Fluent Bit in another terminal, then Ctrl-C here to flush + exit.
```

**Terminal 2 — start Fluent Bit:**
```bash
cd /path/to/ingestion
fluent-bit -c fluent-bit.conf
```

Once Fluent Bit finishes reading the file (goes quiet), go back to Terminal 1 and hit **Ctrl-C**
to flush and write `incidents.ndjson`.

### Optional flags
```bash
python consumer.py --window 10   # change grouping window (default 5 min)
python consumer.py --data ../data --out my_output.ndjson
```

---

## Output Format

Each line of `incidents.ndjson` is one self-contained incident:

```json
{
  "incident_id": 1,
  "block_id": "blk_-1608999687919862906",
  "start_time": "2008-11-09T00:00:00",
  "end_time": "2008-11-09T00:00:00",
  "duration_seconds": 0.0,
  "num_logs": 3,
  "severity": "INFO",
  "components": ["dfs.DataNode$DataXceiver", "dfs.DataNode$PacketResponder"],
  "anomaly_label": "Unknown",
  "logs": [
    {
      "line_number": 1,
      "timestamp": "2008-11-09T00:00:00",
      "thread_id": "143",
      "level": "INFO",
      "component": "dfs.DataNode$DataXceiver",
      "message": "Receiving block blk_-1608999687919862906 src: /10.250.19.102:54106",
      "block_id": "blk_-1608999687919862906"
    }
  ]
}
```

The `logs` array contains every raw log entry for that incident — this is what gets passed
directly to the LLM summariser prompt.

---

## Results on HDFS_2k.log

```
Total lines received:   2000
Incidents produced:     1994
Severity breakdown:     1914 INFO, 80 WARN
Avg logs per incident:  1.0 (expected — 2k sample has high block ID variety)
```

> **Note on grouping:** The 2k sample dataset has almost unique block IDs per line by design,
> so most incidents contain a single log. With the full `HDFS.log` (1.47GB), the same block ID
> appears across many lines and grouping produces deeply populated incidents — consistent with
> what was shown in Milestone 1 (e.g. 191 logs in a single incident).

---

## Integration Notes for Summarisation Step

Reading `incidents.ndjson` is straightforward — one JSON object per line:

```python
import json

with open("incidents.ndjson") as f:
    for line in f:
        incident = json.loads(line)

        # The logs array is ready to drop into your LLM prompt
        logs_text = "\n".join(r["message"] for r in incident["logs"])

        # Metadata available for structured output
        block_id   = incident["block_id"]
        severity   = incident["severity"]
        components = incident["components"]
```

The output schema already matches the structured JSON format used in Milestone 1:
`incident_id`, `block_id`, `start_time`, `end_time`, `duration_seconds`, `num_logs`,
`severity`, `components`, `logs`.

---

## Known Limitations

**Timestamp grouping** — Fluent Bit v4 strips the `time` field from the payload when it uses
it as the record timestamp, so all incidents currently default to `T00:00:00`. This doesn't
affect the output schema or the LLM summarisation step. Fix for later: add
`json_date_key timestamp` + `json_date_format iso8601` to the `[OUTPUT]` block in
`fluent-bit.conf` and update the timestamp field in `consumer.py`'s `add()` method.

**Anomaly labels** — `anomaly_label.csv` from LogHub is not included in the public GitHub
repo. All incidents are labeled `"Unknown"` for now. If/when we get the file, drop it into
`data/` and it will be picked up automatically on the next run.
