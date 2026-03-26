"""
ingestion/consumer.py  (self-contained)
────────────────────────────────────────
Single-file ingestion consumer. No repo access needed.

Includes:
  - HDFSLogParser   (ported from hdfs_parser.py)
  - IncidentGrouper (ported from incident_grouper.py)
  - Anomaly label loader (from LogHub anomaly_label.csv)
  - Fluent Bit HTTP receiver
  - incidents.ndjson writer

Usage
-----
    # Terminal 1
    python consumer.py --data ../data --out incidents.ndjson

    # Terminal 2
    fluent-bit -c fluent-bit.conf
"""

import argparse
import csv
import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("consumer")


# ══════════════════════════════════════════════════════════════════════════════
# 1.  HDFS LOG PARSER
#     Ported from hdfs_parser.py in the team repo.
#     Parses a single raw log line into a structured dict.
# ══════════════════════════════════════════════════════════════════════════════

class HDFSLogParser:
    """
    Parses lines in the LogHub HDFS format:
        DATE    TIME  TID  LEVEL  COMPONENT: MESSAGE
        081109 203518  143  INFO  dfs.DataNode$DataXceiver: Receiving block blk_-1608...
    """

    def __init__(self):
        # Main line pattern: captures date / time / thread / level / component / message
        self.pattern = re.compile(
            r'^(\d{6})\s+(\d{6})\s+(\d+)\s+(\w+)\s+([\w.$]+):\s+(.+)$'
        )
        # Block ID pattern — may appear anywhere in the message
        self.block_pattern = re.compile(r'blk_-?\d+')

    def parse_line(self, line: str, line_number: int) -> Optional[Dict]:
        line = line.strip()
        if not line:
            return None

        match = self.pattern.match(line)
        if not match:
            return None  # silently skip malformed lines (header rows, blank, etc.)

        date_str, time_str, thread_id, level, component, message = match.groups()

        # Parse timestamp
        try:
            timestamp = datetime.strptime(f"{date_str} {time_str}", "%y%m%d %H%M%S")
            timestamp_str = timestamp.isoformat()
        except ValueError:
            # Fallback: manually build ISO string
            timestamp_str = (
                f"20{date_str[:2]}-{date_str[2:4]}-{date_str[4:6]}"
                f"T{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
            )

        # Extract block ID from message (if present)
        block_match = self.block_pattern.search(message)
        block_id = block_match.group(0) if block_match else None

        return {
            "line_number": line_number,
            "timestamp":   timestamp_str,
            "thread_id":   thread_id,
            "level":       level,
            "component":   component,
            "message":     message,
            "block_id":    block_id,
            "raw_line":    line,
        }

    def parse_lines(self, lines: List[str]) -> List[Dict]:
        """Parse a list of raw strings. Returns only successfully parsed entries."""
        parsed, failed = [], 0
        for i, line in enumerate(lines, 1):
            entry = self.parse_line(line, i)
            if entry:
                parsed.append(entry)
            else:
                failed += 1
        log.info("Parser: %d/%d lines parsed successfully (%d failed)",
                 len(parsed), len(lines), failed)
        return parsed


# ══════════════════════════════════════════════════════════════════════════════
# 2.  INCIDENT GROUPER
#     Ported from incident_grouper.py in the team repo.
#     Groups parsed log dicts into incidents by block_id + time window.
# ══════════════════════════════════════════════════════════════════════════════

LEVEL_PRIORITY = {"FATAL": 4, "ERROR": 3, "WARN": 2, "WARNING": 2, "INFO": 1, "DEBUG": 0}


class IncidentGrouper:
    """
    Groups logs into incidents.
    Rule: logs sharing the same block_id within time_window_minutes form one incident.
    """

    def __init__(self, time_window_minutes: int = 5):
        self.time_window = timedelta(minutes=time_window_minutes)

    def group_incidents(self, logs: List[Dict]) -> List[Dict]:
        # Only logs that have a block_id can be grouped
        logs_with_blocks = [l for l in logs if l.get("block_id")]
        log.info("Grouper: %d/%d logs have a block_id", len(logs_with_blocks), len(logs))

        # Bucket by block_id
        buckets: Dict[str, List[Dict]] = defaultdict(list)
        for entry in logs_with_blocks:
            buckets[entry["block_id"]].append(entry)

        log.info("Grouper: %d unique block IDs found", len(buckets))

        incidents = []
        incident_id = 1

        for block_id, block_logs in buckets.items():
            # Sort chronologically
            block_logs.sort(key=lambda x: x["timestamp"])

            current: List[Dict] = []
            window_start: Optional[datetime] = None

            for entry in block_logs:
                entry_time = datetime.fromisoformat(entry["timestamp"])

                if window_start is None:
                    window_start = entry_time
                    current = [entry]
                elif entry_time - window_start <= self.time_window:
                    current.append(entry)
                else:
                    # Close current window, open a new one
                    incidents.append(self._build(incident_id, block_id, current))
                    incident_id += 1
                    window_start = entry_time
                    current = [entry]

            if current:
                incidents.append(self._build(incident_id, block_id, current))
                incident_id += 1

        log.info(
            "Grouper: produced %d incidents (avg %.1f logs each)",
            len(incidents),
            len(logs_with_blocks) / len(incidents) if incidents else 0,
        )
        return incidents

    def _build(self, incident_id: int, block_id: str, logs: List[Dict]) -> Dict:
        timestamps  = [datetime.fromisoformat(l["timestamp"]) for l in logs]
        start, end  = min(timestamps), max(timestamps)
        severity    = max((l["level"] for l in logs), key=lambda x: LEVEL_PRIORITY.get(x, 0))
        components  = sorted(set(l["component"] for l in logs))

        return {
            "incident_id":      incident_id,
            "block_id":         block_id,
            "start_time":       start.isoformat(),
            "end_time":         end.isoformat(),
            "duration_seconds": (end - start).total_seconds(),
            "num_logs":         len(logs),
            "severity":         severity,
            "components":       components,
            "logs":             logs,   # full records — passed straight to LLM summariser
        }

    def get_stats(self, incidents: List[Dict]) -> Dict:
        if not incidents:
            return {}
        log_counts = [i["num_logs"]         for i in incidents]
        durations  = [i["duration_seconds"] for i in incidents]
        severities = [i["severity"]         for i in incidents]
        return {
            "total_incidents":       len(incidents),
            "total_logs":            sum(log_counts),
            "avg_logs_per_incident": round(sum(log_counts) / len(incidents), 1),
            "min_logs":              min(log_counts),
            "max_logs":              max(log_counts),
            "avg_duration_seconds":  round(sum(durations) / len(durations), 1),
            "severity_distribution": {
                lvl: severities.count(lvl) for lvl in set(severities)
            },
        }


# ══════════════════════════════════════════════════════════════════════════════
# 3.  ANOMALY LABEL LOADER
#     Reads LogHub's anomaly_label.csv and maps block_id -> "Normal"|"Anomaly"
# ══════════════════════════════════════════════════════════════════════════════

def load_anomaly_labels(data_dir: Path) -> Dict[str, str]:
    label_path = data_dir / "anomaly_label.csv"
    if not label_path.exists():
        log.warning("anomaly_label.csv not found at %s — 'anomaly_label' field will be 'Unknown'", label_path)
        return {}

    labels = {}
    with open(label_path, newline="") as f:
        for row in csv.DictReader(f):
            labels[row["BlockId"].strip()] = row["Label"].strip()

    log.info("Loaded %d anomaly labels from %s", len(labels), label_path)
    return labels


# ══════════════════════════════════════════════════════════════════════════════
# 4.  LINE BUFFER
#     Accumulates raw lines from Fluent Bit, then flushes through the pipeline.
# ══════════════════════════════════════════════════════════════════════════════

class LineBuffer:

    def __init__(self, time_window_minutes: int, anomaly_labels: Dict, out_path: str):
        self.parser   = HDFSLogParser()
        self.grouper  = IncidentGrouper(time_window_minutes=time_window_minutes)
        self.labels   = anomaly_labels
        self.out_path = out_path
        self._lines: List[str] = []

    def add(self, records: List[Dict]) -> None:
        for rec in records:
            # Fluent Bit may send either separate date/time fields or a unix timestamp
            date_str = str(rec.get("date", ""))
            time_str = str(rec.get("time", ""))
            
            timestamp = None
            
            # Try unix timestamp first (what Fluent Bit actually sends)
            for key in ("time_unix", "time", "@timestamp"):
                val = rec.get(key)
                if val and isinstance(val, (int, float)):
                    try:
                        timestamp = datetime.utcfromtimestamp(float(val)).isoformat()
                        break
                    except (ValueError, OSError):
                        continue
            
            # Fall back to date+time string fields
            if timestamp is None and date_str and time_str:
                try:
                    timestamp = datetime.strptime(f"{date_str} {time_str}", "%y%m%d %H%M%S").isoformat()
                except ValueError:
                    timestamp = f"20{date_str[:2]}-{date_str[2:4]}-{date_str[4:6]}T{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
            
            if timestamp is None:
                timestamp = datetime.utcnow().isoformat()

            message = rec.get("message", "") or rec.get("log", "")
            block_match = re.search(r'blk_-?\d+', message)
            block_id = block_match.group(0) if block_match else None

            entry = {
                "line_number": len(self._lines) + 1,
                "timestamp":   timestamp,
                "thread_id":   rec.get("thread_id", ""),
                "level":       rec.get("level", "INFO"),
                "component":   rec.get("component", ""),
                "message":     message,
                "block_id":    block_id,
                "raw_line":    message,
            }
            self._lines.append(entry)
        log.debug("Buffer: %d entries accumulated", len(self._lines))

    def flush(self) -> List[Dict]:
        if not self._lines:
            log.info("Buffer empty — nothing to flush")
            return []

        log.info("─" * 60)
        log.info("Flushing %d buffered entries…", len(self._lines))

    # Skip parsing — entries are already structured dicts from Fluent Bit
        parsed = [e for e in self._lines if e.get("block_id")]
        log.info("Entries with block_id: %d / %d", len(parsed), len(self._lines))

        incidents = self.grouper.group_incidents(parsed)

        labeled = 0
        for inc in incidents:
            label = self.labels.get(inc["block_id"])
            inc["anomaly_label"] = label or "Unknown"
            if label:
                labeled += 1
        log.info("Labeled %d/%d incidents with ground-truth anomaly labels", labeled, len(incidents))

        with open(self.out_path, "w") as f:
            for inc in incidents:
                f.write(json.dumps(inc, default=str) + "\n")
        log.info("Written → %s", self.out_path)

        stats = self.grouper.get_stats(incidents)
        stats["anomaly_count"] = sum(1 for i in incidents if i["anomaly_label"] == "Anomaly")
        stats["normal_count"]  = len(incidents) - stats["anomaly_count"]
        log.info("Stats:\n%s", json.dumps(stats, indent=2))
        log.info("─" * 60)

        return incidents


# ══════════════════════════════════════════════════════════════════════════════
# 5.  HTTP SERVER  (Fluent Bit → POST /ingest)
# ══════════════════════════════════════════════════════════════════════════════

_buffer: Optional[LineBuffer] = None


class FluentBitHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        if self.path != "/ingest":
            self._reply(404, "Not found")
            return

        length  = int(self.headers.get("Content-Length", 0))
        payload = self.rfile.read(length).decode("utf-8", errors="replace")

        records = []
        for line in payload.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass

        if records:
            _buffer.add(records)

        self._reply(200, "OK")

    def _reply(self, code: int, msg: str) -> None:
        body = msg.encode()
        self.send_response(code)
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass  # suppress per-request noise


# ══════════════════════════════════════════════════════════════════════════════
# 6.  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="HDFS ingestion consumer (self-contained)")
    ap.add_argument("--port",   type=int, default=9880,               help="HTTP port (must match fluent-bit.conf)")
    ap.add_argument("--window", type=int, default=5,                  help="Incident grouping window in minutes")
    ap.add_argument("--data",   type=str, default="../data",          help="Folder containing anomaly_label.csv")
    ap.add_argument("--out",    type=str, default="incidents.ndjson", help="Output file path")
    args = ap.parse_args()

    labels = load_anomaly_labels(Path(args.data))

    global _buffer
    _buffer = LineBuffer(
        time_window_minutes=args.window,
        anomaly_labels=labels,
        out_path=args.out,
    )

    log.info("Consumer ready  |  port=%d  window=%dm  labels=%d  out=%s",
             args.port, args.window, len(labels), args.out)
    log.info("Start Fluent Bit in another terminal, then Ctrl-C here to flush + exit.")

    server = HTTPServer(("127.0.0.1", args.port), FluentBitHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down…")
        _buffer.flush()


if __name__ == "__main__":
    main()