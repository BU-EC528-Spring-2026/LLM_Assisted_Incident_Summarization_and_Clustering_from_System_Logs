"""
ingestion/consumer.py  (multi-dataset)

Supports HDFS, Thunderbird, and Zookeeper log datasets.
This consumer groups records into incidents and writes incidents.ndjson.

    python consumer.py --dataset hdfs         --out incidents.ndjson
    python consumer.py --dataset thunderbird  --out incidents.ndjson
    python consumer.py --dataset zookeeper    --out incidents.ndjson
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

LEVEL_PRIORITY = {"FATAL": 4, "ERROR": 3, "WARN": 2, "WARNING": 2, "INFO": 1, "DEBUG": 0}



def _ts(rec: dict) -> str:
    """Extract ISO timestamp from Fluent Bit record. Falls back to date field."""
    return rec.get("timestamp") or rec.get("date", "1970-01-01T00:00:00")


# HDFS 

def _parse_hdfs(rec: dict) -> Optional[dict]:
    block_id = rec.get("block_id", "").strip() or None
    return {
        "timestamp":  _ts(rec),
        "thread_id":  rec.get("thread_id", ""),
        "level":      rec.get("level", "INFO"),
        "component":  rec.get("component", ""),
        "message":    rec.get("message", ""),
        "block_id":   block_id,
    }

def _group_hdfs(entry: dict) -> Optional[str]:
    """Group by block_id — logs without one are discarded."""
    return entry.get("block_id")


#  Thunderbird 

def _parse_thunderbird(rec: dict) -> Optional[dict]:
    return {
        "timestamp":  _ts(rec),
        "node":       rec.get("node", ""),
        "hostname":   rec.get("hostname", ""),
        "level":      rec.get("level", "INFO"),
        "component":  rec.get("component", ""),
        "message":    rec.get("message", ""),
        "label":      rec.get("label", "-"),   # "-" = normal, else anomaly tag
    }

def _group_thunderbird(entry: dict) -> Optional[str]:
    """Group by node — each physical node is treated as an incident context."""
    return entry.get("node") or "unknown_node"


# Zookeeper 

def _parse_zookeeper(rec: dict) -> Optional[dict]:
    return {
        "timestamp":  _ts(rec),
        "thread":     rec.get("thread", ""),
        "level":      rec.get("level", "INFO"),
        "component":  rec.get("component", ""),
        "message":    rec.get("message", ""),
    }

def _group_zookeeper(entry: dict) -> Optional[str]:
    """Zookeeper is single-node — group everything into one rolling window."""
    return "zookeeper"


# Registry

DATASETS = {
    "hdfs": {
        "parse":     _parse_hdfs,
        "group_key": _group_hdfs,
        "desc":      "HDFS (LogHub HDFS_v1) — groups by block_id",
    },
    "thunderbird": {
        "parse":     _parse_thunderbird,
        "group_key": _group_thunderbird,
        "desc":      "Thunderbird supercomputer logs — groups by node",
    },
    "zookeeper": {
        "parse":     _parse_zookeeper,
        "group_key": _group_zookeeper,
        "desc":      "Zookeeper logs — groups by time window",
    },
}



class IncidentGrouper:

    def __init__(self, time_window_minutes: int, group_key_fn):
        self.time_window   = timedelta(minutes=time_window_minutes)
        self.group_key_fn  = group_key_fn

    def group(self, entries: List[dict]) -> List[dict]:
        buckets: Dict[str, List[dict]] = defaultdict(list)
        for e in entries:
            key = self.group_key_fn(e)
            if key:
                buckets[key].append(e)

        log.info("Grouper: %d unique group keys across %d entries", len(buckets), len(entries))

        incidents, iid = [], 1
        for key, group_logs in buckets.items():
            group_logs.sort(key=lambda x: x["timestamp"])
            current, window_start = [], None

            for entry in group_logs:
                try:
                    t = datetime.fromisoformat(entry["timestamp"])
                except ValueError:
                    t = datetime(1970, 1, 1)

                if window_start is None:
                    window_start, current = t, [entry]
                elif t - window_start <= self.time_window:
                    current.append(entry)
                else:
                    incidents.append(self._build(iid, key, current))
                    iid += 1
                    window_start, current = t, [entry]

            if current:
                incidents.append(self._build(iid, key, current))
                iid += 1

        log.info("Grouper: produced %d incidents", len(incidents))
        return incidents

    def _build(self, iid: int, group_key: str, logs: List[dict]) -> dict:
        timestamps = []
        for l in logs:
            try:
                timestamps.append(datetime.fromisoformat(l["timestamp"]))
            except ValueError:
                pass

        start = min(timestamps) if timestamps else datetime(1970, 1, 1)
        end   = max(timestamps) if timestamps else datetime(1970, 1, 1)
        severity = max((l["level"] for l in logs), key=lambda x: LEVEL_PRIORITY.get(x, 0))
        components = sorted(set(l.get("component", "") for l in logs if l.get("component")))

        return {
            "incident_id":      iid,
            "group_key":        group_key,
            "start_time":       start.isoformat(),
            "end_time":         end.isoformat(),
            "duration_seconds": (end - start).total_seconds(),
            "num_logs":         len(logs),
            "severity":         severity,
            "components":       components,
            "logs":             logs,
        }



def load_anomaly_labels(data_dir: Path) -> Dict[str, str]:
    label_path = data_dir / "anomaly_label.csv"
    if not label_path.exists():
        log.warning("anomaly_label.csv not found — 'anomaly_label' will be 'Unknown'")
        return {}
    labels = {}
    with open(label_path, newline="") as f:
        for row in csv.DictReader(f):
            labels[row["BlockId"].strip()] = row["Label"].strip()
    log.info("Loaded %d anomaly labels", len(labels))
    return labels


class LineBuffer:

    def __init__(self, dataset: str, time_window_minutes: int, anomaly_labels: dict, out_path: str):
        cfg = DATASETS[dataset]
        self.parse_fn  = cfg["parse"]
        self.grouper   = IncidentGrouper(time_window_minutes, cfg["group_key"])
        self.labels    = anomaly_labels
        self.out_path  = out_path
        self.dataset   = dataset
        self._entries: List[dict] = []

    def add(self, records: List[dict]) -> None:
        for rec in records:
            entry = self.parse_fn(rec)
            if entry:
                self._entries.append(entry)
        log.debug("Buffer: %d entries", len(self._entries))

    def flush(self) -> List[dict]:
        if not self._entries:
            log.info("Buffer empty — nothing to flush")
            return []

        log.info("─" * 60)
        log.info("Flushing %d entries  [dataset=%s]", len(self._entries), self.dataset)

        incidents = self.grouper.group(self._entries)

        # Enrich with anomaly labels (HDFS uses block_id / group_key as label key)
        labeled = 0
        for inc in incidents:
            label = self.labels.get(inc["group_key"])
            inc["anomaly_label"] = label or "Unknown"
            if label:
                labeled += 1

        if self.labels:
            log.info("Labeled %d/%d incidents", labeled, len(incidents))

        with open(self.out_path, "w") as f:
            for inc in incidents:
                f.write(json.dumps(inc, default=str) + "\n")

        log.info("Written → %s", self.out_path)

        severity_dist = defaultdict(int)
        for inc in incidents:
            severity_dist[inc["severity"]] += 1
        stats = {
            "total_incidents": len(incidents),
            "total_logs":      sum(i["num_logs"] for i in incidents),
            "avg_logs":        round(sum(i["num_logs"] for i in incidents) / len(incidents), 1) if incidents else 0,
            "severity":        dict(severity_dist),
        }
        log.info("Stats: %s", json.dumps(stats))
        log.info("─" * 60)
        return incidents


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

    def _reply(self, code, msg):
        body = msg.encode()
        self.send_response(code)
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass



def main():
    ap = argparse.ArgumentParser(description="Multi-dataset log ingestion consumer")
    ap.add_argument("--dataset", choices=DATASETS.keys(), default="hdfs",
                    help="Log dataset format (default: hdfs)")
    ap.add_argument("--port",   type=int, default=9880,               help="HTTP port")
    ap.add_argument("--window", type=int, default=5,                  help="Incident grouping window (minutes)")
    ap.add_argument("--data",   type=str, default="./data",           help="Data dir (for anomaly_label.csv)")
    ap.add_argument("--out",    type=str, default="incidents.ndjson", help="Output file")
    args = ap.parse_args()

    log.info("Dataset : %s — %s", args.dataset, DATASETS[args.dataset]["desc"])

    labels = load_anomaly_labels(Path(args.data))

    global _buffer
    _buffer = LineBuffer(
        dataset=args.dataset,
        time_window_minutes=args.window,
        anomaly_labels=labels,
        out_path=args.out,
    )

    log.info("Consumer ready  |  port=%d  window=%dm  out=%s", args.port, args.window, args.out)
    log.info("Start Fluent Bit in another terminal, then Ctrl-C here to flush + exit.")

    server = HTTPServer(("127.0.0.1", args.port), FluentBitHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down…")
        _buffer.flush()


if __name__ == "__main__":
    main()