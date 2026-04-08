"""
LLM Summarization Runner
Summarizes cluster data using a swappable LLM backend.

Usage:
    python llmtest.py <clusters_json> [--provider openai|ollama|mistral]
"""

import sys
import json
import argparse
from llm_provider import get_provider


def build_logs_input(cluster: dict) -> str:
    log_lines = "\n".join(
        f"[{r.get('severity', 'INFO')}] {r.get('message', '')}"
        for r in cluster.get("representative_logs", [])
    )

    return (
        f"Cluster of {cluster['n_incidents']} incidents "
        f"({cluster['anomaly_frac']:.0%} anomalous).\n"
        f"Dominant failure type: {cluster['dominant_type']}.\n"
        f"Components: {', '.join(i['value'] for i in cluster.get('top_components', []))}.\n\n"
        f"Representative log messages:\n{log_lines}"
    )


def filter_noise(clusters: list) -> list:
    filtered = []
    for c in clusters:
        is_noise = c.get("is_noise")
        if isinstance(is_noise, str):
            is_noise = is_noise.lower() == "true"
        if not is_noise:
            filtered.append(c)
    return filtered


def main():
    parser = argparse.ArgumentParser(description="Run LLM summarization on cluster data")
    parser.add_argument("clusters_file", help="Path to clusters JSON")
    parser.add_argument("--provider", default="openai",
                        choices=["openai", "ollama", "mistral"],
                        help="LLM provider to use (default: openai)")
    args = parser.parse_args()

    with open(args.clusters_file) as f:
        clusters = json.load(f)

    print(f"Loaded {len(clusters)} clusters")
    print(f"Using provider: {args.provider}")

    provider = get_provider(args.provider)
    to_summarise = filter_noise(clusters)

    print(f"\nSummarising {len(to_summarise)} clusters...\n")

    for cluster in to_summarise:
        logs_input = build_logs_input(cluster)

        print(f"Cluster {cluster['cluster_id']}  |  "
              f"{cluster['n_incidents']} incidents  |  "
              f"{cluster['anomaly_frac']:.0%} anomaly")

        result = provider.summarize(logs_input)
        print(json.dumps(result, indent=2))
        print("=" * 80)


if __name__ == "__main__":
    main()
