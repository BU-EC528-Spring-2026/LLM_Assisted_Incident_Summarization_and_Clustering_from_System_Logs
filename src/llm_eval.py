"""
LLM Evaluation
Measures summarization quality across providers before fine-tuning.

Evaluation methods:
  1. Schema compliance — does the output match the expected JSON structure
  2. LLM-as-judge — GPT-4o scores summaries on faithfulness, completeness, conciseness

Usage:
    python llm_eval.py <provider> <clusters_json> [--samples 20]
    python llm_eval.py openai clusters.json
    python llm_eval.py ollama clusters.json --samples 50
"""

import sys
import json
import argparse
import os
from typing import Dict, List
from dotenv import load_dotenv
from jsonschema import validate, ValidationError
from llm_provider import get_provider

load_dotenv()

SUMMARY_SCHEMA = {
    "type": "object",
    "required": ["title", "affected_components", "root_cause_hypothesis", "severity"],
    "properties": {
        "title": {"type": "string"},
        "affected_components": {"type": "array", "items": {"type": "string"}},
        "root_cause_hypothesis": {"type": "string"},
        "severity": {"type": "string", "enum": ["low", "medium", "high", "critical"]}
    }
}


def check_schema(summary: dict) -> bool:
    try:
        validate(summary, SUMMARY_SCHEMA)
        return True
    except ValidationError:
        return False


def llm_judge(logs: str, summary: dict) -> dict:
    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    prompt = (
        "Rate this incident summary on 3 criteria (1-5 each):\n"
        "- Faithfulness: only states what's in the logs\n"
        "- Completeness: captures all key info\n"
        "- Conciseness: no unnecessary detail\n\n"
        f"Logs:\n{logs}\n\n"
        f"Summary:\n{json.dumps(summary)}\n\n"
        'Respond as JSON only: {"faithfulness": X, "completeness": X, "conciseness": X}'
    )

    response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"}
    )
    return json.loads(response.choices[0].message.content)


def build_logs_input(cluster: dict) -> str:
    log_lines = "\n".join(
        f"[{r.get('severity', 'INFO')}] {r.get('message', '')}"
        for r in cluster.get("representative_logs", [])
    )
    return (
        f"Cluster of {cluster['n_incidents']} incidents "
        f"({cluster['anomaly_frac']:.0%} anomalous).\n"
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


def evaluate_provider(provider_name: str, clusters_path: str, n_samples: int = 20):
    provider = get_provider(provider_name)

    with open(clusters_path) as f:
        clusters = json.load(f)

    to_eval = filter_noise(clusters)[:n_samples]
    print(f"Evaluating {provider_name} on {len(to_eval)} clusters\n")

    schema_pass = 0
    judge_scores: List[Dict] = []

    for cluster in to_eval:
        logs_input = build_logs_input(cluster)

        try:
            summary = provider.summarize(logs_input)
        except Exception as e:
            print(f"  Cluster {cluster['cluster_id']} FAILED: {e}")
            continue

        valid = check_schema(summary)
        if valid:
            schema_pass += 1

        scores = llm_judge(logs_input, summary)
        judge_scores.append(scores)

        print(f"  Cluster {cluster['cluster_id']}  |  "
              f"schema={'PASS' if valid else 'FAIL'}  |  "
              f"faith={scores.get('faithfulness')}  "
              f"comp={scores.get('completeness')}  "
              f"conc={scores.get('conciseness')}")

    total = len(to_eval)
    print(f"\n{'=' * 60}")
    print(f"Results: {provider_name}")
    print(f"{'=' * 60}")
    print(f"Schema compliance: {schema_pass}/{total} ({schema_pass/total*100:.0f}%)")

    if judge_scores:
        avg = {
            k: round(sum(s[k] for s in judge_scores) / len(judge_scores), 2)
            for k in judge_scores[0]
        }
        print(f"Avg faithfulness:  {avg.get('faithfulness')}")
        print(f"Avg completeness:  {avg.get('completeness')}")
        print(f"Avg conciseness:   {avg.get('conciseness')}")


def main():
    parser = argparse.ArgumentParser(description="Evaluate LLM summarization quality")
    parser.add_argument("provider", choices=["openai", "ollama", "mistral"],
                        help="LLM provider to evaluate")
    parser.add_argument("clusters_file", help="Path to clusters JSON")
    parser.add_argument("--samples", type=int, default=20,
                        help="Number of clusters to evaluate (default: 20)")
    args = parser.parse_args()

    evaluate_provider(args.provider, args.clusters_file, args.samples)


if __name__ == "__main__":
    main()
