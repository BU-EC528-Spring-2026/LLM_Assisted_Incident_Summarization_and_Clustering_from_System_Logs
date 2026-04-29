"""
LLM-as-Judge Evaluator for Log Summarization Pipeline
======================================================
Integrates with your existing AnythingLLM cluster analysis pipeline.
 
Takes the same inputs (clusters.csv + block_summaries JSON), captures the
AnythingLLM output, then sends each summary to Claude Sonnet for scoring.
 
Usage:
    # Full pipeline: run AnythingLLM then judge
    python llm_judge_evaluator.py \
        --csv clusters.csv \
        --summaries block_summaries_4o_mini_longer.json \
        --anthropic_key sk-ant-... \
        --output eval_results.json
 
    # Judge only (reuse saved AnythingLLM outputs):
    python llm_judge_evaluator.py \
        --presaved_results pipeline_outputs.json \
        --anthropic_key sk-ant-... \
        --output eval_results.json
"""
 
import requests
import json
import csv
import os
import sys
import time
import argparse
from pathlib import Path
 
try:
    import anthropic
except ImportError:
    print("Install the Anthropic SDK: pip install anthropic")
    sys.exit(1)
 
 
# =====================================================================
# AnythingLLM integration (same as your existing script)
# =====================================================================
 
ANYTHING_LLM_API_KEY = "KBVF8D3-QRGMJE3-PGBVTW4-HMZAXTX"
WORKSPACE_SLUG = "summarization"
BASE_URL = "http://localhost:3001"
 
 
def chat(message: str) -> str:
    """Send a message to AnythingLLM and return the response."""
    url = f"{BASE_URL}/api/v1/workspace/{WORKSPACE_SLUG}/chat"
    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {ANYTHING_LLM_API_KEY}",
            "Content-Type": "application/json",
        },
        json={"message": message, "mode": "chat"},
    )
    if response.status_code != 200:
        print(f"  AnythingLLM error {response.status_code}: {response.text}")
        return None
    return response.json()["textResponse"]
 
 
def load_clusters(csv_path: str) -> dict:
    """Load CSV: Block ID -> Cluster ID mapping. Returns {cluster_id: [block_ids]}"""
    clusters = {}
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cluster_id = row["cluster_id"]
            block_id = row["block_id"]
            if cluster_id not in clusters:
                clusters[cluster_id] = []
            clusters[cluster_id].append(block_id)
    return clusters
 
"""
def load_summaries(json_path: str) -> dict:
    with open(json_path, "r") as f:
        return json.load(f)
"""
 
def load_summaries(csv_path: str) -> dict:
    """Load CSV: block_id, summary -> {block_id: summary_text}"""
    summaries = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            block_id = row["block_id"]
            summary = row["summary"]
            summaries[block_id] = summary
    return summaries

def build_prompt(cluster_id: str, block_ids: list, summaries: dict) -> str:
    """Build the same prompt you send to AnythingLLM."""
    block_details = ""
    for bid in block_ids:
        summary = summaries.get(bid, "No summary available")
        block_details += f"  {bid}: {summary}\n"
 
    return f"""Cluster ID: {cluster_id}
Number of blocks: {len(block_ids)}
 
Block summaries:
{block_details}"""
 
 
def run_anythingllm_pipeline(clusters: dict, summaries: dict) -> list[dict]:
    """
    Run your existing AnythingLLM pipeline and capture results.
    Returns list of {cluster_id, prompt, summary, block_ids}.
    """
    results = []
    total = len(clusters)
 
    for i, (cluster_id, block_ids) in enumerate(clusters.items()):
        print(f"[{i+1}/{total}] Summarizing cluster {cluster_id} "
              f"({len(block_ids)} blocks)...")
 
        prompt = build_prompt(cluster_id, block_ids, summaries)
        llm_output = chat(prompt)
 
        if llm_output is None:
            print(f"  WARNING: No response for cluster {cluster_id}")
            llm_output = "[ERROR: No response from AnythingLLM]"
 
        results.append({
            "cluster_id": cluster_id,
            "num_blocks": len(block_ids),
            "block_ids": block_ids,
            "input_prompt": prompt,
            "summary": llm_output,
        })
 
    return results
 
 
# =====================================================================
# Claude LLM-as-Judge
# =====================================================================
 
JUDGE_SYSTEM_PROMPT = """\
You are an expert evaluator for an HDFS log anomaly summarization system.
 
You will receive:
1. INPUT CONTEXT — the block-level summaries that were fed to the summarizer LLM.
2. LLM SUMMARY — the cluster-level summary produced by the summarizer LLM.
 
Your job is to evaluate the cluster summary on four dimensions, each scored 1-5:
 
### Scoring Rubric
 
**Faithfulness** (Does the summary only state things supported by the input?)
- 5: Every claim is directly traceable to the input block summaries. No hallucinations.
- 4: Minor inference that is reasonable but not explicitly stated.
- 3: One or two unsupported claims that do not change the overall meaning.
- 2: Multiple unsupported claims or one significant hallucination.
- 1: Major fabrications or contradictions with the input evidence.
 
**Completeness** (Does the summary capture all important events and failures?)
- 5: All key block statuses, errors, state transitions, and patterns are covered.
- 4: Covers most key events; misses minor details only.
- 3: Misses one significant block event or failure pattern.
- 2: Misses multiple important events.
- 1: Only captures a small fraction of what happened.
 
**Conciseness** (Is the summary free of unnecessary repetition or filler?)
- 5: Every sentence adds value. No redundancy.
- 4: Mostly concise with minor repetition.
- 3: Some filler or repeated information.
- 2: Noticeably verbose or repetitive.
- 1: Extremely padded or redundant.
 
**Actionability** (Does it help an engineer understand root cause and next steps?)
- 5: Clearly identifies root cause, severity, affected blocks, and suggests actions.
- 4: Identifies root cause and severity but actions are vague.
- 3: Partially identifies root cause; no clear next steps.
- 2: Describes symptoms only; no root cause analysis.
- 1: Not useful for incident response.
 
### Response Format
Respond ONLY with valid JSON (no markdown fences, no preamble):
{
  "faithfulness": {"score": <1-5>, "justification": "<1-2 sentences>"},
  "completeness": {"score": <1-5>, "justification": "<1-2 sentences>"},
  "conciseness":  {"score": <1-5>, "justification": "<1-2 sentences>"},
  "actionability": {"score": <1-5>, "justification": "<1-2 sentences>"},
  "overall_notes": "<optional 1-2 sentence high-level comment>"
}
"""
 
 
def evaluate_summary(
    client: anthropic.Anthropic,
    input_prompt: str,
    summary: str,
    model: str = "claude-sonnet-4-6",
    max_retries: int = 3,
) -> dict:
    """Send a single summary + its input context to Claude for evaluation."""
    user_msg = f"""=== INPUT CONTEXT ===
{input_prompt}
 
=== LLM SUMMARY ===
{summary}
 
Evaluate the summary against the input context. Return JSON only."""
 
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=1024,
                system=JUDGE_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_msg}],
            )
 
            raw_text = response.content[0].text.strip()
            # Strip markdown fences if present
            if raw_text.startswith("```"):
                raw_text = raw_text.split("\n", 1)[1]
                raw_text = raw_text.rsplit("```", 1)[0].strip()
 
            return json.loads(raw_text)
 
        except json.JSONDecodeError as e:
            print(f"  [attempt {attempt+1}] JSON parse error: {e}")
            if attempt == max_retries - 1:
                return {"error": "json_parse_failure", "raw": raw_text}
 
        except anthropic.RateLimitError:
            wait = 2 ** (attempt + 1)
            print(f"  [attempt {attempt+1}] Rate limited, waiting {wait}s...")
            time.sleep(wait)
 
        except anthropic.APIError as e:
            print(f"  [attempt {attempt+1}] API error: {e}")
            if attempt == max_retries - 1:
                return {"error": "api_failure", "detail": str(e)}
 
    return {"error": "max_retries_exceeded"}
 
 
def evaluate_batch(
    client: anthropic.Anthropic,
    pipeline_results: list[dict],
    model: str = "claude-sonnet-4-6",
    delay: float = 1.0,
) -> list[dict]:
    """Evaluate all pipeline results with Claude judge."""
    evaluated = []
    total = len(pipeline_results)
 
    for i, item in enumerate(pipeline_results):
        cid = item["cluster_id"]
        print(f"[{i+1}/{total}] Judging cluster {cid}...")
 
        scores = evaluate_summary(
            client=client,
            input_prompt=item["input_prompt"],
            summary=item["summary"],
            model=model,
        )
 
        evaluated.append({
            "cluster_id": cid,
            "num_blocks": item["num_blocks"],
            "summary": item["summary"],
            "scores": scores,
        })
 
        if i < total - 1:
            time.sleep(delay)
 
    return evaluated
 
 
# =====================================================================
# Report generation
# =====================================================================
 
def generate_report(evaluated: list[dict]) -> dict:
    """Aggregate scores into a final report."""
    dimensions = ["faithfulness", "completeness", "conciseness", "actionability"]
    valid = [e for e in evaluated if "error" not in e.get("scores", {})]
 
    if not valid:
        return {"error": "no_valid_results", "per_cluster": evaluated}
 
    # Aggregate stats
    aggregate = {}
    for dim in dimensions:
        scores = [
            e["scores"][dim]["score"]
            for e in valid
            if dim in e.get("scores", {})
        ]
        if scores:
            aggregate[dim] = {
                "mean": round(sum(scores) / len(scores), 2),
                "min": min(scores),
                "max": max(scores),
                "count": len(scores),
            }
 
    # Score distribution
    distribution = {}
    for dim in dimensions:
        dist = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        for e in valid:
            if dim in e.get("scores", {}):
                dist[e["scores"][dim]["score"]] += 1
        distribution[dim] = dist
 
    # Flag low scores (<=2)
    flagged = []
    for e in valid:
        for dim in dimensions:
            if dim in e.get("scores", {}) and e["scores"][dim]["score"] <= 2:
                flagged.append({
                    "cluster_id": e["cluster_id"],
                    "dimension": dim,
                    "score": e["scores"][dim]["score"],
                    "justification": e["scores"][dim]["justification"],
                })
 
    return {
        "aggregate": aggregate,
        "score_distribution": distribution,
        "flagged_low_scores": flagged,
        "num_evaluated": len(valid),
        "num_errors": len(evaluated) - len(valid),
        "per_cluster": evaluated,
    }
 
 
def print_report(report: dict):
    """Print a formatted report to console."""
    print("\n" + "=" * 60)
    print("  LLM-AS-JUDGE EVALUATION REPORT")
    print("=" * 60)
 
    if "error" in report:
        print(f"  ERROR: {report['error']}")
        print("  No valid results to report.")
        print("=" * 60)
        return
 
    print(f"  Clusters evaluated: {report['num_evaluated']}")
    print(f"  Errors:             {report['num_errors']}")
    print()
 
    if "aggregate" in report:
        print("  AGGREGATE SCORES")
        print("  " + "-" * 50)
        for dim, stats in report["aggregate"].items():
            bar = "#" * int(stats["mean"] * 4)
            print(f"  {dim:15s}  mean={stats['mean']:.2f}  "
                  f"min={stats['min']}  max={stats['max']}  {bar}")
        print()
 
    if report.get("flagged_low_scores"):
        print(f"  FLAGGED LOW SCORES ({len(report['flagged_low_scores'])})")
        print("  " + "-" * 50)
        for flag in report["flagged_low_scores"]:
            print(f"  Cluster {flag['cluster_id']}: "
                  f"{flag['dimension']}={flag['score']}")
            print(f"    -> {flag['justification']}")
        print()
 
    print("=" * 60)
 
 
# =====================================================================
# CLI
# =====================================================================
 
def main():
    parser = argparse.ArgumentParser(
        description="LLM-as-Judge evaluator for AnythingLLM log summarization"
    )
 
    # Input: either run pipeline or use presaved results
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--csv",
        help="Path to clusters.csv (runs AnythingLLM pipeline first)",
    )
    input_group.add_argument(
        "--presaved_results",
        help="Path to previously saved pipeline results JSON (skip AnythingLLM)",
    )
 
    parser.add_argument(
        "--summaries",
        default="block_summaries_4o_mini_longer.json",
        help="Path to block summaries JSON (required with --csv)",
    )
    parser.add_argument(
        "--anthropic_key",
        default=os.environ.get("ANTHROPIC_API_KEY"),
        help="Claude API key (or set ANTHROPIC_API_KEY env var)",
    )
    parser.add_argument(
        "--model",
        default="claude-sonnet-4-6",
        help="Claude model for judging (default: claude-sonnet-4-6)",
    )
    parser.add_argument(
        "--output",
        default="eval_results.json",
        help="Output JSON file path",
    )
    parser.add_argument(
        "--save_pipeline",
        default="pipeline_outputs.json",
        help="Save AnythingLLM outputs here (reuse later with --presaved_results)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay between Claude API calls in seconds",
    )
 
    args = parser.parse_args()
 
    if not args.anthropic_key:
        parser.error("Provide --anthropic_key or set ANTHROPIC_API_KEY env var")
 
    # ---- Step 1: Get pipeline outputs ----
    if args.presaved_results:
        print(f"Loading presaved results from {args.presaved_results}...")
        with open(args.presaved_results) as f:
            pipeline_results = json.load(f)
    else:
        if not args.summaries:
            parser.error("--summaries is required when using --csv")
 
        print("=== Step 1: Running AnythingLLM Pipeline ===\n")
        clusters = load_clusters(args.csv)
        summaries = load_summaries(args.summaries)
        print(f"Loaded {len(clusters)} clusters, {len(summaries)} block summaries\n")
 
        pipeline_results = run_anythingllm_pipeline(clusters, summaries)
 
        # Save pipeline outputs so you can rerun judge without AnythingLLM
        with open(args.save_pipeline, "w") as f:
            json.dump(pipeline_results, f, indent=2)
        print(f"\nPipeline outputs saved to {args.save_pipeline}")
 
    # ---- Step 2: Judge with Claude ----
    print(f"\n=== Step 2: Claude Judge Evaluation ===\n")
    client = anthropic.Anthropic(api_key=args.anthropic_key)
    evaluated = evaluate_batch(
        client, pipeline_results, model=args.model, delay=args.delay
    )
 
    # ---- Step 3: Generate report ----
    report = generate_report(evaluated)
 
    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)
 
    print_report(report)
    print(f"Full results saved to: {args.output}")
 
 
if __name__ == "__main__":
    main()