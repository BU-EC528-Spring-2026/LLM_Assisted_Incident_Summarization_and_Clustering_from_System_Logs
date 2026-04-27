import requests
import csv
from dotenv import load_dotenv
import os

load_dotenv()

# ---- CONFIG ----
API_KEY = os.getenv("SUMM_API_KEY")
WORKSPACE_SLUG = "summarization"
BASE_URL = "http://localhost:3001"

def chat(message: str) -> str:
    url = f"{BASE_URL}/api/v1/workspace/{WORKSPACE_SLUG}/chat"
    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        },
        json={"message": message, "mode": "chat"}
    )
    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
        return None
    return response.json().get("textResponse", "No response text found")

def load_clusters(csv_path: str) -> dict:
    """Load CSV: Block ID -> Cluster ID mapping. Returns {cluster_id: list(block_ids)}"""
    clusters = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cluster_id = row.get("cluster_id")
            block_id = row.get("block_id")
            if cluster_id not in clusters:
                clusters[cluster_id] = []
            clusters[cluster_id].append(block_id)
    return clusters

def load_summaries(csv_path: str) -> dict:
    """Load CSV: Block ID -> Individual Summary. Returns {block_id: summary_text}"""
    summaries = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            block_id = row.get("block_id")
            summary = row.get("summary")
            summaries[block_id] = summary
    return summaries

def analyze_cluster(cluster_id: str, block_ids: list, summaries: dict, max_blocks: int = 50) -> str:
    """
    Send one cluster's info to AnythingLLM for analysis.
    Adds a safe limit to prevent sending massive context windows (like thousands of blocks) to the API.
    """
    block_details = ""
    
    # Optional limit to prevent payload from being too large for a single prompt
    sampled_blocks = block_ids[:max_blocks]
    
    for bid in sampled_blocks:
        summary = summaries.get(bid, "No summary available")
        block_details += f"  - {bid}: {summary}\n"

    truncation_note = f"\n(Note: Truncated to the first {max_blocks} blocks out of {len(block_ids)} to fit prompt limits.)" if len(block_ids) > max_blocks else ""

    prompt = f"""You are analyzing a cluster of anomalous system behavior logs.
Cluster ID: {cluster_id}
Total number of blocks in this cluster: {len(block_ids)}
{truncation_note}

Please analyze the summaries below and determine the overarching root cause or pattern for this cluster.

Block summaries:
{block_details}"""

    return chat(prompt)

if __name__ == "__main__":
    # ---- PATHS TO YOUR FILES (Assumes running inside pipeline_output directory) ----
    CLUSTERS_CSV = "clusters.csv"
    SUMMARIES_CSV = "summaries.csv"
    OUTPUT_CSV = "cluster_summaries.csv"

    print("=== AnythingLLM Cluster Analysis ===\n")

    # Ensure files exist before starting
    if not os.path.exists(CLUSTERS_CSV) or not os.path.exists(SUMMARIES_CSV):
        print(f"Error: Missing CSV files. Make sure {CLUSTERS_CSV} and {SUMMARIES_CSV} exist.")
        exit(1)

    # Load data
    clusters = load_clusters(CLUSTERS_CSV)
    summaries = load_summaries(SUMMARIES_CSV)

    print(f"Loaded {len(clusters)} clusters, {len(summaries)} block summaries\n")

    # Open the output CSV file for writing
    with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as out_file:
        writer = csv.writer(out_file)
        # Write the header row
        writer.writerow(["cluster_id", "cluster_summary"])

        # Analyze each cluster
        for cluster_id, block_ids in clusters.items():
            print(f"--- Cluster {cluster_id} ({len(block_ids)} blocks) ---")
            
            # Optional: skip cluster "-1" if you don't want to synthesize pure noise
            if cluster_id == "-1":
                print("Skipping noise cluster (-1)...\n")
                continue
                
            result = analyze_cluster(cluster_id, block_ids, summaries)
            print(f"Analysis: {result}\n")
            print("-" * 50)
            
            # Save the result to the CSV
            writer.writerow([cluster_id, result])
            
    print(f"Finished! Cluster summaries saved to {OUTPUT_CSV}")