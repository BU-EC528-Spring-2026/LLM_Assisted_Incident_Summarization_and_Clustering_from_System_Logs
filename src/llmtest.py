from openai import OpenAI
from dotenv import load_dotenv
import os
import json

# Load API key
load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_PROMPT = """
You are an SRE (Site Reliability Engineering) incident summarization assistant.

INPUT:
- You will be given log lines (string or array) representing ONE production incident.

OUTPUT:
Return ONLY valid JSON with exactly these keys:
{
  "title": "Short, standardized incident title",
  "affected_components": ["component1", "component2"],
  "root_cause_hypothesis": "Most likely explanation based ONLY on the logs",
  "severity": "low | medium | high | critical"
}

RULES:
1) Use ONLY information present in the logs. Do NOT invent systems, causes, or timelines.
2) If the root cause is unclear, set "root_cause_hypothesis" to "unknown".
3) "affected_components" must list components explicitly mentioned in the logs (e.g., Database, API, Redis, Kubernetes).
4) Keep the "title" consistent and concise. Prefer standardized phrasing (avoid creative rewording).
   - Title format: "<Primary Issue> affecting <Primary Component>".
5) Severity must follow these rules:
   - critical: full outage, data loss, security breach, or widespread transaction failures
   - high: widespread production errors (5xx spikes), major degradation impacting many users
   - medium: partial impact, intermittent failures, or auto-recovered incidents
   - low: warnings/minor errors with little/no user impact

Return JSON only. No additional text.
"""

def summarize_incident(logs: str):
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Logs:\n{logs}"}
        ],
        response_format={"type": "json_object"}
    )

    return json.loads(response.choices[0].message.content)



if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            clusters = json.load(f)
        
        print(f"Loaded {len(clusters)} clusters")
        
        # Check actual type of is_noise
        if clusters:
            print(f"Type of is_noise: {type(clusters[0]['is_noise'])}")
            print(f"Value: {clusters[0]['is_noise']}")
        
        # Safe filtering that handles both boolean and string
        to_summarise = []
        for c in clusters:
            is_noise = c.get("is_noise")
            # Convert string to boolean if needed
            if isinstance(is_noise, str):
                is_noise = is_noise.lower() == "true"
            
            if not is_noise:
                to_summarise.append(c)
        
        print(f"\nSummarising {len(to_summarise)} clusters...\n")
        
        for cluster in to_summarise:
            # Build log string from representative logs
            log_lines = "\n".join(
                f"[{r.get('severity', 'INFO')}] {r.get('message', '')}"
                for r in cluster.get("representative_logs", [])
            )
            
            # Prepend cluster context
            logs_input = (
                f"Cluster of {cluster['n_incidents']} incidents "
                f"({cluster['anomaly_frac']:.0%} anomalous).\n"
                f"Dominant failure type: {cluster['dominant_type']}.\n"
                f"Components: {', '.join(i['value'] for i in cluster.get('top_components', []))}.\n\n"
                f"Representative log messages:\n{log_lines}"
            )
            
            print(f"Cluster {cluster['cluster_id']}  |  "
                  f"{cluster['n_incidents']} incidents  |  "
                  f"{cluster['anomaly_frac']:.0%} anomaly")
            
            result = summarize_incident(logs_input)
            print(json.dumps(result, indent=2))
            print("=" * 80)