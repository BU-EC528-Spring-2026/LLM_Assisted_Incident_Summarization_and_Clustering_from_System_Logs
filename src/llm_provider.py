"""
LLM Provider Interface
Swappable summarization backends for incident log analysis
"""

from abc import ABC, abstractmethod
import json
import os
import requests
from dotenv import load_dotenv

load_dotenv()

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


class LLMProvider(ABC):

    @abstractmethod
    def summarize(self, logs: str) -> dict:
        pass


class OpenAIProvider(LLMProvider):

    def __init__(self, model="gpt-4o-mini"):
        from openai import OpenAI
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = model

    def summarize(self, logs: str) -> dict:
        response = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Logs:\n{logs}"}
            ],
            response_format={"type": "json_object"}
        )
        return json.loads(response.choices[0].message.content)


class OllamaProvider(LLMProvider):

    def __init__(self, model="llama3.1", base_url="http://localhost:11434"):
        self.model = model
        self.base_url = base_url

    def summarize(self, logs: str) -> dict:
        response = requests.post(
            f"{self.base_url}/api/chat",
            json={
                "model": self.model,
                "stream": False,
                "format": "json",
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": f"Logs:\n{logs}"}
                ]
            }
        )
        return json.loads(response.json()["message"]["content"])


class MistralProvider(LLMProvider):

    def __init__(self, model="mistral-small-latest"):
        self.model = model
        self.api_key = os.getenv("MISTRAL_API_KEY")

    def summarize(self, logs: str) -> dict:
        response = requests.post(
            "https://api.mistral.ai/v1/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={
                "model": self.model,
                "temperature": 0,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": f"Logs:\n{logs}"}
                ]
            }
        )
        return json.loads(response.json()["choices"][0]["message"]["content"])


PROVIDERS = {
    "openai": OpenAIProvider,
    "ollama": OllamaProvider,
    "mistral": MistralProvider,
}


def get_provider(name: str) -> LLMProvider:
    if name not in PROVIDERS:
        raise ValueError(f"Unknown provider '{name}'. Options: {list(PROVIDERS.keys())}")
    return PROVIDERS[name]()
