from __future__ import annotations

import json
import os
import threading
import time
from urllib import request
from dataclasses import dataclass

from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import ChatOllama
from tenacity import retry, stop_after_attempt, wait_exponential

STRUCTURE_PROMPT_TEMPLATE = """You are a CV parser for an HR system.
Return ONLY a valid JSON object matching this exact schema (no markdown, no preamble):

{{
  "contact": {{"name": "", "email": "", "phone": "", "linkedin": "", "location": ""}},
  "education": [
    {{
      "institution": "",
      "establishment": "",
      "typeEducation": null,
      "dateGraduation": null
    }}
  ],
  "experience": [
    {{
      "role": "",
      "company": "",
      "location": "",
      "startDate": "",
      "endDate": "",
      "description": ""
    }}
  ],
  "certifications": [
    {{
      "title": "",
      "issuer": "",
      "issueDate": "",
      "expiryDate": "",
      "description": ""
    }}
  ],
  "achievement": [
    {{
      "projectName": "",
      "description": "",
      "startDate": null,
      "endDate": null
    }}
  ],
  "skills": {{
    "score": null,
    "catalogId": null,
    "languages": [{{"language": "", "proficiency": null}}],
    "technical": [],
    "soft": []
  }},
  "summary": ""
}}

Enums (use EXACTLY one of these strings, or null if unknown):
- education[].typeEducation: LICENCE | MASTER | DOCTORAT | INGENIEUR | BTS | DUT | FORMATION_PROFESSIONNELLE
- skills.score: BASIC | INTERMEDIATE | ADVANCED | EXPERT
- skills.languages[].proficiency: A1 | A2 | B1 | B2 | C1 | C2 | NATIVE
- skills.languages[].language: prefer uppercase locale keys when clear, e.g. FRENCH, ARABIC, ENGLISH

Rules:
- dateGraduation: graduation year as integer (e.g. 2023) when a single year is clear; else null.
- Dates for experience/achievement: use DD/MM/YYYY when day is known, else month/year or year as in the CV.
- If a field is not found: use null for scalars and [] for arrays.
- Do not invent information not present in the text.
- Keep each experience "description" brief (max ~2 lines); omit filler.
- certifications.description: short optional note if the CV provides one.
- achievement: notable projects (replace old "projects"); put tech stack in description if no separate field.
- skills.catalogId: integer ID only if explicitly stated in the CV; otherwise null.
- skills.technical / skills.soft: keyword lists as before.

RAW CV TEXT:
{raw_text}
"""


@dataclass
class _CircuitState:
    fail_count: int = 0
    opened_at: float = 0.0


class CircuitBreaker:
    def __init__(self, fail_max: int = 3, reset_timeout: int = 60) -> None:
        self.fail_max = fail_max
        self.reset_timeout = reset_timeout
        self._state = _CircuitState()
        self._lock = threading.Lock()

    def is_open(self) -> bool:
        with self._lock:
            if self._state.fail_count < self.fail_max:
                return False
            if time.time() - self._state.opened_at >= self.reset_timeout:
                self._state.fail_count = 0
                self._state.opened_at = 0.0
                return False
            return True

    def on_success(self) -> None:
        with self._lock:
            self._state.fail_count = 0
            self._state.opened_at = 0.0

    def on_failure(self) -> None:
        with self._lock:
            self._state.fail_count += 1
            if self._state.fail_count >= self.fail_max:
                self._state.opened_at = time.time()


class OllamaClient:
    def __init__(self, model_name: str, base_url: str, timeout_seconds: int = 180) -> None:
        self._model_name = model_name
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._model = ChatOllama(model=model_name, base_url=base_url, timeout=timeout_seconds)
        self._breaker = CircuitBreaker(fail_max=3, reset_timeout=60)

    @property
    def breaker_open(self) -> bool:
        return self._breaker.is_open()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8), reraise=True)
    def call(self, prompt_template: ChatPromptTemplate, params: dict[str, str]) -> str:
        if self._breaker.is_open():
            raise RuntimeError("Circuit breaker is open for Ollama")
        chain = prompt_template | self._model
        try:
            llm_response = chain.invoke(params)
            content = llm_response.content if isinstance(llm_response.content, str) else str(llm_response.content)
            self._breaker.on_success()
            return content
        except Exception:
            self._breaker.on_failure()
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8), reraise=True)
    def call_structured_cv(self, raw_text: str) -> str:
        if self._breaker.is_open():
            raise RuntimeError("Circuit breaker is open for Ollama")
        try:
            prompt = STRUCTURE_PROMPT_TEMPLATE.format(raw_text=raw_text)
            num_predict = int(os.getenv("OLLAMA_NUM_PREDICT", "1400"))
            num_thread = int(os.getenv("OLLAMA_LLAMA_NUM_THREAD", os.getenv("OLLAMA_NUM_THREAD", "4")))
            num_ctx = int(os.getenv("OLLAMA_NUM_CTX", "8192"))
            options: dict = {
                "num_predict": num_predict,
                "temperature": 0,
                "top_p": 0.9,
                "num_thread": max(1, num_thread),
                "num_ctx": max(2048, num_ctx),
            }
            payload = {
                "model": self._model_name,
                "prompt": prompt,
                "format": "json",
                "stream": False,
                "options": options,
            }
            req = request.Request(
                url=f"{self._base_url}/api/generate",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with request.urlopen(req, timeout=self._timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
            obj = json.loads(raw)
            if isinstance(obj, dict) and obj.get("error"):
                raise RuntimeError(f"Ollama error: {obj.get('error')}")
            content = str(obj.get("response", ""))
            if not content.strip():
                raise RuntimeError("Ollama returned empty response")
            self._breaker.on_success()
            return content
        except Exception:
            self._breaker.on_failure()
            raise
