from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from http.client import HTTPConnection, HTTPSConnection
from urllib.parse import urlparse

from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import ChatOllama
from tenacity import retry, stop_after_attempt, wait_exponential

STRUCTURE_PROMPT_TEMPLATE = """CV parser. Return ONLY valid JSON, no markdown.
Schema: {{"contact":{{"name":"","email":"","phone":"","linkedin":"","location":""}},"education":[{{"institution":"","establishment":"","typeEducation":null,"dateGraduation":null}}],"experience":[{{"role":"","company":"","location":"","startDate":"","endDate":"","description":""}}],"certifications":[{{"title":"","issuer":"","issueDate":"","expiryDate":"","description":""}}],"achievement":[{{"projectName":"","description":"","startDate":null,"endDate":null}}],"skills":{{"score":null,"catalogId":null,"languages":[{{"language":"","proficiency":null}}],"technical":[],"soft":[]}},"summary":""}}
Enums (or null): typeEducation=LICENCE|MASTER|DOCTORAT|INGENIEUR|BTS|DUT|FORMATION_PROFESSIONNELLE; score=BASIC|INTERMEDIATE|ADVANCED|EXPERT; proficiency=A1|A2|B1|B2|C1|C2|NATIVE; language=FRENCH|ARABIC|ENGLISH|SPANISH|GERMAN etc.
Rules: dateGraduation=year int (e.g. 2023). Dates DD/MM/YYYY when possible. null for missing scalars, [] for missing arrays. No invention. Brief descriptions. catalogId only if explicit.

CV TEXT:
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
        parsed = urlparse(self._base_url)
        self._api_host = parsed.hostname or "localhost"
        self._api_port = parsed.port or (443 if parsed.scheme == "https" else 11434)
        self._api_scheme = parsed.scheme or "http"
        self._conn: HTTPConnection | HTTPSConnection | None = None
        self._conn_lock = threading.Lock()

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

    def _get_conn(self) -> HTTPConnection | HTTPSConnection:
        with self._conn_lock:
            if self._conn is not None:
                return self._conn
            if self._api_scheme == "https":
                conn = HTTPSConnection(self._api_host, self._api_port, timeout=self._timeout_seconds)
            else:
                conn = HTTPConnection(self._api_host, self._api_port, timeout=self._timeout_seconds)
            self._conn = conn
            return conn

    def _post_json(self, path: str, payload: dict) -> dict:
        body = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json", "Connection": "keep-alive"}
        conn = self._get_conn()
        try:
            conn.request("POST", path, body=body, headers=headers)
            resp = conn.getresponse()
            raw = resp.read().decode("utf-8")
        except Exception:
            with self._conn_lock:
                self._conn = None
            raise
        return json.loads(raw)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8), reraise=True)
    def call_structured_cv(self, raw_text: str) -> str:
        if self._breaker.is_open():
            raise RuntimeError("Circuit breaker is open for Ollama")
        try:
            prompt = STRUCTURE_PROMPT_TEMPLATE.format(raw_text=raw_text)
            num_predict = int(os.getenv("OLLAMA_NUM_PREDICT", "900"))
            num_thread = int(os.getenv("OLLAMA_LLAMA_NUM_THREAD", os.getenv("OLLAMA_NUM_THREAD", "4")))
            num_ctx = int(os.getenv("OLLAMA_NUM_CTX", "4096"))
            payload = {
                "model": self._model_name,
                "prompt": prompt,
                "format": "json",
                "stream": False,
                "options": {
                    "num_predict": num_predict,
                    "temperature": 0,
                    "top_p": 0.9,
                    "num_thread": max(1, num_thread),
                    "num_ctx": max(2048, num_ctx),
                },
            }
            obj = self._post_json("/api/generate", payload)
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
