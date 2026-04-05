from __future__ import annotations

import json
import re

from app.llm.ollama_client import OllamaClient
from app.schemas import CvExtractionResult


def truncate_text(text: str, max_chars: int = 6000) -> str:
    """
    Keep the most informative parts while bounding prompt size:
    - Start of CV tends to contain contact + summary
    - End often contains skills / certifications / projects
    """
    clean = text.strip()
    if len(clean) <= max_chars:
        return clean
    head = clean[: max_chars // 2].rstrip()
    tail = clean[-(max_chars - len(head)) :].lstrip()
    return f"{head}\n...\n{tail}".strip()


class LlmExtractionError(RuntimeError):
    pass


class LlmExtractor:
    def __init__(self, model_name: str, base_url: str, timeout_seconds: int = 180) -> None:
        self._client = OllamaClient(model_name=model_name, base_url=base_url, timeout_seconds=timeout_seconds)

    @property
    def circuit_open(self) -> bool:
        return self._client.breaker_open

    def structure_cv(self, raw_text: str) -> CvExtractionResult:
        content = self._client.call_structured_cv(truncate_text(raw_text, max_chars=6000))
        parsed_json = self._parse_json(content)
        parsed_json = self._normalize_llm_payload(parsed_json)

        try:
            return CvExtractionResult.model_validate(parsed_json)
        except Exception as exc:
            raise LlmExtractionError("LLM returned invalid schema content") from exc

    def _normalize_llm_payload(self, payload: dict) -> dict:
        if not isinstance(payload, dict):
            return payload

        certifications = payload.get("certifications")
        if certifications is None:
            payload["certifications"] = []
        elif isinstance(certifications, dict):
            payload["certifications"] = [certifications]

        projects = payload.get("projects")
        if projects is None:
            payload["projects"] = []
        elif isinstance(projects, dict):
            payload["projects"] = [projects]

        skills = payload.get("skills")
        if isinstance(skills, dict):
            for key in ("technical", "soft", "languages"):
                skills[key] = self._normalize_string_list(skills.get(key))
        return payload

    def _normalize_string_list(self, values) -> list[str]:
        if values is None:
            return []
        if not isinstance(values, list):
            values = [values]

        normalized: list[str] = []
        seen: set[str] = set()
        for item in values:
            value = self._stringify_skill_item(item)
            if not value:
                continue
            dedupe_key = value.casefold()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            normalized.append(value)
        return normalized

    def _stringify_skill_item(self, item) -> str | None:
        if isinstance(item, str):
            text = item.strip()
            return text or None
        if isinstance(item, dict):
            for key in ("name", "language", "value", "label", "title"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            return None
        return None

    def _parse_json(self, raw: str) -> dict:
        text = raw.strip()
        for candidate in self._json_candidates(text):
            parsed = self._try_json_parse(candidate)
            if isinstance(parsed, dict):
                return parsed

        # Some models occasionally omit outer braces and return key/value lines.
        maybe_object_body = text.strip().strip(",")
        if maybe_object_body.startswith('"') and not maybe_object_body.startswith("{"):
            parsed = self._try_json_parse("{" + maybe_object_body + "}")
            if isinstance(parsed, dict):
                return parsed

        snippet = text[:600].replace("\n", "\\n")
        raise LlmExtractionError(f"Unable to parse JSON from LLM response. LLM snippet: {snippet}")

    def _json_candidates(self, text: str) -> list[str]:
        candidates: list[str] = [text]

        fenced = re.search(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.DOTALL | re.IGNORECASE)
        if fenced:
            candidates.append(fenced.group(1).strip())

        first_obj = self._extract_first_balanced_object(text)
        if first_obj:
            candidates.append(first_obj)

        return candidates

    def _extract_first_balanced_object(self, text: str) -> str | None:
        start = text.find("{")
        if start == -1:
            return None
        depth = 0
        in_string = False
        escape = False
        for i in range(start, len(text)):
            ch = text[i]
            if escape:
                escape = False
                continue
            if ch == "\\":
                escape = True
                continue
            if ch == '"':
                in_string = not in_string
                continue
            if in_string:
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start : i + 1]
        return None

    def _try_json_parse(self, candidate: str) -> dict | None:
        normalized = self._normalize_json_candidate(candidate)
        try:
            parsed = json.loads(normalized)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    def _normalize_json_candidate(self, text: str) -> str:
        s = text.strip()
        s = s.replace("\u201c", '"').replace("\u201d", '"').replace("\u2019", "'")
        # Remove trailing commas before closing braces/brackets.
        s = re.sub(r",\s*([}\]])", r"\1", s)
        return s
