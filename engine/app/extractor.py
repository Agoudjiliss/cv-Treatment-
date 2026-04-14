from __future__ import annotations

import json
import re
from difflib import SequenceMatcher

from app.llm.ollama_client import SKILLS_CATALOG_CSV, OllamaClient
from app.schemas import CvExtractionResult


def _parse_catalog_csv() -> dict[int, tuple[str, str]]:
    catalog: dict[int, tuple[str, str]] = {}
    for line in SKILLS_CATALOG_CSV.splitlines():
        line = line.strip()
        if not line or line.lower().startswith("id,"):
            continue
        parts = [p.strip() for p in line.split(",", 2)]
        if len(parts) != 3:
            continue
        id_str, name, category = parts
        try:
            cid = int(id_str)
        except ValueError:
            continue
        if not name:
            continue
        catalog[cid] = (name, category)
    return catalog


_CATALOG: dict[int, tuple[str, str]] = _parse_catalog_csv()
_CATALOG_ITEMS: list[tuple[int, str, str]] = [(cid, name, cat) for cid, (name, cat) in _CATALOG.items()]
_MATCH_THRESHOLD = 0.45


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
        parsed_json = self._match_skills_to_catalog(parsed_json)

        try:
            return CvExtractionResult.model_validate(parsed_json)
        except Exception as exc:
            # Bubble up useful details for the gateway (worker error string).
            snippet = str(content).strip().replace("\n", "\\n")[:800]
            raise LlmExtractionError(
                f"LLM returned invalid schema content: {exc}. LLM snippet: {snippet}"
            ) from exc

    def _normalize_llm_payload(self, payload: dict) -> dict:
        if not isinstance(payload, dict):
            return payload

        # Ensure list fields are lists of dicts (LLM can emit broken JSON fragments).
        for key in ("education", "experience", "certifications", "achievement", "languages"):
            val = payload.get(key)
            if val is None:
                continue
            if isinstance(val, dict):
                payload[key] = [val]
            elif not isinstance(val, list):
                payload[key] = []

        if isinstance(payload.get("experience"), list):
            payload["experience"] = [e for e in payload["experience"] if isinstance(e, dict)]

        certifications = payload.get("certifications")
        if certifications is None:
            payload["certifications"] = []
        elif isinstance(certifications, dict):
            payload["certifications"] = [certifications]
        cleaned_certs: list[dict] = []
        for cert in payload["certifications"]:
            if not isinstance(cert, dict):
                continue
            if cert.get("title") is None and cert.get("name"):
                cert["title"] = cert["name"]
            if cert.get("issuer") is None and cert.get("institution"):
                cert["issuer"] = cert["institution"]
            if cert.get("expiryDate") is None and cert.get("expiration"):
                cert["expiryDate"] = cert["expiration"]
            # Drop placeholder/empty certification items.
            if any((cert.get("title"), cert.get("issuer"), cert.get("issueDate"), cert.get("expiryDate"), cert.get("description"))):
                cleaned_certs.append(cert)
        payload["certifications"] = cleaned_certs

        achievement = payload.get("achievement")
        if achievement is None and payload.get("projects") is not None:
            achievement = payload.get("projects")
        if achievement is None:
            achievement = []
        elif isinstance(achievement, dict):
            achievement = [achievement]
        payload["achievement"] = self._normalize_achievements([a for a in achievement if isinstance(a, dict)])
        payload.pop("projects", None)

        for edu in payload.get("education") or []:
            if not isinstance(edu, dict):
                continue
            if edu.get("dateGraduation") is None and edu.get("year") is not None:
                y = self._coerce_graduation_year(edu.get("year"))
                if y is not None:
                    edu["dateGraduation"] = y
        # Drop placeholder/empty education items.
        if isinstance(payload.get("education"), list):
            payload["education"] = [
                e
                for e in payload["education"]
                if isinstance(e, dict) and any((e.get("institution"), e.get("establishment"), e.get("typeEducation"), e.get("dateGraduation")))
            ]

        for exp in payload.get("experience") or []:
            if not isinstance(exp, dict):
                continue
            if exp.get("role") is None and exp.get("title"):
                exp["role"] = exp["title"]
            if exp.get("startDate") is None and exp.get("duration"):
                exp["startDate"] = str(exp["duration"]).strip() or None
        # Drop placeholder/empty experience items.
        if isinstance(payload.get("experience"), list):
            payload["experience"] = [
                e
                for e in payload["experience"]
                if isinstance(e, dict) and any((e.get("role"), e.get("company"), e.get("description"), e.get("startDate"), e.get("endDate")))
            ]

        if "languages" in payload:
            payload["languages"] = self._normalize_language_proficiencies(payload.get("languages"))

        skills = payload.get("skills")
        if isinstance(skills, dict):
            for key in ("technical", "soft"):
                skills[key] = self._normalize_string_list(skills.get(key))
            cid = skills.get("catalogId")
            if cid is not None and isinstance(cid, str) and cid.strip().isdigit():
                skills["catalogId"] = int(cid.strip())
        return payload

    @staticmethod
    def _match_skills_to_catalog(payload: dict) -> dict:
        """Approximate free-text skills to the closest catalog entry.

        Behavior:
        - Replace each skill string with the closest catalog name (if above threshold)
        - Record match details (id/name/category/ratio/source) in _meta
        - Keep only catalog-based skills (no extra inventions)
        """
        skills = payload.get("skills")
        if not isinstance(skills, dict):
            return payload

        meta = payload.get("_meta")
        if not isinstance(meta, dict):
            meta = {}
            payload["_meta"] = meta

        matches: list[dict] = []
        best_overall: tuple[int, float] | None = None

        for group_key in ("technical", "soft"):
            raw_list = skills.get(group_key) or []
            if not isinstance(raw_list, list):
                raw_list = [raw_list]

            mapped: list[str] = []
            seen: set[str] = set()

            for raw_skill in raw_list:
                source = str(raw_skill).strip()
                if not source:
                    continue
                normed = source.lower()

                best: tuple[int, str, str, float] | None = None  # (id,name,category,ratio)
                for cid, name, category in _CATALOG_ITEMS:
                    ratio = SequenceMatcher(None, normed, name.lower()).ratio()
                    if best is None or ratio > best[3]:
                        best = (cid, name, category, ratio)

                if best is None or best[3] < _MATCH_THRESHOLD:
                    continue

                cid, name, category, ratio = best
                key = name.casefold()
                if key not in seen:
                    seen.add(key)
                    mapped.append(name)

                matches.append(
                    {
                        "source": source,
                        "matchedId": cid,
                        "matchedName": name,
                        "matchedCategory": category,
                        "ratio": round(ratio, 3),
                        "group": group_key,
                    }
                )

                if best_overall is None or ratio > best_overall[1]:
                    best_overall = (cid, ratio)

            skills[group_key] = mapped

        if best_overall is not None:
            skills["catalogId"] = best_overall[0]

        if matches:
            meta["skill_catalog_matches"] = matches[:50]

        return payload

    def _coerce_graduation_year(self, value) -> int | None:
        if value is None:
            return None
        if isinstance(value, int) and 1900 <= value <= 2100:
            return value
        text = str(value).strip()
        if not text:
            return None
        m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
        if m:
            return int(m.group(1))
        return None

    def _normalize_achievements(self, items: list) -> list[dict]:
        out: list[dict] = []
        for raw in items:
            if not isinstance(raw, dict):
                continue
            item = dict(raw)
            if item.get("projectName") is None and item.get("name"):
                item["projectName"] = item["name"]
            desc = item.get("description") or ""
            tech = item.get("technologies")
            if tech and isinstance(tech, list):
                extra = ", ".join(str(t) for t in tech if t)
                if extra:
                    item["description"] = (desc + " " if desc else "") + f"Technologies: {extra}.".strip()
            url = item.get("url")
            if url and str(url).strip():
                base = item.get("description") or ""
                item["description"] = (base + " " if base else "") + f"URL: {url}".strip()
            if item.get("projectName") or item.get("description"):
                out.append(item)
        return out

    def _normalize_language_proficiencies(self, values) -> list[dict]:
        if values is None:
            return []
        if not isinstance(values, list):
            values = [values]
        out: list[dict] = []
        for item in values:
            if isinstance(item, str):
                text = item.strip()
                if text:
                    out.append({"language": text, "proficiency": None})
                continue
            if isinstance(item, dict):
                lang = item.get("language") or item.get("name") or item.get("label")
                if isinstance(lang, str) and lang.strip():
                    out.append(
                        {
                            "language": lang.strip(),
                            "proficiency": item.get("proficiency"),
                        }
                    )
        return out

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

        # LLM may have been cut off by num_predict limit → truncated JSON.
        repaired = self._try_repair_truncated_json(text)
        if repaired is not None:
            return repaired

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

    def _try_repair_truncated_json(self, text: str) -> dict | None:
        """Try to salvage a JSON object that was cut off mid-generation.

        Strategy: find the opening '{', strip the dangling tail back to the
        last complete key-value, then close every open bracket/brace.
        """
        start = text.find("{")
        if start == -1:
            return None
        fragment = text[start:]

        # Walk the string tracking open brackets/braces (outside strings).
        stack: list[str] = []
        in_string = False
        escape = False
        last_good = start  # position of last structural char at depth balance
        for i, ch in enumerate(fragment):
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
            if ch in ("{", "["):
                stack.append("}" if ch == "{" else "]")
            elif ch in ("}", "]"):
                if stack:
                    stack.pop()
                last_good = i

        if not stack:
            return None

        # Trim back to a safe cut point: last comma, colon, or complete value.
        cut = fragment[: last_good + 1] if last_good > 0 else fragment
        cut = cut.rstrip()
        cut = re.sub(r'[,:\s"]+$', "", cut)
        cut = re.sub(r",\s*$", "", cut)

        # Close everything that's still open.
        closing = "".join(reversed(stack))
        repaired = cut + closing

        parsed = self._try_json_parse(repaired)
        if isinstance(parsed, dict):
            return parsed
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
