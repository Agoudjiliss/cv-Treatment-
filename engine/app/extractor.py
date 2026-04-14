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

# Minimum SequenceMatcher ratio to accept a catalog match.
# Set high to avoid spurious matches (e.g. "java spring boot" → "smart pricing").
_MATCH_THRESHOLD = 0.70

# Explicit keyword → catalogId table for technical terms that are too short or too
# different in character composition to be matched by SequenceMatcher alone.
# Keys are lowercase substrings; first match wins (order matters for specificity).
_KEYWORD_CATALOG_MAP: list[tuple[str, int]] = [
    # Development / engineering
    ("machine learning", 120), ("artificial intelligence", 120), ("deep learning", 120),
    ("data science", 58), ("data model", 58), ("data pipeline", 58),
    ("spring boot", 8), ("spring", 8), ("java", 8),
    ("python", 8), ("django", 8), ("flask", 8), ("fastapi", 8),
    ("javascript", 8), ("typescript", 8), ("node.js", 8), ("nodejs", 8),
    ("react", 8), ("angular", 8), ("vue", 8),
    ("c#", 8), (".net", 8), ("dotnet", 8),
    ("php", 8), ("ruby", 8), ("golang", 8), ("rust", 8), ("swift", 8),
    ("flutter", 8), ("kotlin", 8),
    ("full-stack", 8), ("fullstack", 8), ("full stack", 8), ("backend", 8), ("frontend", 8),
    ("rest api", 8), ("graphql", 8), ("grpc", 8),
    ("microservices", 186), ("micro-services", 186), ("micro services", 186),
    ("integration", 186), ("service integration", 186),
    ("systems integration", 197), ("system integration", 197),
    ("cloud", 30), ("aws", 30), ("azure", 30), ("gcp", 30),
    ("docker", 30), ("kubernetes", 30), ("k8s", 30), ("devops", 30), ("ci/cd", 29),
    ("internet of things", 108), ("iot", 108),
    ("oracle", 111), ("sql", 111), ("postgresql", 111), ("mysql", 111),
    ("mongodb", 111), ("redis", 111), ("elasticsearch", 111), ("database", 111),
    ("cyber security", 55), ("cybersecurity", 55), ("security", 180),
    ("blockchain", 108),
    # Soft skills
    ("problem solving", 152), ("problem-solving", 152),
    ("analytical thinking", 7), ("analytical", 7),
    ("critical thinking", 47), ("critical", 47),
    ("creative thinking", 45), ("creativity", 45),
    ("negotiation", 132),
    ("communication", 49), ("teamwork", 152), ("collaboration", 152),
    ("autonomy", 7), ("adaptability", 7), ("leadership", 149),
    ("project management", 156), ("agile", 6), ("scrum", 6),
    ("troubleshoot", 217), ("technical support", 217),
    # Generic IT
    ("computer", 32), ("office technology", 32),
    ("digital technology", 70), ("digital", 63),
    ("website", 219), ("web", 8),
    ("network", 134), ("networking", 134),
    ("enterprise architecture", 76),
    ("big data", 16),
    ("user acceptance testing", 218), ("uat", 218), ("testing", 218),
]


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

    def structure_cv(self, raw_text: str, anchors: str = "") -> CvExtractionResult:
        truncated = truncate_text(raw_text, max_chars=6000)
        content = self._client.call_structured_cv(truncated, anchors=anchors)
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
            # Coerce dateGraduation from any source (string, int, or sibling keys).
            dg = edu.get("dateGraduation")
            coerced = self._coerce_graduation_year(dg)
            if coerced is None and edu.get("year") is not None:
                coerced = self._coerce_graduation_year(edu.get("year"))
            if coerced is None:
                for field in ("establishment", "institution"):
                    coerced = self._coerce_graduation_year(edu.get(field))
                    if coerced:
                        break
            if coerced is not None:
                edu["dateGraduation"] = coerced
        # Drop placeholder/empty education items.
        # Require at least institution OR establishment to be non-empty —
        # typeEducation alone (e.g. "MASTER") is not enough to keep an entry.
        if isinstance(payload.get("education"), list):
            payload["education"] = [
                e
                for e in payload["education"]
                if isinstance(e, dict) and (e.get("institution") or e.get("establishment"))
            ]

        for exp in payload.get("experience") or []:
            if not isinstance(exp, dict):
                continue
            # Fill role from common aliases.
            if not exp.get("role"):
                for alias in ("title", "position", "jobTitle", "job_title", "poste"):
                    if exp.get(alias):
                        exp["role"] = str(exp[alias]).strip()
                        break
            # Last resort: first non-empty line of description.
            if not exp.get("role"):
                desc = (exp.get("description") or "").strip()
                if desc:
                    first_line = desc.split("\n")[0].split(".")[0].strip()
                    if first_line and len(first_line) < 80:
                        exp["role"] = first_line
            if exp.get("startDate") is None and exp.get("duration"):
                exp["startDate"] = str(exp["duration"]).strip() or None
            # Normalize dates to DD/MM/YYYY.
            for date_key in ("startDate", "endDate"):
                exp[date_key] = self._normalize_date(exp.get(date_key))

        # Drop placeholder/empty experience items.
        if isinstance(payload.get("experience"), list):
            payload["experience"] = [
                e
                for e in payload["experience"]
                if isinstance(e, dict) and any((e.get("role"), e.get("company"), e.get("description"), e.get("startDate"), e.get("endDate")))
            ]

        if "languages" in payload:
            payload["languages"] = self._normalize_language_proficiencies(payload.get("languages"))

        # Strip common "professional summary:" prefixes the LLM adds.
        summary = payload.get("summary")
        if isinstance(summary, str):
            payload["summary"] = re.sub(
                r"^(?:professional\s+summary|résumé|resume|cv|summary)\s*:\s*\n*",
                "",
                summary.strip(),
                flags=re.IGNORECASE,
            ).strip() or None

        skills = payload.get("skills")
        if isinstance(skills, dict):
            for key in ("technical", "soft"):
                skills[key] = self._normalize_string_list(skills.get(key))
            cid = skills.get("catalogId")
            if cid is not None and isinstance(cid, str) and cid.strip().isdigit():
                skills["catalogId"] = int(cid.strip())
        return payload

    _ONGOING_TERMS = re.compile(
        r"^(?:current|present|today|now|ongoing|en\s+cours|actuel(?:le)?|jusqu'à\s+(?:ce\s+jour|aujourd'hui))$",
        re.IGNORECASE,
    )

    @classmethod
    def _normalize_date(cls, value: str | None) -> str | None:
        """Normalise any recognisable date to DD/MM/YYYY; leave others unchanged.
        "CURRENT" / "present" / "ongoing" → None (still active).
        """
        if not value or not isinstance(value, str):
            return value
        v = value.strip()
        if not v:
            return None
        # Ongoing / still active → null
        if cls._ONGOING_TERMS.fullmatch(v):
            return None
        # Already DD/MM/YYYY
        if re.fullmatch(r"\d{2}/\d{2}/\d{4}", v):
            return v
        # ISO YYYY-MM-DD
        m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", v)
        if m:
            return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
        # MM/YYYY or MM-YYYY → 01/MM/YYYY
        m = re.fullmatch(r"(\d{2})[/-](\d{4})", v)
        if m:
            return f"01/{m.group(1)}/{m.group(2)}"
        # YYYY only → 01/01/YYYY
        m = re.fullmatch(r"(\d{4})", v)
        if m:
            return f"01/01/{m.group(1)}"
        return v

    @staticmethod
    def _match_skills_to_catalog(payload: dict) -> dict:
        """Approximate free-text skills to the closest catalog entry.

        Behavior:
        - skills.technical and skills.soft keep the ORIGINAL free-text strings.
        - catalogId is set to the best catalog match across all skills.
        - Match uses a keyword lookup table first (for tech terms SequenceMatcher
          can't handle), then falls back to SequenceMatcher with a high threshold.
        - Match details are stored in _meta.skill_catalog_matches for audit.
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

            for raw_skill in raw_list:
                source = str(raw_skill).strip()
                if not source:
                    continue
                normed = source.lower()

                # 1) Keyword lookup (explicit mapping for tech terms).
                kw_cid: int | None = None
                for keyword, kid in _KEYWORD_CATALOG_MAP:
                    if keyword in normed:
                        kw_cid = kid
                        break

                if kw_cid is not None:
                    name, category = _CATALOG.get(kw_cid, ("", ""))
                    matches.append({
                        "source": source,
                        "matchedId": kw_cid,
                        "matchedName": name,
                        "matchedCategory": category,
                        "ratio": 1.0,
                        "method": "keyword",
                        "group": group_key,
                    })
                    if best_overall is None or 1.0 > best_overall[1]:
                        best_overall = (kw_cid, 1.0)
                    continue

                # 2) SequenceMatcher fallback with high threshold.
                sm_best: tuple[int, str, str, float] | None = None
                for cid, name, category in _CATALOG_ITEMS:
                    ratio = SequenceMatcher(None, normed, name.lower()).ratio()
                    if sm_best is None or ratio > sm_best[3]:
                        sm_best = (cid, name, category, ratio)

                if sm_best is not None and sm_best[3] >= _MATCH_THRESHOLD:
                    cid, name, category, ratio = sm_best
                    matches.append({
                        "source": source,
                        "matchedId": cid,
                        "matchedName": name,
                        "matchedCategory": category,
                        "ratio": round(ratio, 3),
                        "method": "fuzzy",
                        "group": group_key,
                    })
                    if best_overall is None or ratio > best_overall[1]:
                        best_overall = (cid, ratio)

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
