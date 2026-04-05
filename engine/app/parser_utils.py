from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date


@dataclass
class DateSpan:
    raw: str
    start: str
    end: str
    months: int | None

    @property
    def duration_label(self) -> str:
        if self.months is None:
            return self.raw
        years, months = divmod(self.months, 12)
        if years > 0 and months > 0:
            return f"{years}y {months}m"
        if years > 0:
            return f"{years}y"
        return f"{months}m"


class DateExtractor:
    RANGE_RE = re.compile(
        r"(?P<start>(?:\d{2}/\d{2}/\d{4})|(?:\d{2}/\d{4})|(?:\d{4}))\s*[-–]\s*(?P<end>(?:present|current|now|today|\d{2}/\d{2}/\d{4}|\d{2}/\d{4}|\d{4}))",
        re.IGNORECASE,
    )

    def extract_ranges(self, text: str) -> list[DateSpan]:
        spans: list[DateSpan] = []
        for m in self.RANGE_RE.finditer(text):
            start_raw = m.group("start")
            end_raw = m.group("end")
            months = self._months_between(start_raw, end_raw)
            spans.append(
                DateSpan(
                    raw=f"{start_raw} - {end_raw}",
                    start=start_raw,
                    end=end_raw,
                    months=months,
                )
            )
        return spans

    def _months_between(self, start_raw: str, end_raw: str) -> int | None:
        start = self._to_date(start_raw, floor=True)
        end = self._to_date(end_raw, floor=False)
        if start is None or end is None:
            return None
        diff = (end.year - start.year) * 12 + (end.month - start.month)
        return max(diff, 0)

    def _to_date(self, token: str, floor: bool) -> date | None:
        t = token.strip().lower()
        if t in {"present", "current", "now", "today"}:
            return date.today()
        if re.fullmatch(r"\d{2}/\d{2}/\d{4}", t):
            dd, mm, yyyy = t.split("/")
            return date(int(yyyy), int(mm), int(dd))
        if re.fullmatch(r"\d{2}/\d{4}", t):
            mm, yyyy = t.split("/")
            day = 1 if floor else 28
            return date(int(yyyy), int(mm), day)
        if re.fullmatch(r"\d{4}", t):
            month = 1 if floor else 12
            day = 1 if floor else 28
            return date(int(t), month, day)
        return None


class LocationCleaner:
    NOISE_RE = re.compile(
        r"\b(phone|mobile|email|e-?mail|website|web|github|linkedin|address)\b.*",
        re.IGNORECASE,
    )

    def clean(self, raw: str) -> str:
        if not raw:
            return ""
        addr_match = re.search(r"\baddress\s*:\s*([^|\n]+)", raw, flags=re.IGNORECASE)
        if addr_match:
            candidate = addr_match.group(1).strip()
        else:
            candidate = raw.split("|")[0].strip()
        candidate = self.NOISE_RE.sub("", candidate).strip(" :-|")
        candidate = re.sub(r"^\(?\+?\d[\d\s()\-]{6,}$", "", candidate).strip()
        candidate = re.sub(r"\s{2,}", " ", candidate)
        return candidate


class LinkExtractor:
    URL_RE = re.compile(
        r"(https?://[^\s|)\]}>\"']+|www\.[A-Za-z0-9][-A-Za-z0-9./_%+?#=&]*[A-Za-z0-9/])",
        re.IGNORECASE,
    )

    def extract(self, text: str) -> tuple[list[str], list[str], list[str]]:
        linkedin: list[str] = []
        github: list[str] = []
        portfolio: list[str] = []
        seen: set[str] = set()
        for raw in self.URL_RE.findall(text):
            url = self._normalize(raw)
            key = url.lower()
            if key in seen:
                continue
            seen.add(key)
            if "linkedin.com" in key:
                linkedin.append(url)
            elif "github.com" in key:
                github.append(url)
            else:
                portfolio.append(url)
        return linkedin, github, portfolio

    def _normalize(self, url: str) -> str:
        u = url.strip()
        if u.lower().startswith("www."):
            u = f"https://{u}"
        return u


def segment_cv_blocks(text: str) -> dict[str, str]:
    lines = [ln.strip() for ln in text.splitlines()]
    blocks = {"CONTACT": [], "EXPERIENCE": [], "EDUCATION": [], "SKILLS": [], "OTHER": []}
    current = "CONTACT"
    for line in lines:
        upper = line.upper()
        if "EXPERIENCE" in upper or "WORK EXPERIENCE" in upper:
            current = "EXPERIENCE"
        elif "EDUCATION" in upper or "FORMATION" in upper:
            current = "EDUCATION"
        elif "SKILLS" in upper or "COMPETENC" in upper:
            current = "SKILLS"
        blocks[current].append(line)
    return {k: "\n".join(v).strip() for k, v in blocks.items()}


def normalize_skill_token(token: str) -> str:
    mapping = {
        "springboot": "Spring Boot",
        "spring boot": "Spring Boot",
        "js": "JavaScript",
        "nodejs": "Node.js",
        "node js": "Node.js",
        "csharp": "C#",
        "c# .net": "C#/.NET",
        "oracle sql": "Oracle SQL",
        "micro-services": "Microservices",
    }
    raw = token.strip()
    key = re.sub(r"\s+", " ", raw.lower())
    return mapping.get(key, raw)


def dedupe_skills(skills: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for s in skills:
        norm = normalize_skill_token(s)
        key = norm.lower()
        if not norm or key in seen:
            continue
        seen.add(key)
        out.append(norm)
    return out
