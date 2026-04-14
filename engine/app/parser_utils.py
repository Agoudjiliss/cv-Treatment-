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


_FR_MONTHS: dict[str, int] = {
    "janvier": 1, "jan": 1,
    "février": 2, "fevrier": 2, "fév": 2, "fev": 2,
    "mars": 3, "mar": 3,
    "avril": 4, "avr": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7, "juil": 7,
    "août": 8, "aout": 8,
    "septembre": 9, "sep": 9, "sept": 9,
    "octobre": 10, "oct": 10,
    "novembre": 11, "nov": 11,
    "décembre": 12, "decembre": 12, "déc": 12, "dec": 12,
    # English
    "january": 1, "february": 2, "march": 3, "april": 4,
    "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

_ONGOING_DATE_TOKENS = re.compile(
    r"^(?:present|current|now|today|actuel(?:le)?|en\s+cours|"
    r"aujourd.hui|à\s+ce\s+jour|a\s+ce\s+jour|ongoing)$",
    re.IGNORECASE,
)

_FR_MONTH_YEAR_RE = re.compile(
    r"(?P<month>[a-zéèêëàâôùûç]+\.?)\s+(?P<year>\d{4})",
    re.IGNORECASE,
)

_DATE_TOKEN = (
    r"(?:\d{2}/\d{2}/\d{4}|\d{2}/\d{4}|\d{4}|"
    r"(?:[a-zéèêëàâôùûç]+\.?\s+\d{4}))"
)
_ONGOING_TOKEN = r"(?:present|current|now|today|actuel(?:le)?|en\s+cours|aujourd.hui|à\s+ce\s+jour|a\s+ce\s+jour|ongoing)"


class DateExtractor:
    RANGE_RE = re.compile(
        rf"(?P<start>{_DATE_TOKEN})\s*[-–]\s*(?P<end>{_DATE_TOKEN}|{_ONGOING_TOKEN})",
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
        t = token.strip()
        tl = t.lower()
        # Ongoing / active
        if _ONGOING_DATE_TOKENS.fullmatch(tl):
            return date.today()
        # DD/MM/YYYY
        if re.fullmatch(r"\d{2}/\d{2}/\d{4}", t):
            dd, mm, yyyy = t.split("/")
            try:
                return date(int(yyyy), int(mm), int(dd))
            except ValueError:
                return None
        # MM/YYYY
        if re.fullmatch(r"\d{2}/\d{4}", t):
            mm, yyyy = t.split("/")
            try:
                day = 1 if floor else 28
                return date(int(yyyy), int(mm), day)
            except ValueError:
                return None
        # YYYY
        if re.fullmatch(r"\d{4}", t):
            month = 1 if floor else 12
            day = 1 if floor else 28
            try:
                return date(int(t), month, day)
            except ValueError:
                return None
        # "janvier 2022", "march 2020", "sept. 2019" etc.
        m = _FR_MONTH_YEAR_RE.fullmatch(tl.strip())
        if m:
            month_str = m.group("month").rstrip(".")
            month_num = _FR_MONTHS.get(month_str)
            if month_num:
                try:
                    day = 1 if floor else 28
                    return date(int(m.group("year")), month_num, day)
                except ValueError:
                    return None
        return None


class LocationCleaner:
    NOISE_RE = re.compile(
        r"\b(?:"
        # English
        r"phone|mobile|email|e-?mail|website|web|github|linkedin|address|"
        # French
        r"t[eé]l[eé]phone|t[eé]l[eé]|portable|courriel|adresse|site(?:\s+web)?|"
        r"github|linkedin|permis|nationalit[eé]|date\s+de\s+naissance|"
        r"n[eé](?:\s*le)?|sexe|genre"
        r")\b.*",
        re.IGNORECASE,
    )

    _ADDR_LABEL_RE = re.compile(
        r"\b(?:address|adresse|localisation|location|domicile|r[eé]sidence|ville)\s*:\s*([^|\n]+)",
        re.IGNORECASE,
    )

    def clean(self, raw: str) -> str:
        if not raw:
            return ""
        addr_match = self._ADDR_LABEL_RE.search(raw)
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


_SECTION_MAP: list[tuple[re.Pattern[str], str]] = [
    # EXPERIENCE — EN + FR
    (re.compile(
        r"\b(?:work\s+experience|professional\s+experience|employment|experience|"
        r"exp[eé]riences?\s*(?:professionnelles?)?|parcours\s+professionnel|"
        r"historique\s+professionnel)\b",
        re.IGNORECASE,
    ), "EXPERIENCE"),
    # EDUCATION — EN + FR
    (re.compile(
        r"\b(?:education(?:\s+&\s+training)?|training|academic\s+background|"
        r"[eé]ducation|formation(?:s)?|[eé]tudes?|dipl[oô]mes?|"
        r"cursus|parcours\s+acad[eé]mique|sc[oa]larité)\b",
        re.IGNORECASE,
    ), "EDUCATION"),
    # SKILLS — EN + FR
    (re.compile(
        r"\b(?:skills?|technical\s+skills?|core\s+competencies|"
        r"comp[eé]tences?(?:\s+(?:techniques?|professionnelles?|cl[eé]s?))?|"
        r"savoir[-\s]faire|aptitudes?|expertise)\b",
        re.IGNORECASE,
    ), "SKILLS"),
    # LANGUAGES — EN + FR
    (re.compile(
        r"\b(?:languages?|language\s+skills?|"
        r"langues?(?:\s+[eé]trang[eè]res?)?|comp[eé]tences?\s+linguistiques?)\b",
        re.IGNORECASE,
    ), "LANGUAGES"),
    # PROJECTS / ACHIEVEMENTS — EN + FR
    (re.compile(
        r"\b(?:projects?|achievements?|accomplishments?|portfolio|"
        r"projets?|r[eé]alisations?|contributions?)\b",
        re.IGNORECASE,
    ), "PROJECTS"),
    # CERTIFICATIONS — EN + FR
    (re.compile(
        r"\b(?:certifications?|certificates?|licen[sc]es?|accreditations?|"
        r"certifications?\s+et\s+formations?)\b",
        re.IGNORECASE,
    ), "CERTIFICATIONS"),
    # SUMMARY / PROFILE — EN + FR
    (re.compile(
        r"\b(?:summary|profile|objective|about\s+me|overview|"
        r"profil(?:\s+professionnel)?|r[eé]sum[eé]|pr[eé]sentation|"
        r"objectif(?:\s+professionnel)?|synth[eè]se)\b",
        re.IGNORECASE,
    ), "SUMMARY"),
]


def segment_cv_blocks(text: str) -> dict[str, str]:
    lines = [ln.strip() for ln in text.splitlines()]
    block_keys = ["CONTACT", "EXPERIENCE", "EDUCATION", "SKILLS", "LANGUAGES",
                  "PROJECTS", "CERTIFICATIONS", "SUMMARY", "OTHER"]
    blocks: dict[str, list[str]] = {k: [] for k in block_keys}
    current = "CONTACT"
    for line in lines:
        matched = False
        for pattern, section in _SECTION_MAP:
            if pattern.search(line) and len(line) < 80:
                current = section
                matched = True
                break
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
