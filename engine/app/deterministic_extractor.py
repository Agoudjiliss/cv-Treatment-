from __future__ import annotations

import re
from dataclasses import dataclass, field

from app.parser_utils import DateExtractor, LinkExtractor, LocationCleaner, segment_cv_blocks
from app.text_cleaner import clean_cv_text
_SPACY_NLP = None



EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
PHONE_RE = re.compile(
    r"(?:\+?\d{1,4}[\s.-]?)?(?:\(?\d{2,4}\)?[\s.-]?)?\d{2,4}[\s.-]?\d{2,4}[\s.-]?\d{2,6}\b"
)
LINKEDIN_RE = re.compile(r"(https?://(?:www\.)?linkedin\.com/[^\s|)\]}>\"']+)", re.IGNORECASE)
URL_RE = re.compile(
    r"(https?://[^\s|)\]}>\"']+|www\.[A-Za-z0-9][-A-Za-z0-9./_%+?#=&]*[A-Za-z0-9/])",
    re.IGNORECASE,
)


@dataclass
class DeterministicExtractions:
    emails: list[str] = field(default_factory=list)
    phones: list[str] = field(default_factory=list)
    linkedin_urls: list[str] = field(default_factory=list)
    github_urls: list[str] = field(default_factory=list)
    portfolio_urls: list[str] = field(default_factory=list)
    other_urls: list[str] = field(default_factory=list)
    date_ranges: list[str] = field(default_factory=list)
    person_name_candidates: list[str] = field(default_factory=list)
    primary_email: str = ""
    primary_phone: str = ""
    primary_linkedin: str = ""
    primary_name: str = ""
    location_hint: str = ""


def extract_deterministic(text: str) -> DeterministicExtractions:
    cleaned = clean_cv_text(text)
    out = DeterministicExtractions()
    blocks = segment_cv_blocks(cleaned)

    emails = list(dict.fromkeys(EMAIL_RE.findall(cleaned)))
    out.emails = emails
    if emails:
        out.primary_email = emails[0]

    phones: list[str] = []
    seen_digits: set[str] = set()
    for raw in PHONE_RE.findall(cleaned):
        norm = _normalize_phone(raw)
        digits = re.sub(r"\D", "", norm)
        if len(digits) < 8:
            continue
        if digits in seen_digits:
            continue
        seen_digits.add(digits)
        phones.append(norm)
    out.phones = phones
    if phones:
        out.primary_phone = phones[0]

    link_extractor = LinkExtractor()
    linkedin, github, portfolio = link_extractor.extract(cleaned)
    out.linkedin_urls = linkedin
    out.github_urls = github
    out.portfolio_urls = portfolio
    if out.linkedin_urls:
        out.primary_linkedin = out.linkedin_urls[0]

    urls = list(dict.fromkeys(URL_RE.findall(cleaned)))
    for u in urls:
        if "linkedin.com" in u.lower():
            continue
        if u not in out.other_urls:
            out.other_urls.append(u)

    loc_cleaner = LocationCleaner()
    out.location_hint = loc_cleaner.clean(_extract_address_line(cleaned) or blocks.get("CONTACT", ""))
    out.person_name_candidates = _spacy_person_names(cleaned)
    out.primary_name = _guess_name_line(cleaned) or (
        out.person_name_candidates[0] if out.person_name_candidates else ""
    )

    date_extractor = DateExtractor()
    out.date_ranges = [span.raw for span in date_extractor.extract_ranges(blocks.get("EXPERIENCE", cleaned))]

    return out


def format_anchor_block(d: DeterministicExtractions) -> str:
    lines = [
        "DETERMINISTIC_CONTACT_ANCHORS (copy EXACTLY into JSON contact fields; do not invent or alter):",
        f'  "email": "{d.primary_email}"',
        f'  "phone": "{d.primary_phone}"',
        f'  "linkedin": "{d.primary_linkedin}"',
        f'  "name": "{d.primary_name}"',
        f'  "location": "{d.location_hint}"',
        "All emails found: " + ", ".join(d.emails) if d.emails else "All emails found: (none)",
        "All phones found: " + ", ".join(d.phones) if d.phones else "All phones found: (none)",
        "All date ranges found: " + ", ".join(d.date_ranges[:20]) if d.date_ranges else "All date ranges found: (none)",
        "GitHub URLs: " + ", ".join(d.github_urls[:5]) if d.github_urls else "GitHub URLs: (none)",
        "Other URLs: " + ", ".join(d.other_urls[:12]) if d.other_urls else "Other URLs: (none)",
    ]
    return "\n".join(lines)


def _normalize_phone(raw: str) -> str:
    s = raw.strip()
    s = re.sub(r"^\+?\d{1,3}\)\s*", lambda m: m.group(0).replace(")", ""), s)
    s = re.sub(r"\s+", " ", s)
    return s


def _extract_address_line(text: str) -> str:
    # Prefer explicit "Address: ..." fragments even on mixed contact lines.
    match = re.search(r"\baddress\s*:\s*([^|\n]+)", text, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()
    for line in text.splitlines():
        lower = line.lower()
        if "address" in lower or "location" in lower:
            addr_match = re.search(r"\baddress\s*:\s*([^|\n]+)", line, flags=re.IGNORECASE)
            if addr_match:
                return addr_match.group(1).strip()
            loc_match = re.search(r"\blocation\s*:\s*([^|\n]+)", line, flags=re.IGNORECASE)
            if loc_match:
                return loc_match.group(1).strip()
            parts = line.split(":", 1)
            return parts[1].strip() if len(parts) == 2 else line.strip()
    return ""


def _guess_name_line(text: str) -> str:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    blacklist = (
        "phone",
        "email",
        "address",
        "website",
        "experience",
        "education",
        "skills",
        "work",
        "summary",
    )
    for line in lines[:10]:
        lower = line.lower()
        if any(b in lower for b in blacklist):
            continue
        if ":" in line and "|" not in line:
            continue
        words = line.replace("|", " ").split()
        if 2 <= len(words) <= 6 and all(any(c.isalpha() for c in w) for w in words):
            return line.split("|")[0].strip()
    return ""


TECH_KEYWORDS = [
    "python",
    "java",
    "spring",
    "fastapi",
    "docker",
    "kubernetes",
    "langchain",
    "pytorch",
    "pandas",
    "numpy",
    "sql",
    "postgresql",
    "mongodb",
    "javascript",
    "typescript",
    "react",
    "node",
    "c#",
    ".net",
]

SOFT_KEYWORDS = [
    "leadership",
    "communication",
    "teamwork",
    "problem solving",
    "autonomy",
    "adaptability",
]

LANG_KEYWORDS = ["english", "french", "arabic", "spanish", "german", "italian"]


def keyword_skills(lower_text: str) -> tuple[list[str], list[str], list[str]]:
    def present(keys: list[str]) -> list[str]:
        return [k for k in keys if k in lower_text]

    return present(TECH_KEYWORDS), present(SOFT_KEYWORDS), present(LANG_KEYWORDS)


def _spacy_person_names(text: str) -> list[str]:
    global _SPACY_NLP
    try:
        import spacy
    except ImportError:
        return []
    if _SPACY_NLP is None:
        try:
            _SPACY_NLP = spacy.load("en_core_web_sm")
        except OSError:
            return []
    doc = _SPACY_NLP(text[:8000])
    names: list[str] = []
    for ent in doc.ents:
        if ent.label_ == "PERSON" and 2 <= len(ent.text.strip()) <= 80:
            names.append(ent.text.strip())
    return list(dict.fromkeys(names))
