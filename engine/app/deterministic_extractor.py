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
    # Location extraction: avoid feeding the whole CV when CONTACT segmentation fails.
    # Prefer explicit address label, otherwise restrict to early header lines.
    header_slice = "\n".join([ln for ln in cleaned.splitlines() if ln.strip()][:10])
    out.location_hint = loc_cleaner.clean(_extract_address_line(cleaned) or blocks.get("CONTACT", "") or header_slice)
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
    # Remove surrounding parentheses from country code: "(+213) 7..." → "+213 7..."
    s = re.sub(r"^\((\+?\d{1,4})\)\s*", r"\1 ", s)
    # Remove stray trailing parenthesis in country code: "+213) 7..." → "+213 7..."
    s = re.sub(r"^(\+?\d{1,4})\)\s*", r"\1 ", s)
    # Collapse multiple spaces
    s = re.sub(r"\s+", " ", s)
    return s


_ADDRESS_LABEL_RE = re.compile(
    r"\b(?:address|adresse|localisation|location|domicile|r[eé]sidence|ville)\s*:\s*([^|\n]+)",
    re.IGNORECASE,
)
_ADDRESS_KEYWORD_RE = re.compile(
    r"\b(?:address|adresse|localisation|location|domicile|r[eé]sidence)\b",
    re.IGNORECASE,
)


def _extract_address_line(text: str) -> str:
    # Try "Address/Adresse/Localisation: ..." anywhere in the text first.
    match = _ADDRESS_LABEL_RE.search(text)
    if match:
        return match.group(1).strip()
    for line in text.splitlines():
        if _ADDRESS_KEYWORD_RE.search(line):
            m = _ADDRESS_LABEL_RE.search(line)
            if m:
                return m.group(1).strip()
            parts = line.split(":", 1)
            return parts[1].strip() if len(parts) == 2 else line.strip()
    return ""


def _guess_name_line(text: str) -> str:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    # Use word-boundary checks to avoid short tokens matching unrelated words.
    _BLACKLIST_EXACT = {
        "phone", "mobile", "email", "address", "website", "web",
        "experience", "education", "skills", "work", "summary", "linkedin",
        "github", "portfolio", "objective", "contact",
        "téléphone", "telephone", "portable", "courriel", "adresse", "site",
        "profil", "expérience", "formation", "compétence", "competence",
        "langues", "projets", "présentation", "presentation", "objectif",
        "nationalité", "nationalite", "permis", "naissance", "sexe", "genre",
    }
    # Short tokens that need word-boundary protection
    _BLACKLIST_PATTERN = re.compile(
        r"\b(?:né(?:\s+le)?|nee|cv|tel|tél|fax)\b",
        re.IGNORECASE,
    )
    _TITLE_HINTS = re.compile(
        r"\b(?:backend|développement|developpement|developer|développeur|"
        r"engineer|ingénieur|intern|stagiaire|freelance|transformation)\b",
        re.IGNORECASE,
    )

    def looks_like_person_name(line: str) -> bool:
        # Heuristic: 2-4 words, mostly alphabetic, and at least two capitalized tokens.
        toks = [t for t in re.split(r"\s+", line.replace("|", " ").strip()) if t]
        if not (2 <= len(toks) <= 4):
            return False
        caps = sum(1 for t in toks if t[:1].isupper())
        alpha = all(any(c.isalpha() for c in t) for t in toks)
        return alpha and caps >= 2

    for line in lines[:12]:
        lower = line.lower()
        if any(b in lower for b in _BLACKLIST_EXACT):
            continue
        if _BLACKLIST_PATTERN.search(lower):
            continue
        # Skip job titles/objectives (common OCR mistake: using headline as name).
        # Even if it "looks like" a name (2 capitalized tokens), words like "Backend"
        # are overwhelmingly titles, not names.
        if _TITLE_HINTS.search(lower):
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

LANG_KEYWORDS = [
    "english", "french", "arabic", "spanish", "german", "italian",
    "portuguese", "chinese", "mandarin", "dutch", "russian", "turkish",
    "japanese", "korean", "hindi", "persian", "farsi", "urdu",
    "swedish", "norwegian", "danish", "polish", "czech", "romanian",
]

# French / local names for each language → canonical English key for _LANG_NORMALIZE lookup.
# Each tuple: (pattern_string, canonical_english_name_matching_LANG_KEYWORDS)
_LANG_LOCAL_ALIASES: list[tuple[str, str]] = [
    # French names of languages
    (r"anglais", "english"),
    (r"fran[çc]ais", "french"),
    (r"arabe", "arabic"),
    (r"espagnol", "spanish"),
    (r"allemand", "german"),
    (r"italien", "italian"),
    (r"portugais", "portuguese"),
    (r"chinois", "chinese"),
    (r"n[eé]erlandais", "dutch"),
    (r"russe", "russian"),
    (r"turc", "turkish"),
    (r"japonais", "japanese"),
    (r"cor[eé]en", "korean"),
    (r"polonais", "polish"),
    (r"roumain", "romanian"),
    (r"su[eé]dois", "swedish"),
    (r"norv[eé]gien", "norwegian"),
    (r"danois", "danish"),
    (r"tch[eè]que", "czech"),
    (r"hindi", "hindi"),
    # Arabic names / transliterations
    (r"[Ee]nglish", "english"),
    (r"عربية|عربي", "arabic"),
    (r"إنجليزي|انجليزي", "english"),
    (r"فرنسي|فرنسية", "french"),
]

_LANG_LOCAL_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b" + pat + r"\b", re.IGNORECASE | re.UNICODE), canonical)
    for pat, canonical in _LANG_LOCAL_ALIASES
]

# Compiled word-boundary patterns for each language keyword (avoids "french fries" → FRENCH).
_LANG_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (lang, re.compile(r"\b" + re.escape(lang) + r"\b", re.IGNORECASE))
    for lang in LANG_KEYWORDS
]


def keyword_skills(lower_text: str) -> tuple[list[str], list[str], list[str]]:
    def present(keys: list[str]) -> list[str]:
        return [k for k in keys if k in lower_text]

    seen: set[str] = set()
    langs: list[str] = []

    # English language names (word-boundary safe)
    for lang, pat in _LANG_PATTERNS:
        if pat.search(lower_text) and lang not in seen:
            seen.add(lang)
            langs.append(lang)

    # French / local language names → resolved to English canonical name
    for pat, canonical in _LANG_LOCAL_PATTERNS:
        if pat.search(lower_text) and canonical not in seen:
            seen.add(canonical)
            langs.append(canonical)

    return present(TECH_KEYWORDS), present(SOFT_KEYWORDS), langs


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
