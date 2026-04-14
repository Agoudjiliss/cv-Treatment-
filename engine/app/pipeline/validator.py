from __future__ import annotations

import re
from datetime import datetime
from datetime import date

from app.schemas import CvExtractionResult

STOP_WORDS = {"and", "with", "using", "or", "the", "of"}
EMAIL_RE = re.compile(r"(?:[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,})")
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DMY_DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    s = s.strip()
    try:
        if ISO_DATE_RE.fullmatch(s):
            return datetime.strptime(s, "%Y-%m-%d").date()
        if DMY_DATE_RE.fullmatch(s):
            return datetime.strptime(s, "%d/%m/%Y").date()
    except Exception:
        return None
    return None


def semantic_validate(result: CvExtractionResult) -> list[str]:
    errors: list[str] = []
    now = datetime.utcnow().year

    for exp in result.experience:
        # Basic sanity: start <= end when both are real dates.
        sd = _parse_date(exp.startDate)
        ed = _parse_date(exp.endDate)
        if sd and ed and ed < sd:
            errors.append("experience_end_before_start")

        date_blob = " ".join(
            s for s in (exp.startDate, exp.endDate) if s
        )
        years = [int(y) for y in YEAR_RE.findall(date_blob)]
        if years and max(years) > now + 1:
            errors.append("experience_duration_year_future")

    for group in [result.skills.technical, result.skills.soft]:
        for item in group:
            if item.strip().lower() in STOP_WORDS:
                errors.append("skills_contains_stopword")
                break
    for lp in result.languages:
        lang = (lp.language or "").strip().lower()
        if lang in STOP_WORDS:
            errors.append("skills_contains_stopword")
            break

    if result.contact.email and not EMAIL_RE.fullmatch(result.contact.email.strip()):
        errors.append("invalid_email")

    for edu in result.education:
        grad = edu.dateGraduation
        years: list[int] = []
        if isinstance(grad, int):
            years = [grad]
        elif isinstance(grad, str):
            years = [int(y) for y in YEAR_RE.findall(grad)]
        for y in years:
            if y < 1940 or y > now + 5:
                errors.append("education_year_out_of_range")
                break
    return errors
