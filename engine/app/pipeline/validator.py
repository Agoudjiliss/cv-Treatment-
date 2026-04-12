from __future__ import annotations

import re
from datetime import datetime

from app.schemas import CvExtractionResult

STOP_WORDS = {"and", "with", "using", "or", "the", "of"}
EMAIL_RE = re.compile(r"(?:[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,})")
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")


def semantic_validate(result: CvExtractionResult) -> list[str]:
    errors: list[str] = []
    now = datetime.utcnow().year

    for exp in result.experience:
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
    for lp in result.skills.languages:
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
