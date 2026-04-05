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
        years = [int(y) for y in YEAR_RE.findall(exp.duration or "")]
        if years and max(years) > now + 1:
            errors.append("experience_duration_year_future")

    for group in [result.skills.technical, result.skills.soft, result.skills.languages]:
        for item in group:
            if item.strip().lower() in STOP_WORDS:
                errors.append("skills_contains_stopword")
                break

    if result.contact.email and not EMAIL_RE.fullmatch(result.contact.email.strip()):
        errors.append("invalid_email")

    for edu in result.education:
        years = [int(y) for y in YEAR_RE.findall(edu.year or "")]
        for y in years:
            if y < 1940 or y > now + 5:
                errors.append("education_year_out_of_range")
                break
    return errors
