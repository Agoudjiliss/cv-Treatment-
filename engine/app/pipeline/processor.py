from __future__ import annotations

import asyncio
import re
import time

from app.deterministic_extractor import DeterministicExtractions, extract_deterministic, format_anchor_block, keyword_skills
from app.extractor import LlmExtractor
from app.ocr import OcrEngine
from app.pipeline.validator import semantic_validate
from app.schemas import CvExtractionResult, LanguageProficiency


def compute_confidence(cv: CvExtractionResult) -> float:
    skill_signal = bool(
        cv.skills.technical
        or cv.skills.soft
        or cv.languages
        or (cv.skills.score and cv.skills.score.strip())
        or cv.skills.catalog_id is not None
    )
    filled = sum(
        [
            bool(cv.contact.name),
            bool(cv.contact.email),
            bool(cv.contact.phone),
            bool(cv.contact.location),
            bool(cv.education),
            bool(cv.experience),
            skill_signal,
            bool(cv.summary),
            bool(cv.achievement or cv.certifications),
        ]
    )
    return round(filled / 9, 2)


def _fill_summary(cv: CvExtractionResult) -> None:
    """Build a minimal summary when the LLM didn't produce one."""
    if cv.summary and cv.summary.strip():
        return
    parts: list[str] = []
    name = (cv.contact.name or "").strip()
    if name:
        parts.append(name)
    roles = list(dict.fromkeys(
        e.role.strip() for e in cv.experience if e.role and e.role.strip()
    ))
    if roles:
        parts.append(f"experienced as {', '.join(roles[:2])}")
    tech = (cv.skills.technical or [])[:4]
    if tech:
        parts.append(f"skilled in {', '.join(tech)}")
    if parts:
        sentence = ". ".join(parts)
        cv.summary = sentence[0].upper() + sentence[1:] + "."


def _is_probable_email(text: str) -> bool:
    t = (text or "").strip()
    return "@" in t and "." in t and " " not in t


_LANG_NATIVE_HINT_RE = re.compile(
    r"\b(?:mother\s+tongue|langue\s+maternelle|native|bilingue|bilingual)\b",
    re.IGNORECASE,
)
_LANG_NORMALIZE = {
    "english": "ENGLISH", "french": "FRENCH", "arabic": "ARABIC",
    "spanish": "SPANISH", "german": "GERMAN", "italian": "ITALIAN",
    "portuguese": "PORTUGUESE", "chinese": "CHINESE", "mandarin": "CHINESE",
    "dutch": "DUTCH", "russian": "RUSSIAN", "turkish": "TURKISH",
    "japanese": "JAPANESE", "korean": "KOREAN", "hindi": "HINDI",
    "persian": "PERSIAN", "farsi": "PERSIAN", "urdu": "URDU",
    "swedish": "SWEDISH", "norwegian": "NORWEGIAN", "danish": "DANISH",
    "polish": "POLISH", "czech": "CZECH", "romanian": "ROMANIAN",
}


def _detect_native_languages(raw_text: str) -> set[str]:
    """Return language keys that appear on or near a 'mother tongue' line."""
    native: set[str] = set()
    lines = raw_text.splitlines()
    for i, line in enumerate(lines):
        if not _LANG_NATIVE_HINT_RE.search(line):
            continue
        # Scan the hint line itself + 1 line before and 2 lines after.
        context = " ".join(lines[max(0, i - 1): i + 3]).lower()
        for lang_raw, lang_key in _LANG_NORMALIZE.items():
            if lang_raw in context:
                native.add(lang_key)
    return native


def _merge_deterministic(cv: CvExtractionResult, det: DeterministicExtractions, raw_text: str = "") -> None:
    """Override contact fields with high-confidence deterministic values."""
    if det.primary_email and (not cv.contact.email or not cv.contact.email.strip()):
        cv.contact.email = det.primary_email
    if det.primary_phone and (not cv.contact.phone or not cv.contact.phone.strip()):
        cv.contact.phone = det.primary_phone
    if det.primary_linkedin and (not cv.contact.linkedin or not cv.contact.linkedin.strip()):
        cv.contact.linkedin = det.primary_linkedin
    if det.primary_name and (not cv.contact.name or not cv.contact.name.strip()):
        cv.contact.name = det.primary_name
    # Override obviously wrong locations (e.g., email copied into location).
    if det.location_hint and (not cv.contact.location or not cv.contact.location.strip() or _is_probable_email(cv.contact.location)):
        cv.contact.location = det.location_hint

    # Merge languages the LLM missed, using keyword detection.
    if raw_text:
        _, _, det_langs = keyword_skills(raw_text.lower())
        existing = {(lp.language or "").upper() for lp in cv.languages}
        native_langs = _detect_native_languages(raw_text)
        for lang_raw in det_langs:
            lang_key = _LANG_NORMALIZE.get(lang_raw.lower(), lang_raw.upper())
            if lang_key not in existing:
                proficiency = "NATIVE" if lang_key in native_langs else None
                cv.languages.append(LanguageProficiency(language=lang_key, proficiency=proficiency))
                existing.add(lang_key)


async def run_cv_pipeline_async(
    pdf_bytes: bytes, ocr_engine: OcrEngine, llm_extractor: LlmExtractor
) -> CvExtractionResult:
    t0 = time.monotonic()
    try:
        raw_text = await asyncio.to_thread(ocr_engine.extract_text_from_pdf_bytes, pdf_bytes)
    except Exception as exc:
        cv = CvExtractionResult()
        cv.meta = {"error": f"ocr_failed: {exc}"}
        cv.confidence = compute_confidence(cv)
        return cv
    time_ocr_ms = int((time.monotonic() - t0) * 1000)
    if not raw_text.strip():
        cv = CvExtractionResult()
        cv.meta = {"time_ocr_ms": time_ocr_ms, "error": "ocr_empty_text"}
        cv.confidence = compute_confidence(cv)
        return cv

    # Run deterministic extraction first (fast, ~100ms) to build anchors for the LLM prompt.
    t1 = time.monotonic()
    try:
        det = await asyncio.to_thread(extract_deterministic, raw_text)
        anchors = format_anchor_block(det)
    except Exception as _det_exc:
        det = DeterministicExtractions()
        anchors = ""

    time_det_ms = int((time.monotonic() - t1) * 1000)

    t2 = time.monotonic()
    try:
        llm_res = await asyncio.to_thread(llm_extractor.structure_cv, raw_text, anchors)
    except Exception as _llm_exc:
        llm_res = _llm_exc
    time_llm_ms = int((time.monotonic() - t2) * 1000)
    time_combined_ms = time_det_ms + time_llm_ms

    t3 = time.monotonic()
    cv_errors: list[str] = []
    if isinstance(llm_res, Exception):
        cv = CvExtractionResult()
        cv_errors.append(f"llm_failed: {llm_res}")
    else:
        cv = llm_res

    _merge_deterministic(cv, det, raw_text=raw_text)
    _fill_summary(cv)
    sem_errors = semantic_validate(cv)
    if sem_errors:
        cv_errors.extend([f"semantic:{e}" for e in sem_errors])

    cv.confidence = compute_confidence(cv)
    time_postprocess_ms = int((time.monotonic() - t3) * 1000)
    # Preserve any enrichment data already attached to cv.meta (e.g. skill_catalog_matches).
    meta: dict = dict(cv.meta) if isinstance(cv.meta, dict) else {}
    meta.update({
        "time_ocr_ms": time_ocr_ms,
        "time_det_ms": time_det_ms,
        "time_llm_ms": time_combined_ms,
        "time_postprocess_ms": time_postprocess_ms,
    })
    if cv_errors:
        meta["errors"] = cv_errors[:20]
    cv.meta = meta
    return CvExtractionResult.model_validate(cv.model_dump(by_alias=True))


def run_cv_pipeline(
    pdf_bytes: bytes, ocr_engine: OcrEngine, llm_extractor: LlmExtractor
) -> CvExtractionResult:
    """Synchronous wrapper for callers without an event loop (e.g. RabbitMQ consumer)."""
    return asyncio.run(run_cv_pipeline_async(pdf_bytes, ocr_engine, llm_extractor))
