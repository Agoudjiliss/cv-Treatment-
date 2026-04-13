from __future__ import annotations

import asyncio
import time

from app.deterministic_extractor import DeterministicExtractions, extract_deterministic
from app.extractor import LlmExtractor
from app.ocr import OcrEngine
from app.pipeline.validator import semantic_validate
from app.schemas import CvExtractionResult


def compute_confidence(cv: CvExtractionResult) -> float:
    skill_signal = bool(
        cv.skills.technical
        or cv.skills.soft
        or cv.skills.languages
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


def _merge_deterministic(cv: CvExtractionResult, det: DeterministicExtractions) -> None:
    """Override contact fields with high-confidence deterministic values."""
    if det.primary_email and not cv.contact.email:
        cv.contact.email = det.primary_email
    if det.primary_phone and not cv.contact.phone:
        cv.contact.phone = det.primary_phone
    if det.primary_linkedin and not cv.contact.linkedin:
        cv.contact.linkedin = det.primary_linkedin
    if det.primary_name and not cv.contact.name:
        cv.contact.name = det.primary_name
    if det.location_hint and not cv.contact.location:
        cv.contact.location = det.location_hint


async def run_cv_pipeline_async(
    pdf_bytes: bytes, ocr_engine: OcrEngine, llm_extractor: LlmExtractor
) -> CvExtractionResult:
    t0 = time.monotonic()
    raw_text = await asyncio.to_thread(ocr_engine.extract_text_from_pdf_bytes, pdf_bytes)
    time_ocr_ms = int((time.monotonic() - t0) * 1000)
    if not raw_text.strip():
        raise RuntimeError("PaddleOCR returned empty text")

    t1 = time.monotonic()
    llm_task = asyncio.to_thread(llm_extractor.structure_cv, raw_text)
    det_task = asyncio.to_thread(extract_deterministic, raw_text)
    cv, det = await asyncio.gather(llm_task, det_task)
    time_llm_ms = int((time.monotonic() - t1) * 1000)

    t2 = time.monotonic()
    _merge_deterministic(cv, det)
    errors = semantic_validate(cv)
    if errors:
        raise RuntimeError(f"Semantic validation failed: {', '.join(errors)}")

    cv.confidence = compute_confidence(cv)
    time_postprocess_ms = int((time.monotonic() - t2) * 1000)
    cv.meta = {
        "time_ocr_ms": time_ocr_ms,
        "time_deterministic_ms": time_llm_ms,
        "time_llm_ms": time_llm_ms,
        "time_postprocess_ms": time_postprocess_ms,
    }
    return CvExtractionResult.model_validate(cv.model_dump(by_alias=True))


def run_cv_pipeline(
    pdf_bytes: bytes, ocr_engine: OcrEngine, llm_extractor: LlmExtractor
) -> CvExtractionResult:
    """Synchronous wrapper for callers without an event loop (e.g. RabbitMQ consumer)."""
    return asyncio.run(run_cv_pipeline_async(pdf_bytes, ocr_engine, llm_extractor))
