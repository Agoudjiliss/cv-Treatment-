from __future__ import annotations

import asyncio
import time

from app.extractor import LlmExtractor
from app.ocr import OcrEngine
from app.pipeline.validator import semantic_validate
from app.schemas import CvExtractionResult


def compute_confidence(cv: CvExtractionResult) -> float:
    filled = sum(
        [
            bool(cv.contact.name),
            bool(cv.contact.email),
            bool(cv.contact.phone),
            bool(cv.contact.location),
            bool(cv.education),
            bool(cv.experience),
            bool(cv.skills.technical),
            bool(cv.summary),
        ]
    )
    return round(filled / 8, 2)


def run_cv_pipeline(pdf_bytes: bytes, ocr_engine: OcrEngine, llm_extractor: LlmExtractor) -> CvExtractionResult:
    async def process_cv() -> CvExtractionResult:
        t0 = time.monotonic()
        raw_text = await asyncio.to_thread(ocr_engine.extract_text_from_pdf_bytes, pdf_bytes)
        time_ocr_ms = int((time.monotonic() - t0) * 1000)
        if not raw_text.strip():
            raise RuntimeError("PaddleOCR returned empty text")

        t1 = time.monotonic()
        cv = await asyncio.to_thread(llm_extractor.structure_cv, raw_text)
        time_llm_ms = int((time.monotonic() - t1) * 1000)

        t2 = time.monotonic()
        errors = semantic_validate(cv)
        if errors:
            raise RuntimeError(f"Semantic validation failed: {', '.join(errors)}")

        cv.confidence = compute_confidence(cv)
        time_postprocess_ms = int((time.monotonic() - t2) * 1000)
        cv.meta = {
            "time_ocr_ms": time_ocr_ms,
            "time_deterministic_ms": 0,
            "time_llm_ms": time_llm_ms,
            "time_postprocess_ms": time_postprocess_ms,
        }
        return CvExtractionResult.model_validate(cv.model_dump(by_alias=True))

    return asyncio.run(process_cv())
