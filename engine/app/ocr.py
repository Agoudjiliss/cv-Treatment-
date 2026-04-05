from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pypdfium2 as pdfium
from paddleocr import PaddleOCR

from app.ocr_layout import extract_text_fallback, ocr_lines_from_result, sort_reading_order


class OcrEngine:
    def __init__(self) -> None:
        # Avoid slow startup connectivity checks in constrained networks.
        os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")
        self._ocr: PaddleOCR | None = None

    def _get_ocr(self) -> PaddleOCR:
        if self._ocr is not None:
            return self._ocr

        # PaddleOCR API changed across versions; try lightweight constructors first
        # to reduce memory usage and avoid OOM kills in containerized runs.
        constructors = [
            {
                "lang": "en",
                "use_doc_orientation_classify": False,
                "use_doc_unwarping": False,
                "use_textline_orientation": False,
                "text_detection_model_name": "PP-OCRv5_mobile_det",
                "text_recognition_model_name": "en_PP-OCRv5_mobile_rec",
            },
            {"lang": "en", "use_angle_cls": True},
            {"lang": "en"},
        ]
        last_error: Exception | None = None
        for kwargs in constructors:
            try:
                self._ocr = PaddleOCR(**kwargs)
                return self._ocr
            except Exception as exc:  # pragma: no cover - version dependent
                last_error = exc
        raise RuntimeError(f"Failed to initialize PaddleOCR: {last_error}")

    def warmup(self) -> None:
        self._get_ocr()

    def extract_text_from_pdf_bytes(self, pdf_bytes: bytes) -> str:
        with tempfile.TemporaryDirectory(prefix="cv-pages-") as tmp_dir:
            tmp_path = Path(tmp_dir)
            pdf_path = tmp_path / "input.pdf"
            pdf_path.write_bytes(pdf_bytes)

            pdf = pdfium.PdfDocument(str(pdf_path))
            pages_text: list[str] = []
            try:
                for page_index in range(len(pdf)):
                    page = pdf[page_index]
                    native_text = self._extract_native_text(page)
                    if native_text:
                        pages_text.append(native_text)
                        continue

                    bitmap = page.render(scale=1.5)
                    pil_image = bitmap.to_pil()
                    image_path = tmp_path / f"page-{page_index + 1}.png"
                    pil_image.save(image_path, format="PNG")

                    try:
                        ocr = self._get_ocr()
                        ocr_result = self._run_ocr(ocr, str(image_path))
                    except Exception as exc:
                        raise RuntimeError(f"PaddleOCR failed on page {page_index + 1}") from exc
                    pairs = ocr_lines_from_result(ocr_result)
                    page_text = sort_reading_order(pairs)
                    if page_text:
                        pages_text.append(page_text)
                    else:
                        pages_text.append(extract_text_fallback(ocr_result))
            finally:
                pdf.close()

            text = "\n\n".join(pages_text).strip()
            if not text:
                raise RuntimeError("PaddleOCR produced empty text")
            return text

    def _extract_native_text(self, page) -> str:
        try:
            text_page = page.get_textpage()
        except Exception:
            return ""
        try:
            text = ""
            if hasattr(text_page, "get_text_range"):
                text = str(text_page.get_text_range()).strip()
            elif hasattr(text_page, "get_text_bounded"):
                text = str(text_page.get_text_bounded()).strip()
            if len(text) < 80:
                return ""
            if not self._looks_readable_native_text(text):
                return ""
            return text
        except Exception:
            return ""
        finally:
            close_fn = getattr(text_page, "close", None)
            if callable(close_fn):
                close_fn()

    def _looks_readable_native_text(self, text: str) -> bool:
        total = len(text)
        if total == 0:
            return False
        whitespace_count = sum(1 for c in text if c.isspace())
        alpha_count = sum(1 for c in text if c.isalpha())
        space_ratio = whitespace_count / total
        alpha_ratio = alpha_count / total
        words = [w for w in text.split() if w]
        if not words:
            return False
        avg_word_len = sum(len(w) for w in words) / len(words)

        # Heuristic to reject broken PDF native text extraction and fallback to OCR.
        return space_ratio >= 0.06 and alpha_ratio >= 0.35 and avg_word_len <= 16.0

    def _run_ocr(self, ocr: PaddleOCR, image_path: str):
        # PaddleOCR call signatures changed across versions.
        call_attempts = (
            {"cls": True},
            {},
        )
        last_error: Exception | None = None
        for kwargs in call_attempts:
            try:
                result = ocr.ocr(image_path, **kwargs)
                return result or []
            except TypeError as exc:
                last_error = exc
                if "unexpected keyword argument" in str(exc):
                    continue
                raise
            except Exception as exc:
                last_error = exc
                # Some PaddleOCR versions expose kwargs but fail at runtime when
                # corresponding orientation models are not initialized.
                if kwargs and (
                    "textline_orientation_model" in str(exc)
                    or "Set use_textline_orientation" in str(exc)
                ):
                    continue
                raise
        raise RuntimeError(f"PaddleOCR call failed: {last_error}")
