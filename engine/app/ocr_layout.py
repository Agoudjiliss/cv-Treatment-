from __future__ import annotations

from typing import Any


def ocr_lines_from_result(ocr_result: Any) -> list[tuple[list[list[float]], str]]:
    lines: list[tuple[list[list[float]], str]] = []
    if not ocr_result:
        return lines
    for page_result in ocr_result:
        if page_result is None:
            continue
        for item in page_result:
            if not item or len(item) < 2:
                continue
            box, info = item[0], item[1]
            if isinstance(info, (list, tuple)) and info:
                text = str(info[0])
            else:
                text = str(info)
            if isinstance(box, list) and len(box) >= 4:
                lines.append((box, text))
    return lines


def sort_reading_order(box_text_pairs: list[tuple[list[list[float]], str]], row_tol: float = 18.0) -> str:
    if not box_text_pairs:
        return ""

    scored: list[tuple[float, float, str]] = []
    for box, text in box_text_pairs:
        ys = [float(p[1]) for p in box]
        xs = [float(p[0]) for p in box]
        y_key = min(ys)
        x_key = min(xs)
        row_bucket = round(y_key / row_tol) * row_tol
        scored.append((row_bucket, x_key, text.strip()))

    scored.sort(key=lambda t: (t[0], t[1]))
    return "\n".join(t[2] for t in scored if t[2])


def extract_text_fallback(ocr_result: Any) -> str:
    # PaddleOCR v2/v3 may return nested dict/list structures without the
    # classic [box, (text, score)] tuples. This fallback recursively collects
    # textual candidates while filtering non-text metadata.
    texts: list[str] = []
    seen: set[str] = set()

    def maybe_add(value: str) -> None:
        text = value.strip()
        if len(text) < 2:
            return
        lower = text.lower()
        if lower in {"true", "false", "none"}:
            return
        if any(ch.isalpha() for ch in text) or any(ch.isdigit() for ch in text):
            if text not in seen:
                seen.add(text)
                texts.append(text)

    def walk(node: Any) -> None:
        if node is None:
            return
        if isinstance(node, str):
            maybe_add(node)
            return
        if isinstance(node, dict):
            for key, value in node.items():
                # Prioritize common OCR text keys.
                if isinstance(value, str) and key.lower() in {"text", "texts", "rec_text", "rec_texts", "transcription"}:
                    maybe_add(value)
                else:
                    walk(value)
            return
        if isinstance(node, (list, tuple)):
            for item in node:
                walk(item)

    walk(ocr_result)
    return "\n".join(texts)
