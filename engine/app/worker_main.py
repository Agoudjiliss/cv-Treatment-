from __future__ import annotations

import os

import structlog

from app.extractor import LlmExtractor
from app.ocr import OcrEngine
from app import rabbit_worker


structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)


def main() -> None:
    ocr_engine = OcrEngine()
    llm_extractor = LlmExtractor(
        model_name=os.getenv("OLLAMA_MODEL", "llama3.2:3b"),
        base_url=os.getenv("OLLAMA_BASE_URL", "http://ollama:11434"),
        timeout_seconds=int(os.getenv("OLLAMA_TIMEOUT_SECONDS", "180")),
    )
    # Blocking consumer loop (run this module as the worker container entrypoint).
    rabbit_worker.consume_forever(ocr_engine, llm_extractor)


if __name__ == "__main__":
    main()

