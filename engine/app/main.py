from __future__ import annotations

import os
import threading
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from app.extractor import LlmExtractor
from app.ocr import OcrEngine
from app.pipeline import run_cv_pipeline_async
from app import rabbit_worker
from app.routers import explain_router
from app.schemas import CvExtractionResult

MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)

ocr_engine = OcrEngine()
llm_extractor = LlmExtractor(
    model_name=os.getenv("OLLAMA_MODEL", "llama3.2:3b"),
    base_url=os.getenv("OLLAMA_BASE_URL", "http://ollama:11434"),
    timeout_seconds=int(os.getenv("OLLAMA_TIMEOUT_SECONDS", "180")),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Warm up OCR in background to avoid blocking consumer startup.
    threading.Thread(target=ocr_engine.warmup, name="ocr-warmup", daemon=True).start()
    if os.getenv("ENABLE_RABBIT_CONSUMER", "true").lower() in ("1", "true", "yes"):
        rabbit_worker.start_rabbit_consumer_thread(ocr_engine, llm_extractor)
    yield


app = FastAPI(title="CV Extraction Engine", version="2.0.0", lifespan=lifespan)
app.include_router(explain_router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready")
def health_ready() -> JSONResponse:
    if os.getenv("ENABLE_RABBIT_CONSUMER", "true").lower() in ("1", "true", "yes"):
        if not rabbit_worker.consumer_ready.is_set():
            return JSONResponse(status_code=503, content={"status": "consumer_starting"})
        if rabbit_worker.consumer_thread is not None and not rabbit_worker.consumer_thread.is_alive():
            return JSONResponse(status_code=503, content={"status": "consumer_stopped"})
    return JSONResponse(content={"status": "ok"})


@app.post("/process", response_model=CvExtractionResult)
async def process(file: UploadFile = File(...)) -> JSONResponse:
    if file.content_type != "application/pdf" and not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")

    pdf_bytes = await file.read()
    if not pdf_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")
    if len(pdf_bytes) > MAX_FILE_SIZE_BYTES:
        raise HTTPException(status_code=413, detail="File exceeds maximum size of 10MB")

    try:
        result = await run_cv_pipeline_async(pdf_bytes, ocr_engine, llm_extractor)
        return JSONResponse(content=result.model_dump(by_alias=True))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"CV pipeline failed: {exc}") from exc
