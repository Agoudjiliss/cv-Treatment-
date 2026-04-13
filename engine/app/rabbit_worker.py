from __future__ import annotations

import base64
import json
import logging
import os
import threading
from typing import TYPE_CHECKING

import pika

from app.extractor import LlmExtractor
from app.ocr import OcrEngine
from app.pipeline import run_cv_pipeline

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

consumer_ready = threading.Event()
consumer_thread: threading.Thread | None = None


def start_rabbit_consumer_thread(ocr_engine: OcrEngine, llm_extractor: LlmExtractor) -> threading.Thread:
    def run() -> None:
        try:
            _consume_loop(ocr_engine, llm_extractor)
        except Exception as exc:  # pragma: no cover
            consumer_ready.clear()
            logger.exception("RabbitMQ consumer crashed: %s", exc)

    global consumer_thread
    thread = threading.Thread(target=run, name="cv-rabbit-consumer", daemon=True)
    thread.start()
    consumer_thread = thread
    return thread


def _consume_loop(ocr_engine: OcrEngine, llm_extractor: LlmExtractor) -> None:
    host = os.getenv("RABBITMQ_HOST", "localhost")
    port = int(os.getenv("RABBITMQ_PORT", "5672"))
    user = os.getenv("RABBITMQ_USER", "guest")
    password = os.getenv("RABBITMQ_PASSWORD", "guest")
    parse_queue = os.getenv("CV_PARSE_QUEUE", "cv_parse_queue")
    result_queue = os.getenv("CV_RESULT_QUEUE", "cv_result_queue")
    dlq_queue = os.getenv("CV_DLQ_QUEUE", "cv_parse_dlq")
    exchange = os.getenv("CV_EXCHANGE", "cv.exchange")

    credentials = pika.PlainCredentials(user, password)
    params = pika.ConnectionParameters(host=host, port=port, credentials=credentials, heartbeat=600)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.exchange_declare(exchange=exchange, exchange_type="direct", durable=True)
    channel.queue_declare(
        queue=parse_queue,
        durable=True,
        arguments={"x-dead-letter-exchange": exchange, "x-dead-letter-routing-key": dlq_queue},
    )
    channel.queue_bind(queue=parse_queue, exchange=exchange, routing_key=parse_queue)
    channel.queue_declare(queue=result_queue, durable=True)
    channel.queue_declare(queue=dlq_queue, durable=True)
    channel.queue_bind(queue=dlq_queue, exchange=exchange, routing_key=dlq_queue)
    prefetch = int(os.getenv("RABBITMQ_PREFETCH", "2"))
    channel.basic_qos(prefetch_count=max(1, prefetch))

    def on_message(ch, method, _, body: bytes) -> None:
        correlation_id = ""
        try:
            data = json.loads(body.decode("utf-8"))
            correlation_id = str(data.get("correlationId", ""))
            pdf_b64 = data.get("pdfBase64")
            if not correlation_id or not pdf_b64:
                raise ValueError("correlationId and pdfBase64 are required")
            pdf_bytes = base64.b64decode(pdf_b64)
            result = run_cv_pipeline(pdf_bytes, ocr_engine, llm_extractor)
            payload = {
                "correlationId": correlation_id,
                "status": "ok",
                "result": result.model_dump(by_alias=True),
                "error": None,
            }
            ch.basic_publish(
                exchange="",
                routing_key=result_queue,
                body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
            )
        except Exception as exc:
            logger.exception("Parse job failed: %s", exc)
            err_payload = {
                "correlationId": correlation_id,
                "status": "error",
                "result": None,
                "error": str(exc),
            }
            try:
                ch.basic_publish(
                    exchange="",
                    routing_key=result_queue,
                    body=json.dumps(err_payload, ensure_ascii=False).encode("utf-8"),
                    properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
                )
            except Exception:
                logger.exception("Failed to publish error result")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=parse_queue, on_message_callback=on_message, auto_ack=False)
    consumer_ready.set()
    logger.info("Consuming queue %s", parse_queue)
    channel.start_consuming()
