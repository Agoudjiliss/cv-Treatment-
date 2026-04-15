from __future__ import annotations

import base64
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import TYPE_CHECKING

import pika
from pika.exceptions import AMQPConnectionError, ChannelClosedByBroker, ConnectionClosedByBroker, StreamLostError

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


def consume_forever(ocr_engine: OcrEngine, llm_extractor: LlmExtractor) -> None:
    """Run the consume loop in current thread with reconnect/backoff."""
    backoff_s = 1.0
    while True:
        try:
            _consume_loop(ocr_engine, llm_extractor)
            backoff_s = 1.0
        except (AMQPConnectionError, StreamLostError, ConnectionClosedByBroker, ChannelClosedByBroker) as exc:
            consumer_ready.clear()
            logger.exception("RabbitMQ connection lost; reconnecting: %s", exc)
            time.sleep(backoff_s)
            backoff_s = min(backoff_s * 2.0, 30.0)
        except Exception as exc:  # pragma: no cover
            consumer_ready.clear()
            logger.exception("RabbitMQ consume loop crashed; restarting: %s", exc)
            time.sleep(backoff_s)
            backoff_s = min(backoff_s * 2.0, 30.0)


def _consume_loop(ocr_engine: OcrEngine, llm_extractor: LlmExtractor) -> None:
    host = os.getenv("RABBITMQ_HOST", "localhost")
    port = int(os.getenv("RABBITMQ_PORT", "5672"))
    user = os.getenv("RABBITMQ_USER", "guest")
    password = os.getenv("RABBITMQ_PASSWORD", "guest")
    parse_queue = os.getenv("CV_PARSE_QUEUE", "cv_parse_queue")
    result_queue = os.getenv("CV_RESULT_QUEUE", "cv_result_queue")
    dlq_queue = os.getenv("CV_DLQ_QUEUE", "cv_parse_dlq")
    exchange = os.getenv("CV_EXCHANGE", "cv.exchange")
    worker_threads = int(os.getenv("RABBITMQ_WORKER_THREADS", os.getenv("CV_WORKER_THREADS", "2")))
    max_in_flight = int(os.getenv("RABBITMQ_MAX_IN_FLIGHT", os.getenv("CV_MAX_IN_FLIGHT", "2")))

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
    effective_prefetch = max(1, min(prefetch, max_in_flight))
    channel.basic_qos(prefetch_count=effective_prefetch)

    # Enable publish confirms where supported; used to decide whether to ack.
    confirms_enabled = False
    try:
        channel.confirm_delivery()
        confirms_enabled = True
    except Exception:
        confirms_enabled = False

    executor = ThreadPoolExecutor(max_workers=max(1, worker_threads), thread_name_prefix="cv-job")
    publish_lock = threading.Lock()

    def _publish_json(payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        props = pika.BasicProperties(content_type="application/json", delivery_mode=2)
        # Pika channels are not thread-safe; protect publish + confirms.
        with publish_lock:
            ok = channel.basic_publish(exchange="", routing_key=result_queue, body=body, properties=props)
            if confirms_enabled and ok is False:
                raise RuntimeError("RabbitMQ publish not confirmed")

    def on_message(ch, method, _, body: bytes) -> None:
        delivery_tag = method.delivery_tag

        def _work() -> None:
            correlation_id = ""
            published = False
            t0 = time.monotonic()
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
                _publish_json(payload)
                published = True
                elapsed_ms = int((time.monotonic() - t0) * 1000)
                logger.info("CV parse completed correlationId=%s elapsed_ms=%s", correlation_id, elapsed_ms)
            except Exception as exc:
                logger.exception("Parse job failed: %s", exc)
                err_payload = {
                    "correlationId": correlation_id,
                    "status": "error",
                    "result": None,
                    "error": str(exc),
                }
                try:
                    _publish_json(err_payload)
                    published = True
                    elapsed_ms = int((time.monotonic() - t0) * 1000)
                    logger.info("CV parse errored correlationId=%s elapsed_ms=%s", correlation_id, elapsed_ms)
                except Exception:
                    logger.exception("Failed to publish error result")
            finally:
                if published:
                    connection.add_callback_threadsafe(lambda: ch.basic_ack(delivery_tag=delivery_tag))
                else:
                    # Publish failed (likely broker issue). Don't ack so message can be retried.
                    connection.add_callback_threadsafe(lambda: ch.basic_nack(delivery_tag=delivery_tag, requeue=True))

        executor.submit(_work)

    channel.basic_consume(queue=parse_queue, on_message_callback=on_message, auto_ack=False)
    consumer_ready.set()
    logger.info("Consuming queue %s", parse_queue)
    try:
        channel.start_consuming()
    finally:
        consumer_ready.clear()
        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
        try:
            connection.close()
        except Exception:
            pass
