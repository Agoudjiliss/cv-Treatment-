# CV Intelligence Platform v2.0

Production-oriented, stateless CV intelligence stack:

- `gateway` (Spring Boot 3 / Java 21) for parse, score, and million-CV match APIs
- `engine` (FastAPI) for OCR + deterministic extraction + Ollama structuring + explain API
- `rabbitmq` for async parse workflow with DLQ and retry
- `ollama` local OSS model runtime (`llama3.2:3b`)
- `elasticsearch` for indexed CV retrieval and ANN-friendly candidate search

## Core Endpoints

- `POST /api/v1/cv/parse`
- `POST /api/v1/cv/score`
- `POST /api/v1/jobs/match`
- `POST /api/v1/cv/explain` (engine)

## Parse Pipeline

1. Gateway validates upload (max 10MB, PDF MIME + `%PDF` magic number), creates `correlationId`, publishes to `cv_parse_queue`.
2. Engine consumes message, runs:
   - OCR (`pypdf` first, PaddleOCR fallback)
   - deterministic extraction
   - LLM structuring (with circuit breaker and retry)
   - semantic validation and post-processing
3. Engine publishes to `cv_result_queue` with the same `correlationId`.
4. Gateway listener resolves pending future, returns JSON with headers:
   - `X-Correlation-Id`
   - `X-Processing-Time-Ms`

If a message fails max retries, it goes to `cv_parse_dlq`, and a structured error is sent to `cv_result_queue`.

## Matching Pipeline (1M-CV design)

`POST /api/v1/jobs/match` runs:

1. **Phase 1** Boolean filter (skills + seniority + optional location) in Elasticsearch.
2. **Phase 2** Vector shortlist using cached ONNX embeddings.
3. **Phase 3** Exact rerank + optional explanation calls (`/api/v1/cv/explain`) for top-K.

## Additive Parse Fields

Parse output keeps existing schema and adds:

- `llm_used: boolean`
- `confidence: float`
- `_meta` timing map:
  - `time_ocr_ms`
  - `time_deterministic_ms`
  - `time_llm_ms`
  - `time_postprocess_ms`

## Docker Services

- Gateway: `http://localhost:8080`
- Engine: `http://localhost:8000`
- Ollama: `http://localhost:11434`
- RabbitMQ AMQP: `localhost:5672`
- RabbitMQ UI: `http://localhost:15672`
- Elasticsearch: `http://localhost:9200`

## Run

```bash
docker compose up --build
docker compose exec ollama ollama pull llama3.2:3b
sh scripts/create-es-index.sh
sh scripts/health-check.sh
```

## Security Baseline

- RabbitMQ default credentials use env vars (`cvuser/cvpass` in compose defaults)
- CORS origins configurable by `CORS_ALLOWED_ORIGINS`
- Parse rejects oversized files (`413`) and non-PDF magic bytes (`415`)
- Structured JSON error responses across gateway and engine

## Health Check Script

`scripts/health-check.sh` verifies within 120 seconds:

1. Gateway `/actuator/health` is `UP`
2. Engine `/health/ready` is `ok`
3. Ollama `/api/tags` contains `llama3.2:3b`
4. RabbitMQ queues API contains `cv_parse_queue` and `cv_result_queue`

## Elasticsearch Index Mapping

Use `scripts/create-es-index.sh` to create mapping with:

- `embeddingVector` as `dense_vector` (`dims=384`, `similarity=cosine`, `index=true`)
- `skillsSet`, `languages`, `locationCountry` as keyword fields
- `cvJson` payload and `indexedAt` timestamp

## Notes on Fallback

If Elasticsearch is unavailable, the code is structured so matching services can be extended with an in-memory ANN fallback (FAISS/flat index strategy) without changing API contracts.
