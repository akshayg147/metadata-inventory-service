# 🔍 Metadata Collection Service

A production-grade FastAPI + MongoDB + Kafka service that collects and caches URL metadata (HTTP headers, cookies, and page source). Features Kafka-powered background processing with retry logic, dead letter queues, and classified error handling.

---

## Architecture

```
┌─────────────┐    ┌──────────────────────────────┐
│             │    │        API Container          │
│   Client    │───▶│  Routes → Service → MongoDB   │
│             │    │           ↓                    │
└─────────────┘    │    Kafka Producer              │
                   └──────────┬───────────────────┘
                              │
                       ┌──────▼──────┐
                       │    Kafka     │
                       │  (KRaft)    │
                       ├─────────────┤
                       │ metadata-   │
                       │ tasks       │──────────┐
                       │ metadata-   │          │
                       │ tasks-dlq   │    ┌─────▼──────────┐
                       └─────────────┘    │ Worker Container │
                                          │ Consumer →       │
                                          │ fetch_url →      │
                                          │ MongoDB upsert   │
                                          └──────────────────┘
```

### Layered Architecture

| Layer | Path | Responsibility |
|-------|------|----------------|
| **API** | `app/api/` | HTTP transport, request validation, response serialization |
| **Domain** | `app/domain/` | Pure business logic, framework-agnostic |
| **Infrastructure** | `app/infrastructure/` | MongoDB, Kafka, HTTP crawler |
| **Worker** | `app/worker/` | Background task processing |
| **Core** | `app/core/` | Configuration, logging, exceptions, lifespan |
| **Utils** | `app/utils/` | URL normalisation, shared utilities |

---

## Quick Start

**Note**: please use any url other than google.com, because I have written it in test, so google.com entry might already be present in the database if tests are ran before running the full stack.

**Note**: Please copy the .env.example file as .env as it is.

### Prerequisites
- Docker & Docker Compose

### Run the full stack

```bash
git clone <repo-url>
cd metadata-service
docker-compose up --build
```

The API will be available at **http://localhost:8000**.

### Run locally (for development)

```bash
# Start MongoDB and Kafka only
docker-compose up mongodb kafka -d

# Install dependencies
pip install -r requirements.txt

# Run the API (uses .env with localhost URIs)
uvicorn app.main:app --reload
```

### API Documentation

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

---

## API Endpoints

### `POST /api/v1/metadata`
Collect and store metadata for a URL (synchronous).

```bash
curl -X POST http://localhost:8000/api/v1/metadata \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com"}'
```

**Response** `201 Created`:
```json
{
  "id": "64f1a2b3...",
  "url": "https://example.com/",
  "headers": {"content-type": "text/html", "...": "..."},
  "cookies": {},
  "page_source": "<!doctype html>...",
  "status_code": 200,
  "status": "completed",
  "collected_at": "2024-01-01T00:00:00Z"
}
```

### `GET /api/v1/metadata?url=<URL>`
Retrieve cached metadata or trigger background collection.

```bash
# Cache hit → 200
curl "http://localhost:8000/api/v1/metadata?url=https://example.com"

# Cache miss → 202 (background collection via Kafka)
curl "http://localhost:8000/api/v1/metadata?url=https://new-site.com"
```

**Response** `200 OK` (cache hit): Full metadata object.

**Response** `202 Accepted` (cache miss):
```json
{
  "url": "https://new-site.com",
  "status": "pending",
  "message": "Metadata collection has been scheduled. Please retry shortly."
}
```

### `GET /health`
Service health check.

```bash
curl http://localhost:8000/health
```

---

## Background Processing & Error Handling

### Kafka-Powered Pipeline

When a cache miss occurs on GET, the URL is published to Kafka's `metadata-tasks` topic. The worker consumer picks it up, fetches the URL, and stores the result. Error handling is fully classified:

```
GET /metadata?url=X → cache miss
  → Producer publishes to "metadata-tasks"
  → Consumer picks up message
  → Worker calls fetch_url(X)
      ├── Success (2xx)     → upsert to MongoDB, commit offset
      ├── Permanent (404)   → DLQ immediately, mark_failed in DB
      └── Transient (503)   → retry (up to 3x), then DLQ
```

### Error Classification

| Error Type | Examples | Action |
|---|---|---|
| **Permanent** | 404, 403, 410, DNS not found, TLS error, too many redirects | → DLQ immediately, `status="failed"` in DB |
| **Transient** | 503, 429, 500, connection timeout, connection refused | → Retry up to `KAFKA_MAX_RETRIES`, then DLQ |

### Dead Letter Queue

Permanently failed and exhausted-retry messages are published to `metadata-tasks-dlq` with full context:
```json
{
  "url": "https://nonexistent.example.com",
  "retry_count": 3,
  "error": "DNS resolution failed — domain does not exist"
}
```

### Manual Offset Commit

The consumer uses `enable.auto.commit=False` with manual commit:
- **Success** → commit offset
- **Permanent failure** → DLQ + commit offset (move forward)
- **Transient failure** → re-publish with `retry_count` + commit offset

This ensures at-least-once delivery without blocking partitions on permanent failures.

---

## Running Tests

```bash
# Install dependencies locally
pip install -r requirements.txt

# Run all tests
pytest -v

# Run unit tests only
pytest tests/unit/ -v

# Run integration tests only
pytest tests/integration/ -v
```

---

## Configuration

All settings are managed via environment variables (`.env` file):

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGO_DB_NAME` | `metadata_service` | Database name |
| `HTTP_TIMEOUT` | `30` | Timeout for outbound HTTP requests (seconds) |
| `LOG_LEVEL` | `INFO` | Logging level |
| `API_HOST` | `0.0.0.0` | API bind host |
| `API_PORT` | `8000` | API bind port |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker(s) |
| `KAFKA_TOPIC` | `metadata-tasks` | Main task topic (3 partitions) |
| `KAFKA_CONSUMER_GROUP` | `metadata-workers` | Consumer group ID |
| `KAFKA_DLQ_TOPIC` | `metadata-tasks-dlq` | Dead letter queue topic |
| `KAFKA_MAX_RETRIES` | `3` | Max retries before DLQ |

> **Note**: When running inside Docker Compose, `MONGO_URI` and `KAFKA_BOOTSTRAP_SERVERS` are overridden to use Docker service names (`mongodb:27017`, `kafka:9092`). The `.env` defaults (`localhost`) are used for local development outside Docker.

---

## Design Decisions

### Confluent Kafka (KRaft mode)
- **No Zookeeper dependency** — single Kafka container using KRaft consensus
- `acks=all` with snappy compression on the producer
- Manual offset commit for at-least-once delivery
- Explicit topic creation via AdminClient at startup (no reliance on auto-create)
- 3 partitions on the main topic for parallel consumer scaling

### Error Classification & Retry Strategy
- HTTP status codes and network error types are classified at the crawler level  
- Permanent errors (4xx, bad DNS) skip retries entirely — no wasted resources
- Transient errors (5xx, timeouts) are retried via re-publish with an incrementing `retry_count`
- Exhausted retries and permanent failures go to a dedicated DLQ topic

### Idempotency
- **Unique index** on `url` prevents duplicate records
- **Upsert semantics** ensure repeated POSTs update rather than duplicate
- **Atomic `mark_pending`** prevents concurrent background fetches for the same URL
- **At-least-once + upserts** = safe duplicate processing

### URL Normalisation
URLs are canonicalised before storage (lowercase hostname, sorted query params, stripped fragments) so `HTTP://EXAMPLE.COM` and `https://example.com/` resolve to the same record.

### Error Resilience
- **MongoDB retry-backoff**: Up to 5 attempts with exponential delay during startup
- **Docker health checks**: API and worker start only after MongoDB and Kafka are healthy
- **Graceful shutdown**: Kafka producer flushes, consumer closes cleanly, MongoDB disconnects

---

## Project Structure

```
metadata-service/
├── app/
│   ├── main.py
│   ├── api/
│   │   ├── routes.py
│   │   ├── dependencies.py
│   │   └── schemas.py
│   ├── core/
│   │   ├── config.py
│   │   ├── logging.py
│   │   ├── exceptions.py          # PermanentCollectionError, TransientCollectionError
│   │   └── lifespan.py            # MongoDB + Kafka lifecycle
│   ├── domain/
│   │   ├── models.py
│   │   └── metadata_service.py
│   ├── infrastructure/
│   │   ├── db/
│   │   │   ├── mongo.py
│   │   │   └── repository.py
│   │   ├── messaging/
│   │   │   ├── producer.py        # Kafka producer + topic creation
│   │   │   └── consumer.py        # Kafka consumer + retry/DLQ logic
│   │   └── crawler/
│   │       └── http_client.py     # Error classification (permanent/transient)
│   ├── worker/
│   │   └── worker.py
│   └── utils/
│       └── url_normalizer.py
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── test_collector.py
│   │   ├── test_metadata_service.py
│   │   └── test_url_normalizer.py
│   └── integration/
│       └── test_api.py
├── docker/
│   ├── Dockerfile.api
│   └── Dockerfile.worker
├── docker-compose.yml
├── requirements.txt
├── pyproject.toml
└── .env
```

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.11+ |
| Web Framework | FastAPI |
| Database | MongoDB 7 (via Motor async driver) |
| Messaging | Confluent Kafka (KRaft, no Zookeeper) |
| HTTP Client | httpx (async) |
| Orchestration | Docker Compose |
| Testing | pytest + pytest-asyncio |
