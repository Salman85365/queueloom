# QueueLoom

**See every background job. Understand why it failed.**

QueueLoom is observability for Python background jobs. Drop a two-line SDK into your Celery
app and get a timeline of every task: queued → started → succeeded / failed / retried, with
queue latency, duration, retry counts and full exception details, filterable by project,
environment, task and time range.

> Status: pre-alpha, under active development. Initial focus is **Celery + Redis/RabbitMQ**.
> RQ, Dramatiq, FastAPI background tasks and Temporal adapters are planned.

## Why

Celery's built-in tooling tells you a task failed. It rarely tells you *how long it sat in the
queue first*, *how many times it retried*, *which worker ran it*, or *what the failure rate for
that task looks like this week versus last week*. Flower shows live state but forgets history.
Sentry sees exceptions but not the lifecycle around them. QueueLoom stores the lifecycle.

## Quick start

```bash
pip install "queueloom[all]"

queueloom project create my-app        # prints an API key (ql_...)
queueloom serve                        # http://127.0.0.1:8800
```

Instrument your Celery app (client and/or worker side — both work):

```python
from celery import Celery
from queueloom.sdk.celery import instrument

app = Celery("my_app", broker="redis://localhost:6379/0")
instrument(app, endpoint="http://127.0.0.1:8800", api_key="ql_...", environment="prod")
```

Or configure through the environment and call `instrument(app)` with no arguments:

| Variable                 | Purpose                                   |
|--------------------------|-------------------------------------------|
| `QUEUELOOM_ENDPOINT`     | Server base URL                           |
| `QUEUELOOM_API_KEY`      | Project API key                           |
| `QUEUELOOM_ENVIRONMENT`  | Environment label (`prod`, `staging`, ...) |

Open `http://127.0.0.1:8800/projects/my-app`.

### Try it without any infrastructure

```bash
git clone https://github.com/Salman85365/queueloom && cd queueloom
pip install -e ".[dev]"
python demo/run_local.py
```

That starts a SQLite-backed server, an in-memory Celery broker and an in-process worker, then
keeps enqueuing a mix of fast, slow, flaky and broken tasks. Open the printed URL.

### Full stack with Docker

```bash
docker compose up --build
# http://localhost:8800/projects/demo
```

PostgreSQL + Redis + server + an instrumented Celery worker + a producer.

## What you get

- **Overview** per project: run counts by state, failure rate, p50/p95 duration, p95 queue
  latency, retries, and a per-task breakdown sorted by failures.
- **Task list** with filters for environment, task name, queue, state and time range.
- **Task detail**: full event timeline, worker, latency, duration, retries/attempts, exception
  type, message and traceback, parent/root links for chains and groups.
- **JSON API** for everything the dashboard shows (`/docs` for OpenAPI).

## How it works

```
Celery client ──before_task_publish──▶ SDK ─┐   (injects queueloom_published_at header)
                                            │
Celery worker ──task_prerun/success/──────▶ SDK ─┤ batched, non-blocking, fork-safe
                failure/retry/revoked            │
                                                 ▼
                                    POST /v1/events  (FastAPI)
                                                 │
                                 task_events (raw, append-only)
                                 task_runs   (one row per task, folded at ingest)
                                                 │
                                   Dashboard  ·  /v1/tasks  ·  /v1/stats
```

- The SDK hooks Celery signals and emits a versioned `TaskEvent` for each transition. Sending
  is a queue put; a daemon thread batches and POSTs. It never blocks or raises inside a task,
  drops (and counts) events when its bounded queue is full, and restarts itself after `fork`.
- The publish hook stamps a `queueloom_published_at` header on the message, so the worker can
  report queue latency even when the producer is not instrumented.
- The server stores raw events unchanged and folds them into `task_runs`, the table the
  dashboard reads. Ingestion is idempotent per `event_id`, so SDK retries are safe. Out-of-order
  events fill in details without regressing state.
- Storage is SQLAlchemy: SQLite for zero-config local use, PostgreSQL for real deployments.

### Event schema

Every event carries `schema_version` (currently `1`). Additive changes keep the version; anything
else bumps it and the server rejects versions it does not understand. See
[`src/queueloom/events.py`](src/queueloom/events.py).

Task arguments are **not** captured by default. `instrument(..., capture_args=True)` records a
truncated `repr()` of args/kwargs.

## Server configuration

All settings are environment variables prefixed with `QUEUELOOM_`:

| Variable                              | Default                     |
|---------------------------------------|-----------------------------|
| `QUEUELOOM_DATABASE_URL`              | `sqlite:///./queueloom.db`  |
| `QUEUELOOM_HOST` / `QUEUELOOM_PORT`   | `127.0.0.1` / `8800`        |
| `QUEUELOOM_AUTO_CREATE_SCHEMA`        | `true`                      |
| `QUEUELOOM_DASHBOARD_REFRESH_SECONDS` | `15`                        |
| `QUEUELOOM_STATS_SAMPLE_LIMIT`        | `50000`                     |

PostgreSQL example: `postgresql+psycopg://user:pass@host:5432/queueloom`.

## API

| Method | Path                | Notes                                            |
|--------|---------------------|--------------------------------------------------|
| POST   | `/v1/events`        | `{"events": [...]}`, up to 1000 per request       |
| GET    | `/v1/tasks`         | `environment, task_name, state, queue, range, since, until, limit, offset` |
| GET    | `/v1/tasks/{id}`    | Run plus its event timeline                      |
| GET    | `/v1/stats`         | Aggregates for a window, overall and per task    |
| GET    | `/v1/projects/me`   | Project for the supplied key                     |
| GET    | `/v1/health`        |                                                  |

Authenticate with `Authorization: Bearer <api key>`. The dashboard itself has no auth in this
release; run it on a trusted network or behind a reverse proxy with auth.

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
ruff check . && ruff format --check . && mypy
pytest                                    # SQLite
QUEUELOOM_TEST_DATABASE_URL=postgresql+psycopg://... pytest   # PostgreSQL
```

The test suite includes integration tests that run a real Celery worker in-process over the
memory broker, so SDK behaviour is verified end to end without Docker.

## Roadmap

- [ ] Failure-rate alerts with webhook delivery
- [ ] Alembic migrations and retention/cleanup job
- [ ] Dashboard authentication and multi-user projects
- [ ] AI incident summaries on top of the stored timelines
- [ ] Adapters: RQ, Dramatiq, FastAPI `BackgroundTasks`, Temporal
- [ ] Hosted version (QueueLoom Cloud)

## License

MIT
