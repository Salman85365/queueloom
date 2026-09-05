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
- **Alerts**: failure-rate rules over a sliding window (per environment and/or task) that
  fire a webhook (generic JSON or Slack) when crossed and again when resolved, with cooldown,
  optional HMAC signing, and a delivery log.
- **Diagnose**: a deterministic incident report for any window: which tasks regressed
  against the previous window (failure rate, p95 duration, queue latency, retry storms),
  failures clustered by exception with variable parts normalised, stuck tasks. Optionally
  explained by Claude, which only ever sees that report, never raw data.
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

## Alerts

```bash
queueloom alert create my-app --webhook-url https://hooks.slack.com/services/... \
    --format slack --threshold 0.1 --window 15 --min-runs 20 --task-name app.send_email
```

A rule fires when `failed / (failed + succeeded)` over the last `window` minutes is at least
`threshold`, provided at least `min-runs` runs finished in that window. It fires once, stays
`firing` quietly, sends a `resolved` webhook when the rate drops below the threshold, and will
not fire again within `cooldown` minutes of resolving. Rules can also be managed on the
dashboard's Alerts page or via `POST /v1/alerts`.

The server evaluates rules every `QUEUELOOM_ALERT_EVAL_INTERVAL_SECONDS` (default 30). Set it to
`0` and run `queueloom alert evaluate` from cron, or call `POST /v1/alerts/evaluate`, if you
prefer to control scheduling yourself.

Webhook payload (`json` format):

```json
{
  "type": "queueloom.alert.fired",
  "text": "🔴 QueueLoom alert firing: emails — failure rate 25.0% (5/20 runs) ...",
  "project": "my-app",
  "rule": {"id": 1, "name": "emails", "environment": null, "task_name": "app.send_email",
           "window_minutes": 15, "threshold": 0.1, "min_runs": 20},
  "failure_rate": 0.25, "failed": 5, "finished": 20,
  "window": {"since": "...", "until": "..."},
  "occurred_at": "...", "link": "https://queueloom.example/projects/my-app/alerts"
}
```

With `--secret`, each request carries `X-QueueLoom-Signature: sha256=<hex HMAC of the body>`.
`slack` format sends only `{"text": ...}`, which Slack, Discord-compatible and most chat
webhooks accept.

## AI incident summaries

The Diagnose page (and `GET /v1/diagnosis`) always works without any model. To have the report
explained, ranked by likely cause, with next steps:

```bash
pip install "queueloom[ai]"
export ANTHROPIC_API_KEY=sk-ant-...
```

then press *Generate summary* on the Diagnose page or call `POST /v1/diagnosis/summary` with
an optional `{"question": "..."}`. The model receives only the deterministic report, so it
cannot invent telemetry; anything it could not determine is listed under "Not enough data for".

| Variable                      | Default         | Notes                                              |
|-------------------------------|-----------------|----------------------------------------------------|
| `QUEUELOOM_AI_PROVIDER`       | `auto`          | `auto` = Anthropic if a key is set, else `template` |
| `QUEUELOOM_AI_MODEL`          | `claude-opus-5` |                                                    |
| `QUEUELOOM_AI_EFFORT`         | `medium`        | `low` … `max`                                      |
| `QUEUELOOM_AI_MAX_TOKENS`     | `2000`          |                                                    |
| `QUEUELOOM_AI_FALLBACKS`      | `true`          | Server-side fallback if the primary model declines |
| `QUEUELOOM_ANTHROPIC_API_KEY` | unset           | Alternative to `ANTHROPIC_API_KEY`                 |

Providers implement a one-method interface (`queueloom.ai.SummaryProvider`), so another vendor
or a local model is a small class away.

## Database migrations

The server runs pending Alembic migrations on startup (`QUEUELOOM_AUTO_MIGRATE=true`). To
manage schema changes yourself, disable that and run:

```bash
queueloom migrate
```

Databases created by the earliest pre-alpha builds (before migrations existed) are detected and
stamped automatically.

## Dashboard access

Set `QUEUELOOM_DASHBOARD_PASSWORD` to require a login for the dashboard; sessions are signed
with `QUEUELOOM_SECRET_KEY` (set one, otherwise sessions reset on restart). With no password
configured the dashboard is open and shows a warning banner, which is fine on `localhost` and
not fine on the internet. Set `QUEUELOOM_HTTPS_ONLY=true` behind TLS. The JSON API always
authenticates with per-project API keys and is unaffected by the dashboard password.

## Server configuration

All settings are environment variables prefixed with `QUEUELOOM_`:

| Variable                              | Default                     |
|---------------------------------------|-----------------------------|
| `QUEUELOOM_DATABASE_URL`              | `sqlite:///./queueloom.db`  |
| `QUEUELOOM_HOST` / `QUEUELOOM_PORT`   | `127.0.0.1` / `8800`        |
| `QUEUELOOM_AUTO_MIGRATE`              | `true`                      |
| `QUEUELOOM_DASHBOARD_PASSWORD`        | unset (open dashboard)      |
| `QUEUELOOM_SECRET_KEY`                | unset (random per process)  |
| `QUEUELOOM_HTTPS_ONLY`                | `false`                     |
| `QUEUELOOM_PUBLIC_BASE_URL`           | unset (no links in webhooks)|
| `QUEUELOOM_ALERT_EVAL_INTERVAL_SECONDS` | `30`                      |
| `QUEUELOOM_WEBHOOK_TIMEOUT_SECONDS`   | `10`                        |
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
| POST/GET | `/v1/alerts`      | Create / list alert rules                        |
| GET/DELETE | `/v1/alerts/{id}` | Rule detail with current measurement / delete   |
| GET    | `/v1/alerts/{id}/events` | Fired / resolved / test history              |
| POST   | `/v1/alerts/{id}/test` | Send a test payload to the webhook            |
| POST   | `/v1/alerts/evaluate` | Evaluate this project's rules now              |
| GET    | `/v1/diagnosis`     | Deterministic diagnosis + text report for a window |
| POST   | `/v1/diagnosis/summary` | Diagnosis explained by the AI provider        |
| GET    | `/v1/health`        |                                                  |

Authenticate with `Authorization: Bearer <api key>`.

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

- [x] Failure-rate alerts with webhook delivery
- [x] Alembic migrations
- [x] Dashboard authentication (single password; multi-user projects later)
- [ ] Retention / cleanup job for old events
- [x] AI incident summaries on top of a deterministic diagnosis
- [ ] Adapters: RQ, Dramatiq, FastAPI `BackgroundTasks`, Temporal
- [ ] Hosted version (QueueLoom Cloud)

## License

MIT
