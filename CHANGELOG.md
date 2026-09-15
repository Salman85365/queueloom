# Changelog

## 0.1.0a1

First packaged alpha release, intended for evaluation and feedback.

### Included

- Background-job lifecycle events, a batched HTTP transport, and a self-hosted API and dashboard.
- Celery instrumentation as the primary focus, plus Dramatiq, RQ, FastAPI/Starlette, and Temporal adapters.
- SQLite and PostgreSQL support, packaged database migrations, project API keys, and dashboard authentication.
- Alert rules, retention controls, deterministic incident reports, and optional AI explanations.
- A local Celery demo with successful jobs, retries, failures, and slow reports; both demo queues are consumed.

### Release readiness

- Wheel and source distribution, with an [installation guide](docs/INSTALL.md).
- The base package now includes the dependency required for CLI help and version commands.
- The Celery and Dramatiq extras include the Redis broker clients used in the examples.
- Automated fresh-install checks for the SDK, CLI, migrations, event ingestion, and dashboard resources in both distributions.
- A [reproducible transport benchmark](docs/BENCHMARK.md) with raw results and an explicit measurement scope.

### Alpha limitations

APIs and storage behavior may change before a stable release. Event delivery is best effort;
full buffers and delivery failures can drop events. The demo uses an in-memory broker, and the
benchmark uses a local test collector. Neither establishes production capacity or distributed
broker reliability. Live external AI calls require separate configuration and validation.
