# Changelog

## Unreleased

### Fixed

- Calculate nearest-rank percentiles correctly at exact rank boundaries, including p50
  for two samples and p95 for twenty samples.
- Preserve the latest retry attempt's worker and timing details when older events arrive late.
- Exit with a useful error when the local demo cannot start its server, and validate demo
  port, rate and duration arguments.
- Reuse a demo database without deleting its project and task history. The demo rotates its
  temporary ingestion key and disables scheduled retention and alert delivery.

### Documentation and evaluation

- A technical walkthrough of failures, retries, queue timing and deterministic diagnosis.
- Structured GitHub forms for bug reports and evaluation feedback.
- Clarified producer/worker instrumentation, process-wide Celery signals, and the exception
  samples included in optional AI reports; the dashboard now explains that data before sending.

These changes are on `main`; the published v0.1.0a1 release assets are unchanged.

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
