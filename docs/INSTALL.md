# Install QueueLoom v0.1.0a1

This alpha is for evaluation and feedback. Python 3.11 or newer is required.
The release is distributed through [GitHub Releases](https://github.com/Salman85365/queueloom/releases/tag/v0.1.0a1).

## Install the released wheel

Create an empty working directory and a virtual environment:

```bash
mkdir queueloom-evaluation
cd queueloom-evaluation
python3 -m venv .venv
source .venv/bin/activate
python -m pip install "queueloom[server,celery] @ https://github.com/Salman85365/queueloom/releases/download/v0.1.0a1/queueloom-0.1.0a1-py3-none-any.whl"
python -m pip check
queueloom --version
```

The version command should print `queueloom 0.1.0a1`. The `server` extra installs the API,
dashboard, CLI server dependencies, and database drivers; `celery` adds the Celery adapter's
dependencies. The wheel contains the dashboard templates and database migrations.

On Windows PowerShell, use `py -3 -m venv .venv` and `.venv\Scripts\Activate.ps1` for the
environment creation and activation steps. Select an installed Python version of at least 3.11.

### Verify a downloaded file

Alternatively, download the wheel and `SHA256SUMS.txt` from the release Assets section.
Compare the wheel's SHA-256 digest with the matching line in that file:

```bash
# macOS
shasum -a 256 queueloom-0.1.0a1-py3-none-any.whl
# Linux
sha256sum queueloom-0.1.0a1-py3-none-any.whl
```

On PowerShell, use `Get-FileHash .\queueloom-0.1.0a1-py3-none-any.whl -Algorithm SHA256`.
Then install the local download with:

```bash
python -m pip install "./queueloom-0.1.0a1-py3-none-any.whl[server,celery]"
```

## Start a local server

From the same working directory and activated environment:

```bash
queueloom project create my-app
queueloom serve
```

Save the API key printed by `project create`; it is shown once. Open
<http://127.0.0.1:8800/projects/my-app>. The dashboard is initially empty until an instrumented
application submits events. The default SQLite database is created in the working directory;
run both commands from the same directory or set `QUEUELOOM_DATABASE_URL` explicitly.

The monitoring server does not start a Redis/RabbitMQ broker or your application's workers.
To connect an existing Celery app, follow the [README integration example](../README.md#quick-start)
using your app's broker and the new QueueLoom API key.

## Try sample jobs without a broker service

The current development branch includes a demo using SQLite and an in-memory Celery broker,
with demo fixes made after v0.1.0a1. To evaluate that version:

```bash
git clone --branch main --depth 1 https://github.com/Salman85365/queueloom.git
cd queueloom
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[server,celery]"
python demo/run_local.py
```

Open the URL printed by the demo. It generates successful jobs, slow reports, retries, and
intentional failures. Both the `default` and `reports` queues are consumed. See the
[technical walkthrough](WALKTHROUGH.md) for what to inspect and how to stop it.
Use `--branch v0.1.0a1` instead of `--branch main` to reproduce the original release's demo.

## Optional extras

Replace `server,celery` in the released-wheel command with the extras you need:

| Extra | Purpose |
| --- | --- |
| `celery` | Celery instrumentation and Redis broker client |
| `dramatiq` | Dramatiq middleware and Redis broker client |
| `rq` | RQ queue and worker integration |
| `fastapi` | FastAPI/Starlette background-task helpers |
| `temporal` | Temporal activity instrumentation |
| `server` | Ingestion API, dashboard, and database support |
| `ai` | Optional Anthropic-backed incident explanation |
| `all` | All adapters, server, and optional AI dependencies |

For example, use `server,dramatiq` for a local server and Dramatiq integration. In an existing
worker environment that only sends events, use its adapter extra without `server`.
The base package supports `queueloom --version` and `--help`; server commands require `server`.
AI is optional: local monitoring and deterministic incident reports work without an API key.

## Scope and limitations

- This is an alpha. Evaluate with test workloads and disposable data before relying on it.
- The local demo uses an in-memory broker; it does not validate distributed workers or broker failover.
- Transport delivery is best effort. Events can be dropped when buffers fill or delivery fails.
- The benchmark measures SDK transport submission and delivery to a local test collector;
  it is not a production capacity or full Celery overhead benchmark. See [method and results](BENCHMARK.md).
- Live third-party AI calls and every adapter/backend combination are not covered by the local demo.
- Before exposing a server beyond localhost, configure dashboard authentication and HTTPS as
  described in [deployment instructions](DEPLOY.md).

For a problem report, include the QueueLoom/Python versions, operating system, framework/broker,
reproduction steps, and the expected versus actual behavior. Use synthetic data and omit API keys
and private task payloads. [Report a bug](https://github.com/Salman85365/queueloom/issues/new?template=bug_report.yml)
or [share evaluation feedback](FEEDBACK.md).
