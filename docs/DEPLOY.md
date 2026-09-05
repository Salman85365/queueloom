# Deploying QueueLoom

QueueLoom is one stateless web process plus PostgreSQL. Any platform that runs a container or a
Python web process works; configs for three are included.

## Environment variables to set on any platform

| Variable | Why |
|---|---|
| `QUEUELOOM_DATABASE_URL` | `postgresql+psycopg://user:pass@host:5432/dbname` (SQLite is for local use only) |
| `QUEUELOOM_DASHBOARD_PASSWORD` | Required on the internet; without it the dashboard is open |
| `QUEUELOOM_SECRET_KEY` | Long random string; signs session cookies |
| `QUEUELOOM_HTTPS_ONLY=true` | Marks the session cookie `Secure` behind TLS |
| `QUEUELOOM_PUBLIC_BASE_URL` | `https://queueloom.example.com`, used for links in webhooks |
| `ANTHROPIC_API_KEY` | Optional; enables AI summaries on the Diagnose page |

Run `queueloom migrate` before the new version serves traffic (all three configs below do), and
`queueloom project create <name>` once to obtain an API key for your SDKs.

## Fly.io

```bash
fly launch --copy-config --no-deploy
fly postgres create --name queueloom-db
fly postgres attach queueloom-db            # sets DATABASE_URL
fly secrets set QUEUELOOM_DATABASE_URL="$(fly ssh console -C 'printenv DATABASE_URL' | sed 's#postgres://#postgresql+psycopg://#')" \
  QUEUELOOM_DASHBOARD_PASSWORD='...' QUEUELOOM_SECRET_KEY="$(openssl rand -hex 32)" \
  QUEUELOOM_PUBLIC_BASE_URL=https://queueloom.fly.dev
fly deploy
fly ssh console -C "queueloom project create my-app"
```

`fly.toml` runs `queueloom migrate` as the release command and health-checks `/v1/health`.

## Render

Connect the repository and choose *Blueprint*; `render.yaml` provisions the web service and a
PostgreSQL instance and wires `QUEUELOOM_DATABASE_URL`. Fill in the two `sync: false` secrets
(dashboard password, public URL) in the Render dashboard, then create a project from the
service shell: `queueloom project create my-app`.

## Heroku (and Heroku-compatible platforms)

```bash
heroku create queueloom
heroku addons:create heroku-postgresql:essential-0
heroku config:set QUEUELOOM_DATABASE_URL="$(heroku config:get DATABASE_URL | sed 's#postgres://#postgresql+psycopg://#')" \
  QUEUELOOM_DASHBOARD_PASSWORD='...' QUEUELOOM_SECRET_KEY="$(openssl rand -hex 32)" \
  QUEUELOOM_HTTPS_ONLY=true QUEUELOOM_PUBLIC_BASE_URL=https://queueloom.herokuapp.com
git push heroku main
heroku run queueloom project create my-app
```

The `Procfile` runs migrations in the release phase. Heroku's Python buildpack installs the
package from `pyproject.toml`; add a `requirements.txt` containing `.[server,ai]` if the
buildpack needs one.

## Docker anywhere

```bash
docker build -t queueloom .
docker run -p 8800:8800 \
  -e QUEUELOOM_DATABASE_URL=postgresql+psycopg://... \
  -e QUEUELOOM_DASHBOARD_PASSWORD=... -e QUEUELOOM_SECRET_KEY=... \
  queueloom
```

The image runs `queueloom serve`, which applies migrations on startup unless
`QUEUELOOM_AUTO_MIGRATE=false`.

## Sizing notes

The ingestion path is one INSERT per event plus an upsert of the run row; a small instance keeps
up with thousands of events per minute. Retention (`QUEUELOOM_RETENTION_DAYS`, default 30) keeps
the database bounded. Percentile statistics sample the most recent
`QUEUELOOM_STATS_SAMPLE_LIMIT` runs per request.
