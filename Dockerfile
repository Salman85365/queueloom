FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY demo ./demo
RUN pip install --upgrade pip && pip install ".[server,celery]"

EXPOSE 8800
CMD ["queueloom", "serve", "--host", "0.0.0.0"]
