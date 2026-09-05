"""Demo Celery app with a realistic mix of fast, slow, flaky and broken tasks.

Run a worker against Redis/RabbitMQ::

    QUEUELOOM_ENDPOINT=http://localhost:8800 QUEUELOOM_API_KEY=ql_... \
        celery -A demo.tasks worker -l info

or use ``python demo/run_local.py`` for a zero-infrastructure version.
"""

from __future__ import annotations

import os
import random
import time
from typing import Any

from celery import Celery

from queueloom.sdk.celery import instrument
from queueloom.sdk.transport import Transport

app = Celery(
    "queueloom_demo",
    broker=os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),
)
app.conf.update(
    task_default_queue="default",
    task_routes={"demo.tasks.generate_report": {"queue": "reports"}},
    broker_connection_retry_on_startup=True,
    worker_hijack_root_logger=False,
)


def configure(transport: Transport | None = None, **options: Any) -> Any:
    return instrument(
        app,
        transport=transport,
        environment=os.environ.get("QUEUELOOM_ENVIRONMENT", "demo"),
        capture_args=True,
        **options,
    )


if os.environ.get("QUEUELOOM_API_KEY") and os.environ.get("QUEUELOOM_ENDPOINT"):
    configure(flush_interval=0.5)


@app.task
def add(x: int, y: int) -> int:
    return x + y


@app.task
def send_email(address: str) -> str:
    time.sleep(random.uniform(0.01, 0.08))
    if address.endswith("@invalid.example"):
        raise ValueError(f"Undeliverable address: {address}")
    return f"sent to {address}"


@app.task
def generate_report(pages: int) -> dict[str, int]:
    time.sleep(0.02 * pages)
    return {"pages": pages}


@app.task(bind=True, max_retries=3)
def fetch_rates(self: Any, currency: str) -> float:
    time.sleep(random.uniform(0.01, 0.05))
    if random.random() < 0.4:
        raise self.retry(exc=TimeoutError(f"rates API timed out for {currency}"), countdown=0)
    return round(random.uniform(0.5, 1.5), 4)


@app.task
def sync_inventory(sku: str) -> None:
    raise RuntimeError(f"inventory service returned 500 for {sku}")


def enqueue_random() -> None:
    """Enqueue one randomly chosen demo task."""
    roll = random.random()
    if roll < 0.35:
        add.delay(random.randint(1, 100), random.randint(1, 100))
    elif roll < 0.6:
        domain = "invalid.example" if random.random() < 0.15 else "example.com"
        send_email.delay(f"user{random.randint(1, 500)}@{domain}")
    elif roll < 0.75:
        generate_report.delay(random.randint(1, 20))
    elif roll < 0.95:
        fetch_rates.delay(random.choice(["EUR", "GBP", "JPY", "PKR"]))
    else:
        sync_inventory.delay(f"SKU-{random.randint(1000, 9999)}")
