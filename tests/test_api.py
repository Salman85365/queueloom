from __future__ import annotations

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from queueloom.events import EventType, TaskEvent
from tests.conftest import ProjectInfo, at, dump, lifecycle, make_event, make_project


def test_health(client: TestClient) -> None:
    body = client.get("/v1/health").json()
    assert body["status"] == "ok"
    assert body["schema_version"] == 1


def test_ingest_requires_api_key(client: TestClient, project: ProjectInfo) -> None:
    payload = dump(lifecycle("t1"))
    assert client.post("/v1/events", json=payload).status_code == 401
    bad = {"Authorization": "Bearer ql_nope"}
    assert client.post("/v1/events", json=payload, headers=bad).status_code == 401


def test_ingest_and_read_back(client: TestClient, project: ProjectInfo) -> None:
    response = client.post("/v1/events", json=dump(lifecycle("t1")), headers=project.headers)
    assert response.status_code == 202, response.text
    assert response.json() == {"accepted": 3, "duplicates": 0}

    listing = client.get(
        "/v1/tasks", params={"since": at(-10).isoformat()}, headers=project.headers
    ).json()
    assert listing["total"] == 1
    item = listing["items"][0]
    assert item["task_id"] == "t1"
    assert item["state"] == "succeeded"
    assert item["queue_latency_ms"] == 250.0

    detail = client.get("/v1/tasks/t1", headers=project.headers).json()
    assert [e["event_type"] for e in detail["events"]] == [
        "task.published",
        "task.started",
        "task.succeeded",
    ]
    assert client.get("/v1/tasks/missing", headers=project.headers).status_code == 404


def test_ingest_rejects_unknown_schema_version(client: TestClient, project: ProjectInfo) -> None:
    payload = dump(lifecycle("t1"))
    payload["events"][0]["schema_version"] = 2
    response = client.post("/v1/events", json=payload, headers=project.headers)
    assert response.status_code == 422


def test_filters(client: TestClient, project: ProjectInfo) -> None:
    events = (
        lifecycle("a", task_name="app.send_email", environment="prod")
        + lifecycle(
            "b", task_name="app.send_email", environment="staging", outcome=EventType.FAILED
        )
        + lifecycle("c", task_name="app.report", environment="prod", queue="reports")
    )
    client.post("/v1/events", json=dump(events), headers=project.headers)
    since = at(-10).isoformat()

    def ids(**params: str | None) -> set[str]:
        params.setdefault("since", since)
        query = {k: v for k, v in params.items() if v is not None}
        body = client.get("/v1/tasks", params=query, headers=project.headers).json()
        return {i["task_id"] for i in body["items"]}

    assert ids() == {"a", "b", "c"}
    assert ids(environment="prod") == {"a", "c"}
    assert ids(task_name="app.send_email") == {"a", "b"}
    assert ids(state="failed") == {"b"}
    assert ids(queue="reports") == {"c"}
    # Without an explicit `since`, the window is relative to now and excludes 2026-09-01 data.
    assert ids(range="1h", since=None) == set()
    assert (
        client.get("/v1/tasks", params={"range": "bogus"}, headers=project.headers).status_code
        == 422
    )


def test_stats(client: TestClient, project: ProjectInfo) -> None:
    events: list = []
    for i in range(8):
        events += lifecycle(f"ok{i}", offset=i * 10)
    for i in range(2):
        events += lifecycle(
            f"bad{i}", offset=100 + i * 10, outcome=EventType.FAILED, task_name="app.tasks.flaky"
        )
    events += [make_event(EventType.PUBLISHED, task_id="pending", timestamp=at(200))]
    client.post("/v1/events", json=dump(events), headers=project.headers)

    stats = client.get(
        "/v1/stats", params={"since": at(-1).isoformat()}, headers=project.headers
    ).json()
    assert stats["total"] == 11
    assert stats["by_state"]["succeeded"] == 8
    assert stats["by_state"]["failed"] == 2
    assert stats["by_state"]["queued"] == 1
    assert stats["failure_rate"] == 0.2
    assert stats["p95_duration_ms"] == 1000.0
    assert stats["avg_queue_latency_ms"] == 250.0
    by_task = {t["task_name"]: t for t in stats["by_task"]}
    assert by_task["app.tasks.flaky"]["failed"] == 2
    assert by_task["app.tasks.flaky"]["failure_rate"] == 1.0
    assert by_task["app.tasks.add"]["failure_rate"] == 0.0
    assert stats["by_task"][0]["task_name"] == "app.tasks.flaky"  # sorted by failures first


def test_projects_are_isolated(client: TestClient, session_factory: sessionmaker[Session]) -> None:
    p1 = make_project(session_factory, "one")
    p2 = make_project(session_factory, "two")
    client.post("/v1/events", json=dump(lifecycle("shared-id")), headers=p1.headers)
    since = at(-10).isoformat()
    assert client.get("/v1/tasks", params={"since": since}, headers=p1.headers).json()["total"] == 1
    assert client.get("/v1/tasks", params={"since": since}, headers=p2.headers).json()["total"] == 0
    assert client.get("/v1/tasks/shared-id", headers=p2.headers).status_code == 404
    assert client.get("/v1/projects/me", headers=p2.headers).json()["name"] == "two"


def test_stats_percentiles_with_varied_timings(client: TestClient, project: ProjectInfo) -> None:
    events: list[TaskEvent] = []
    for n in range(1, 21):
        common = {"task_id": f"timing-{n}", "task_name": "app.report"}
        events.extend(
            [
                make_event(
                    EventType.PUBLISHED, timestamp=at(n * 2), published_at=at(n * 2), **common
                ),
                make_event(EventType.STARTED, timestamp=at(n * 2 + n / 1000), **common),
                make_event(
                    EventType.SUCCEEDED,
                    timestamp=at(n * 2 + 1),
                    runtime_ms=n * 100.0,
                    **common,
                ),
            ]
        )
    response = client.post("/v1/events", json=dump(events), headers=project.headers)
    assert response.status_code == 202
    stats = client.get(
        "/v1/stats", params={"since": at(-1).isoformat()}, headers=project.headers
    ).json()
    assert stats["total"] == 20
    for summary in (stats, stats["by_task"][0]):
        assert summary["p50_duration_ms"] == 1000.0
        assert summary["p95_duration_ms"] == 1900.0
        assert summary["p95_queue_latency_ms"] == 19.0


def test_dashboard_pages_render(client: TestClient, project: ProjectInfo) -> None:
    assert "No projects yet" not in client.get("/").text
    client.post(
        "/v1/events",
        json=dump(lifecycle("t1", outcome=EventType.FAILED)),
        headers=project.headers,
    )
    overview = client.get(f"/projects/{project.name}", params={"range": "30d"})
    assert overview.status_code == 200
    assert overview.text.rstrip().endswith("</html>")  # template block markers must not leak
    assert 'http-equiv="refresh"' not in overview.text  # refresh disabled in tests (0s)
    tasks = client.get(
        f"/projects/{project.name}/tasks", params={"range": "30d", "state": "failed"}
    )
    assert tasks.status_code == 200
    detail = client.get(f"/projects/{project.name}/tasks/t1")
    assert detail.status_code == 200
    assert "ValueError" in detail.text
    assert "Traceback..." in detail.text
    assert client.get("/projects/nope").status_code == 404
    assert client.get(f"/projects/{project.name}/tasks/nope").status_code == 404
    # Garbage filters degrade gracefully instead of erroring.
    assert (
        client.get(
            f"/projects/{project.name}/tasks", params={"range": "zzz", "page": "x"}
        ).status_code
        == 200
    )


def test_dashboard_empty_state(client: TestClient) -> None:
    assert "No projects yet" in client.get("/").text
