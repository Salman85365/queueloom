from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from queueloom.ai import AnthropicProvider, Summary, SummaryError, TemplateProvider, get_provider
from queueloom.ai.providers import SYSTEM_PROMPT
from queueloom.events import EventType
from queueloom.server.db import session_scope
from queueloom.server.diagnosis import diagnose, normalise_message, render_report
from queueloom.server.ingest import ingest_events
from queueloom.server.projects import get_project_by_name
from queueloom.server.queries import RunFilters
from tests.conftest import ProjectInfo, at, dump, lifecycle, make_event


def test_normalise_message_collapses_variable_parts() -> None:
    raw = (
        "Undeliverable address: user347@invalid.example "
        "(job 3fa85f64-5717-4562-b3fc-2c963f66afa6, try 3)"
    )
    assert normalise_message(raw) == "Undeliverable address: <email> (job <uuid>, try #)"
    assert normalise_message(None) == ""
    assert normalise_message("   spaced    out  ") == "spaced out"


def _seed(session_factory: sessionmaker[Session], project: ProjectInfo, events: list[Any]) -> None:
    with session_scope(session_factory) as session:
        p = get_project_by_name(session, project.name)
        assert p is not None
        ingest_events(session, p, events)


def test_diagnose_finds_regressions_clusters_and_stuck_tasks(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    events: list[Any] = []
    # Baseline window [-3600, 0): send_email healthy, report fast.
    for i in range(10):
        events += lifecycle(f"b-mail-{i}", offset=-3500 + i, task_name="app.send_email")
        events += lifecycle(f"b-rep-{i}", offset=-3400 + i, task_name="app.report")
    # Current window [0, 3600): send_email failing 40%, report 3x slower, inventory new failures.
    for i in range(6):
        events += lifecycle(f"c-mail-ok-{i}", offset=10 + i, task_name="app.send_email")
    for i in range(4):
        events += lifecycle(
            f"c-mail-bad-{i}", offset=100 + i, task_name="app.send_email", outcome=EventType.FAILED
        )
        # make the exception messages differ only in variable parts
        events[-1].exception.message = f"Undeliverable address: user{i}@invalid.example"  # type: ignore[union-attr]
    for i in range(5):
        seq = lifecycle(f"c-rep-{i}", offset=200 + i, task_name="app.report")
        seq[-1].runtime_ms = 3000.0
        events += seq
    events += lifecycle("c-inv", offset=300, task_name="app.inventory", outcome=EventType.FAILED)
    # Stuck: published 10 minutes before window end, never started.
    events.append(
        make_event(
            EventType.PUBLISHED,
            task_id="stuck-1",
            task_name="app.report",
            timestamp=at(3600 - 600),
            published_at=at(3600 - 600),
        )
    )
    # Running for 45 minutes.
    events.append(
        make_event(
            EventType.STARTED, task_id="long-1", task_name="app.report", timestamp=at(3600 - 2700)
        )
    )
    _seed(session_factory, project, events)

    with session_scope(session_factory) as session:
        d = diagnose(session, project.id, RunFilters(since=at(0), until=at(3600)))

    assert d.total_runs == 18  # 6+4 mail, 5 report, 1 inventory, stuck, long
    assert d.failed_runs == 5
    assert d.baseline_total_runs == 20
    assert d.baseline_failure_rate == 0.0
    assert d.stuck_queued == 1 and d.stuck_running == 1

    findings = {f.task_name: f for f in d.task_findings}
    assert "failure_rate_up" in findings["app.send_email"].flags
    assert findings["app.send_email"].baseline_failure_rate == 0.0
    assert "slower" in findings["app.report"].flags
    assert findings["app.report"].baseline_p95_duration_ms == 1000.0
    assert "new_failures" in findings["app.inventory"].flags
    assert d.task_findings[0].task_name == "app.send_email"  # most failures first

    top = d.error_clusters[0]
    assert (top.task_name, top.exception_type, top.count) == ("app.send_email", "ValueError", 4)
    assert top.message_pattern == "Undeliverable address: <email>"
    assert len(top.sample_task_ids) == 4 and top.workers == ["w1"]
    assert "5 of 18 runs failed" in d.headline and "versus 0.0%" in d.headline
    assert "queued for over 5 minutes" in d.headline

    report = render_report(d, project.name)
    assert "app.send_email: 4/10 failed (40.0%, baseline 0.0%)" in report
    assert "4x app.send_email -> ValueError: Undeliverable address: <email>" in report
    assert "Sample traceback" in report
    assert "Stuck: 1 queued > 5 min, 1 running > 30 min." in report


def test_diagnose_quiet_window(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    _seed(session_factory, project, lifecycle("ok", offset=10))
    with session_scope(session_factory) as session:
        d = diagnose(session, project.id, RunFilters(since=at(0), until=at(3600)))
    assert d.task_findings == [] and d.error_clusters == []
    assert "Nothing unusual" in render_report(d, project.name)
    with session_scope(session_factory) as session:
        empty = diagnose(session, project.id, RunFilters(since=at(-9000), until=at(-8000)))
    assert empty.headline.startswith("No task runs")


# -- providers ---------------------------------------------------------------------------------


def test_template_provider_and_auto_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    settings = SimpleNamespace(ai_provider="auto", anthropic_api_key=None)
    provider = get_provider(settings)
    assert isinstance(provider, TemplateProvider)
    assert provider.summarize("report text").text == "report text"

    settings = SimpleNamespace(
        ai_provider="auto",
        anthropic_api_key="sk-ant-test",
        ai_model="claude-opus-5",
        ai_max_tokens=500,
        ai_effort="low",
        ai_fallbacks=False,
    )
    provider = get_provider(settings)
    assert isinstance(provider, AnthropicProvider)
    assert provider.model == "claude-opus-5" and provider.effort == "low"
    with pytest.raises(SummaryError):
        get_provider(SimpleNamespace(ai_provider="openai"))


class FakeMessages:
    def __init__(self, response: Any) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def _fake_client(response: Any) -> Any:
    messages = FakeMessages(response)
    return SimpleNamespace(messages=messages, beta=SimpleNamespace(messages=messages))


def _response(text: str, stop_reason: str = "end_turn") -> Any:
    return SimpleNamespace(
        content=[
            SimpleNamespace(type="thinking", thinking=""),
            SimpleNamespace(type="text", text=text),
        ],
        stop_reason=stop_reason,
        stop_details=None,
        model="claude-opus-5",
        usage=SimpleNamespace(input_tokens=123, output_tokens=45),
    )


def test_anthropic_provider_request_shape_with_fallbacks() -> None:
    client = _fake_client(_response("  It is the mail server.  "))
    provider = AnthropicProvider(client=client, effort="medium")
    summary = provider.summarize("REPORT", question="Why?")
    assert summary == Summary("It is the mail server.", "anthropic", "claude-opus-5", 123, 45)
    (call,) = client.beta.messages.calls
    assert call["model"] == "claude-opus-5"
    assert call["betas"] == ["server-side-fallback-2026-07-01"] and call["fallbacks"] == "default"
    assert call["output_config"] == {"effort": "medium"}
    assert call["system"][0]["text"] == SYSTEM_PROMPT
    assert call["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert "thinking" not in call  # adaptive by default on Opus 5
    user = call["messages"][0]["content"]
    assert "<diagnosis_report>\nREPORT\n</diagnosis_report>" in user and "Why?" in user


def test_anthropic_provider_without_fallbacks_uses_stable_endpoint() -> None:
    client = _fake_client(_response("ok"))
    AnthropicProvider(client=client, fallbacks=False).summarize("r")
    (call,) = client.messages.calls
    assert "betas" not in call and "fallbacks" not in call


def test_anthropic_provider_refusal_and_errors() -> None:
    refused = _response("", stop_reason="refusal")
    refused.stop_details = SimpleNamespace(category="cyber")
    with pytest.raises(SummaryError, match=r"declined.*cyber"):
        AnthropicProvider(client=_fake_client(refused)).summarize("r")
    with pytest.raises(SummaryError, match="empty"):
        AnthropicProvider(client=_fake_client(_response("   "))).summarize("r")

    import anthropic
    import httpx2

    request = httpx2.Request("POST", "https://api.anthropic.com/v1/messages")
    auth_error = anthropic.AuthenticationError(
        "bad key", response=httpx2.Response(401, request=request), body=None
    )
    with pytest.raises(SummaryError, match="API key"):
        AnthropicProvider(client=_fake_client(auth_error)).summarize("r")
    conn_error = anthropic.APIConnectionError(request=request)
    with pytest.raises(SummaryError, match="network"):
        AnthropicProvider(client=_fake_client(conn_error)).summarize("r")


# -- API + dashboard ---------------------------------------------------------------------------


class RecordingProvider:
    name = "fake"

    def __init__(self) -> None:
        self.reports: list[str] = []
        self.fail = False

    def summarize(self, report: str, *, question: str | None = None) -> Summary:
        if self.fail:
            raise SummaryError("provider exploded")
        self.reports.append(report)
        return Summary(text=f"SUMMARY({question or '-'})", provider=self.name, model="fake-1")


def test_diagnosis_endpoints(app: FastAPI, client: TestClient, project: ProjectInfo) -> None:
    provider = RecordingProvider()
    app.state.summary_provider = provider
    events: list[Any] = lifecycle("x", outcome=EventType.FAILED, task_name="app.send_email")
    client.post("/v1/events", json=dump(events), headers=project.headers)
    since, until = at(-10).isoformat(), at(3600).isoformat()

    data = client.get(
        "/v1/diagnosis", params={"since": since, "until": until}, headers=project.headers
    ).json()
    assert data["failed_runs"] == 1
    assert data["error_clusters"][0]["exception_type"] == "ValueError"
    assert "1 of 1 runs failed" in data["report"]

    result = client.post(
        "/v1/diagnosis/summary",
        params={"since": since, "until": until},
        json={"question": "root cause?"},
        headers=project.headers,
    )
    assert result.status_code == 200, result.text
    assert result.json()["summary"]["text"] == "SUMMARY(root cause?)"
    assert provider.reports[0] == result.json()["report"]

    provider.fail = True
    failed = client.post("/v1/diagnosis/summary", params={"since": since}, headers=project.headers)
    assert failed.status_code == 502 and "provider exploded" in failed.json()["detail"]

    page = client.get(f"/projects/{project.name}/diagnose", params={"range": "30d"})
    assert page.status_code == 200
    assert "provider: fake" in page.text and "app.send_email" in page.text
    assert "Generate summary" in page.text

    provider.fail = False
    import re

    token = re.search(r'name="csrf" value="([^"]+)"', page.text).group(1)  # type: ignore[union-attr]
    generated = client.post(
        f"/projects/{project.name}/diagnose",
        params={"range": "30d"},
        data={"csrf": token, "question": "why"},
    )
    assert generated.status_code == 200 and "SUMMARY(why)" in generated.text

    provider.fail = True
    errored = client.post(
        f"/projects/{project.name}/diagnose", params={"range": "30d"}, data={"csrf": token}
    )
    assert errored.status_code == 200 and "provider exploded" in errored.text


def test_diagnose_page_template_mode(
    client: TestClient, project: ProjectInfo, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    page = client.get(f"/projects/{project.name}/diagnose")
    assert page.status_code == 200
    assert "provider: template" in page.text and "Show report" in page.text


def test_stuck_thresholds_do_not_flag_fresh_tasks(
    session_factory: sessionmaker[Session], project: ProjectInfo
) -> None:
    _seed(
        session_factory,
        project,
        [
            make_event(
                EventType.PUBLISHED,
                task_id="fresh",
                timestamp=at(3600) - timedelta(minutes=1),
                published_at=at(3600) - timedelta(minutes=1),
            )
        ],
    )
    with session_scope(session_factory) as session:
        d = diagnose(session, project.id, RunFilters(since=at(0), until=at(3600)))
    assert d.stuck_queued == 0
