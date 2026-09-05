from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from queueloom.events import SCHEMA_VERSION, EventBatch, EventType, TaskEvent, truncate


def test_defaults_are_filled() -> None:
    event = TaskEvent(event_type=EventType.STARTED, task_id="abc")
    assert event.schema_version == SCHEMA_VERSION
    assert len(event.event_id) == 32
    assert event.timestamp.tzinfo is not None
    assert event.environment == "default"


def test_naive_datetimes_are_treated_as_utc() -> None:
    event = TaskEvent(
        event_type=EventType.STARTED,
        task_id="abc",
        timestamp=datetime(2026, 1, 1, 10, 0, 0),
        published_at=datetime(2026, 1, 1, 9, 59, 0),
    )
    assert event.timestamp.tzinfo is UTC
    assert event.published_at is not None and event.published_at.tzinfo is UTC


def test_unknown_schema_version_is_rejected() -> None:
    with pytest.raises(ValidationError):
        TaskEvent.model_validate(
            {"schema_version": 99, "event_type": "task.started", "task_id": "abc"}
        )


def test_unknown_fields_are_ignored_for_forward_compatibility() -> None:
    event = TaskEvent.model_validate(
        {"event_type": "task.started", "task_id": "abc", "future_field": 1}
    )
    assert not hasattr(event, "future_field")


def test_json_round_trip() -> None:
    event = TaskEvent(
        event_type=EventType.FAILED,
        task_id="abc",
        exception={"type": "ValueError", "message": "boom"},  # type: ignore[arg-type]
    )
    restored = TaskEvent.model_validate_json(event.model_dump_json())
    assert restored == event


def test_batch_requires_at_least_one_event() -> None:
    with pytest.raises(ValidationError):
        EventBatch(events=[])


def test_truncate_marks_cut_text() -> None:
    assert truncate("short", 100) == "short"
    assert truncate(None, 10) is None
    cut = truncate("x" * 500, 50)
    assert cut is not None and len(cut) == 50 and cut.endswith("…[truncated]")
