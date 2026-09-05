from __future__ import annotations

import re
from datetime import datetime, timedelta

from queueloom.events import ensure_aware, utcnow

_RANGE = re.compile(r"^(\d+)([mhd])$")
_UNITS = {"m": "minutes", "h": "hours", "d": "days"}

DEFAULT_RANGE = "24h"


def parse_range(value: str | None) -> timedelta:
    """Parse a compact range such as ``15m``, ``24h`` or ``7d``."""
    match = _RANGE.match((value or DEFAULT_RANGE).strip())
    if not match:
        raise ValueError(f"invalid range {value!r}; use e.g. 15m, 24h, 7d")
    amount, unit = int(match.group(1)), match.group(2)
    return timedelta(**{_UNITS[unit]: amount})


def resolve_window(
    since: datetime | None, until: datetime | None, range_: str | None
) -> tuple[datetime, datetime]:
    until_dt = ensure_aware(until) if until else utcnow()
    since_dt = ensure_aware(since) if since else until_dt - parse_range(range_)
    return since_dt, until_dt
