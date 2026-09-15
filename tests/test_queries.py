from __future__ import annotations

import pytest

from queueloom.server.queries import percentile


@pytest.mark.parametrize(
    ("values", "pct", "expected"),
    [
        ([], 95, None),
        ([42.0], 95, 42.0),
        ([200.0, 100.0], 50, 100.0),
        ([3.0, 1.0, 2.0], 50, 2.0),
        ([float(n) for n in range(1, 21)], 95, 19.0),
    ],
)
def test_nearest_rank_percentiles(values: list[float], pct: float, expected: float | None) -> None:
    assert percentile(values, pct) == expected
