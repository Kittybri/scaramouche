import asyncio
import math

import pytest

from environment_state import EnvironmentMonitor, _latency_ms


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (math.inf, None),
        (-math.inf, None),
        (math.nan, None),
        ("unavailable", None),
        (-0.25, 0),
        (0.1239, 123),
    ],
)
def test_latency_ms_handles_disconnect_sentinels(value, expected):
    assert _latency_ms(value) == expected


def test_collect_accepts_infinite_gateway_latency(monkeypatch):
    monitor = EnvironmentMonitor()
    monkeypatch.setattr(monitor, "_system_pressure", lambda: ("normal", "unknown", "normal"))

    snapshot = asyncio.run(monitor.collect(discord_latency=math.inf))

    assert snapshot.discord_latency_ms is None
