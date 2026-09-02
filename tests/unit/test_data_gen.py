"""Invariants of the synthetic dispatch generator.

docs/LEARNING_BUILD.md and the data_gen module docstring: ~12% of GPS
pings are late, and the hottest 5% of restaurants get ~60% of orders.
Both are deterministic given --seed.
"""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from ingestion.data_gen import (  # noqa: E402
    HOT_RESTAURANT_WEIGHT,
    LATE_PING_FRACTION,
    generate_dispatch_day,
)


def test_late_ping_fraction_and_hot_key_share_are_seed_deterministic(tmp_path):
    out = tmp_path / "events.jsonl"
    kwargs = dict(hours=4, restaurants=40, orders_per_hour=50, out=out, seed=42)
    first = generate_dispatch_day(**kwargs)
    second = generate_dispatch_day(**kwargs)

    assert first["late_fraction"] == pytest.approx(LATE_PING_FRACTION, abs=0.05)
    assert first["top_share"] == pytest.approx(HOT_RESTAURANT_WEIGHT, abs=0.08)
    assert first["n_late"] == second["n_late"]
    assert first["top_share"] == second["top_share"]

    lines = out.read_text().splitlines()
    events = [json.loads(line) for line in lines]
    gps = [e for e in events if e["event_type"] == "courier_gps"]
    late = [e for e in gps if e["is_late"]]
    assert len(late) == first["n_late"]
    for ping in late:
        assert (
            ping["ts"] > ping["event_ts"]
        ), "late pings keep a truthful event_ts and a later pipeline-visible ts"
