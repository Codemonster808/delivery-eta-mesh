import sys
from pathlib import Path

import pytest
from pytest_bdd import given, scenarios, then, when

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "transformation"))

from replay import apply_watermark, build_spark  # noqa: E402

scenarios("../late-events.feature")


@pytest.fixture(scope="module")
def spark():
    s = build_spark("bdd-watermark")
    yield s
    s.stop()


@given(
    "GPS pings arriving 2 minutes and 20 minutes after event_ts",
    target_fixture="gps_df",
)
def gps_df(spark):
    rows = [
        {"order_id": "a", "event_ts": "2026-01-01T00:00:00", "ts": "2026-01-01T00:02:00"},
        {"order_id": "b", "event_ts": "2026-01-01T00:00:00", "ts": "2026-01-01T00:20:00"},
    ]
    return spark.createDataFrame(rows)


@when("the 10 minute watermark is applied", target_fixture="split")
def apply(gps_df):
    return apply_watermark(gps_df)


@then("exactly one ping is on-time and it is order a")
def on_time_a(split):
    on_time, _late = split
    assert on_time.count() == 1
    assert on_time.collect()[0]["order_id"] == "a"


@then("exactly one ping is late and it is order b")
def late_b(split):
    _on_time, late = split
    assert late.count() == 1
    assert late.collect()[0]["order_id"] == "b"
