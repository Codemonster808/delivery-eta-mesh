import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from replay import SALT_BUCKETS, apply_watermark, build_spark, partition_balance  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = build_spark("test-replay")
    yield s
    s.stop()


def test_watermark_splits_late_from_on_time(spark):
    rows = [
        {"order_id": "a", "event_ts": "2026-01-01T00:00:00", "ts": "2026-01-01T00:02:00"},  # 2 min: on-time
        {"order_id": "b", "event_ts": "2026-01-01T00:00:00", "ts": "2026-01-01T00:20:00"},  # 20 min: late
    ]
    df = spark.createDataFrame(rows)
    on_time, late = apply_watermark(df)
    assert on_time.count() == 1
    assert late.count() == 1
    assert on_time.collect()[0]["order_id"] == "a"
    assert late.collect()[0]["order_id"] == "b"


def test_salting_reduces_partition_imbalance(spark):
    # 90% of rows belong to one hot key — a deliberately extreme skew.
    hot_rows = [{"restaurant_id": "hot", "x": i} for i in range(900)]
    cold_rows = [{"restaurant_id": f"cold_{i}", "x": i} for i in range(100)]
    df = spark.createDataFrame(hot_rows + cold_rows)

    naive = partition_balance(df.repartition(SALT_BUCKETS, "restaurant_id"), "restaurant_id")
    salted = partition_balance(
        df.withColumn("_salt", (F.rand() * SALT_BUCKETS).cast("int")).repartition(
            SALT_BUCKETS, "restaurant_id", "_salt"
        ),
        "restaurant_id",
    )

    assert naive["imbalance_ratio"] > salted["imbalance_ratio"], (
        f"salting should reduce imbalance: naive={naive['imbalance_ratio']}, salted={salted['imbalance_ratio']}"
    )
