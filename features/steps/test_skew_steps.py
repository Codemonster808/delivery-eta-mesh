import sys
from pathlib import Path

import pytest
from pytest_bdd import given, scenarios, then, when

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "transformation"))

from pyspark.sql import functions as F  # noqa: E402
from replay import (  # noqa: E402
    SALT_BUCKETS,
    build_spark,
    naive_order_counts,
    partition_balance,
    salted_order_counts,
)

scenarios("../skew.feature")


@pytest.fixture(scope="module")
def spark():
    s = build_spark("bdd-skew")
    yield s
    s.stop()


@given(
    "900 rows for one hot restaurant_id and 100 rows spread across cold restaurant_ids",
    target_fixture="skewed_df",
)
def skewed_df(spark):
    # 90% of rows belong to one hot key — a deliberately extreme skew.
    hot_rows = [{"restaurant_id": "hot", "x": i} for i in range(900)]
    cold_rows = [{"restaurant_id": f"cold_{i}", "x": i} for i in range(100)]
    return spark.createDataFrame(hot_rows + cold_rows)


@when(
    "the rows are repartitioned naively by restaurant_id and again with a salted key",
    target_fixture="balances",
)
def balances(skewed_df):
    naive = partition_balance(skewed_df.repartition(SALT_BUCKETS, "restaurant_id"), "restaurant_id")
    salted = partition_balance(
        skewed_df.withColumn("_salt", (F.rand() * SALT_BUCKETS).cast("int")).repartition(
            SALT_BUCKETS, "restaurant_id", "_salt"
        ),
        "restaurant_id",
    )
    naive_total = naive_order_counts(skewed_df).agg(F.sum("n_orders")).collect()[0][0]
    salted_total = salted_order_counts(skewed_df).agg(F.sum("n_orders")).collect()[0][0]
    return {
        "naive": naive,
        "salted": salted,
        "naive_total": naive_total,
        "salted_total": salted_total,
    }


@then("the salted partition imbalance ratio is lower than the naive partition imbalance ratio")
def imbalance_lower(balances):
    naive, salted = balances["naive"], balances["salted"]
    assert naive["imbalance_ratio"] > salted["imbalance_ratio"], (
        f"salting should reduce imbalance: naive={naive['imbalance_ratio']}, "
        f"salted={salted['imbalance_ratio']}"
    )


@then("the total row count matches between the naive and salted approaches")
def totals_match(balances):
    assert balances["naive_total"] == balances["salted_total"] == 1000
