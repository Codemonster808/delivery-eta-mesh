#!/usr/bin/env python3
"""
Nightly Spark replay: reconciles the full day, applies a watermark for
late-arriving GPS pings (arrival delay > 10 min from event_ts is treated
as a correction, not a live update), and salts the restaurant_id key
before aggregation to avoid the ~5 restaurants that get ~60% of orders
overloading a single partition.

Honesty note on measuring the skew fix: on a single-machine local[2]
Spark session, wall-clock speedup from salting is not a reliable signal
(there's no real cluster for one overloaded partition to bottleneck).
What IS reliably measurable locally is partition balance — the actual
mechanism salting fixes — so that's what this script's main() prints,
not a timing number that would be noise at this scale.
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

WATERMARK_MINUTES = 10
SALT_BUCKETS = 8


def build_spark(app_name: str = "eta-replay") -> SparkSession:
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.5.0")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def apply_watermark(gps_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Splits GPS pings into on-time (live-updatable) vs late (correction-only)."""
    with_delay = gps_df.withColumn(
        "arrival_delay_min",
        (
            F.col("ts").cast("timestamp").cast("long")
            - F.col("event_ts").cast("timestamp").cast("long")
        )
        / 60,
    )
    on_time = with_delay.filter(F.col("arrival_delay_min") <= WATERMARK_MINUTES)
    late = with_delay.filter(F.col("arrival_delay_min") > WATERMARK_MINUTES)
    return on_time, late


def naive_order_counts(orders_df: DataFrame) -> DataFrame:
    """No salting — groupBy directly on the skewed key."""
    return orders_df.groupBy("restaurant_id").agg(F.count("*").alias("n_orders"))


def salted_order_counts(orders_df: DataFrame, n_buckets: int = SALT_BUCKETS) -> DataFrame:
    """Splits each restaurant's rows across n_buckets salted sub-keys for the
    first aggregation pass, then re-aggregates — the standard fix for a
    groupBy key where a handful of values dominate the row count."""
    salted = orders_df.withColumn("_salt", (F.rand() * n_buckets).cast("int"))
    partial = salted.groupBy("restaurant_id", "_salt").agg(F.count("*").alias("partial_count"))
    return partial.groupBy("restaurant_id").agg(F.sum("partial_count").alias("n_orders"))


def partition_balance(df: DataFrame, key_col: str) -> dict:
    """Row count per Spark partition after a groupBy/repartition on key_col —
    this is the thing salting actually fixes, and it's measurable locally
    without needing a real multi-node cluster."""
    counts = df.rdd.mapPartitionsWithIndex(lambda idx, it: [(idx, sum(1 for _ in it))]).collect()
    sizes = [c for _, c in counts if c > 0]
    return {
        "n_partitions_with_data": len(sizes),
        "max_partition_rows": max(sizes) if sizes else 0,
        "min_partition_rows": min(sizes) if sizes else 0,
        "imbalance_ratio": round(max(sizes) / max(1, min(sizes)), 2) if sizes else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="in_path", default="data/events.jsonl")
    parser.add_argument("--dst", default="s3a://dispatch-agg/order_counts/")
    args = parser.parse_args()

    spark = build_spark()
    try:
        events = spark.read.json(args.in_path)
        orders = events.filter(F.col("event_type") == "order_placed")
        gps = events.filter(F.col("event_type") == "courier_gps")

        on_time_gps, late_gps = apply_watermark(gps)
        n_on_time, n_late = on_time_gps.count(), late_gps.count()

        # Apples-to-apples: both measurements are on raw order ROWS shuffled
        # into SALT_BUCKETS partitions — this is what the first shuffle stage
        # of a groupBy actually does. Naive hash-partitions by restaurant_id
        # alone, so every row for a hot restaurant lands on the same
        # partition. Salted hash-partitions by (restaurant_id, salt), which
        # is exactly what salted_order_counts()'s first aggregation stage
        # does before re-aggregating — splitting a hot restaurant's rows
        # across SALT_BUCKETS partitions instead of concentrating them on one.
        naive_balance = partition_balance(
            orders.repartition(SALT_BUCKETS, "restaurant_id"), "restaurant_id"
        )
        salted_balance = partition_balance(
            orders.withColumn("_salt", (F.rand() * SALT_BUCKETS).cast("int")).repartition(
                SALT_BUCKETS, "restaurant_id", "_salt"
            ),
            "restaurant_id",
        )

        salted_counts = salted_order_counts(orders)
        final_counts = naive_order_counts(orders)

        # Salting is a performance fix for the partition-imbalance measured
        # above, not a change in what gets counted — per-restaurant totals
        # from the naive and salted paths must be identical. This is the
        # "naive vs salted totals must match" invariant CLAUDE.md documents
        # as core to this repo; a mismatch here means salted_order_counts()
        # is silently dropping or double-counting rows, which is worse than
        # the skew it was meant to fix.
        mismatches = (
            final_counts.withColumnRenamed("n_orders", "naive_n_orders")
            .join(
                salted_counts.withColumnRenamed("n_orders", "salted_n_orders"),
                on="restaurant_id",
                how="full_outer",
            )
            .filter(
                F.col("naive_n_orders").isNull()
                | F.col("salted_n_orders").isNull()
                | (F.col("naive_n_orders") != F.col("salted_n_orders"))
            )
            .collect()
        )
        if mismatches:
            raise RuntimeError(
                f"naive vs salted order counts disagree for {len(mismatches)} "
                f"restaurant(s): {mismatches[:5]} (showing up to 5)"
            )

        (final_counts.write.mode("overwrite").parquet(args.dst))

        print(
            f"orders: {orders.count()}, on-time GPS: {n_on_time}, late GPS (watermarked): {n_late}"
        )
        print(f"naive partition balance (by restaurant_id): {naive_balance}")
        print(f"salted-then-reaggregated partition balance: {salted_balance}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
