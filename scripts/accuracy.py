#!/usr/bin/env python3
"""
Computes ETA MAE: the worker's predicted eta_minutes (DynamoDB
eta-current) against each order's synthetic ground-truth
actual_delivery_minutes (embedded in data_gen.py's output) — the metric
the README promised and previously never actually computed.

Also computes and (optionally) persists a real "ETA accuracy by
zone/hour" breakdown — the aggregate the README's architecture diagram
claims lands in the warehouse. Orders don't carry a "zone" field, so a
zone is derived by bucketing the order's courier_gps lat/lon into a
coarse 3x3 grid over the synthetic delivery area (see
src/ingestion/data_gen.py's LAT_MIN/LAT_MAX/LON_MIN/LON_MAX). This
mirrors src/transformation/replay.py's order_counts pattern: written as
Parquet to s3://dispatch-agg/ and queryable through the same DuckDB
"Redshift stand-in" (src/utils/warehouse.py) via `make query`.
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from utils import aws  # noqa: E402

# Matches the lat/lon range data_gen.py generates courier_gps pings in.
LAT_MIN, LAT_MAX = -23.7, -23.4
LON_MIN, LON_MAX = -46.8, -46.5
N_ZONE_LAT_BUCKETS = 3
N_ZONE_LON_BUCKETS = 3


def zone_for(lat: float, lon: float) -> str:
    """Buckets a lat/lon into one of a 3x3 grid of zones over the
    synthetic delivery area. Deterministic and cheap — no external
    geocoding, since the data is synthetic anyway."""
    lat_frac = min(0.999, max(0.0, (lat - LAT_MIN) / (LAT_MAX - LAT_MIN)))
    lon_frac = min(0.999, max(0.0, (lon - LON_MIN) / (LON_MAX - LON_MIN)))
    lat_bucket = int(lat_frac * N_ZONE_LAT_BUCKETS)
    lon_bucket = int(lon_frac * N_ZONE_LON_BUCKETS)
    return f"zone_{lat_bucket}_{lon_bucket}"


def _mae_by_group(ground_truth: dict, zones: dict, predicted: dict) -> list:
    """Pure grouping/MAE logic, kept separate from file/DynamoDB I/O so it's
    unit-testable without infra. ground_truth: order_id -> (actual_minutes,
    hour). zones: order_id -> zone string. predicted: order_id -> eta_minutes."""
    groups: dict = {}
    for order_id, (actual, hour) in ground_truth.items():
        if order_id not in predicted or order_id not in zones:
            continue
        error = abs(predicted[order_id] - actual)
        groups.setdefault((zones[order_id], hour), []).append(error)

    rows = []
    for (zone, hour), errors in sorted(groups.items()):
        rows.append(
            {
                "zone": zone,
                "hour": hour,
                "n_orders": len(errors),
                "mae_minutes": round(sum(errors) / len(errors), 2),
            }
        )
    return rows


def compute_accuracy_by_zone_hour(events_path: str) -> list:
    """ETA MAE grouped by (zone, hour-of-day), read from the same events
    file and eta-current table compute_mae() uses."""
    ground_truth = {}
    zones = {}
    with open(events_path) as f:
        for line in f:
            event = json.loads(line)
            if event.get("event_type") == "order_placed":
                hour = datetime.fromisoformat(event["event_ts"]).hour
                ground_truth[event["order_id"]] = (event["actual_delivery_minutes"], hour)
            elif event.get("event_type") == "courier_gps":
                order_id = event["order_id"]
                if order_id not in zones:  # first ping seen decides the zone
                    zones[order_id] = zone_for(event["lat"], event["lon"])

    ddb = aws.client("dynamodb")
    predicted = {}
    paginator = ddb.get_paginator("scan")
    for page in paginator.paginate(TableName="eta-current"):
        for item in page.get("Items", []):
            predicted[item["order_id"]["S"]] = float(item["eta_minutes"]["N"])

    return _mae_by_group(ground_truth, zones, predicted)


def write_accuracy_by_zone_hour(
    events_path: str, dst: str = "s3://dispatch-agg/eta_accuracy/"
) -> list:
    """Persists the zone/hour MAE breakdown as Parquet under dispatch-agg —
    the same bucket and access pattern src/transformation/replay.py uses
    for order_counts. Skips the write if there's nothing to write (e.g.
    the worker hasn't scored anything yet)."""
    rows = compute_accuracy_by_zone_hour(events_path)
    if not rows:
        return rows

    from utils import warehouse  # noqa: E402 (local import: only needed for --write)

    con = warehouse.connect()
    con.execute(
        "CREATE OR REPLACE TABLE eta_accuracy "
        "(zone VARCHAR, hour INTEGER, n_orders INTEGER, mae_minutes DOUBLE)"
    )
    con.executemany(
        "INSERT INTO eta_accuracy VALUES (?, ?, ?, ?)",
        [(r["zone"], r["hour"], r["n_orders"], r["mae_minutes"]) for r in rows],
    )
    con.execute(f"COPY eta_accuracy TO '{dst}' (FORMAT PARQUET)")
    return rows


def compute_mae(events_path: str) -> dict:
    ground_truth = {}
    with open(events_path) as f:
        for line in f:
            event = json.loads(line)
            if event.get("event_type") == "order_placed":
                ground_truth[event["order_id"]] = event["actual_delivery_minutes"]

    ddb = aws.client("dynamodb")
    predicted = {}
    paginator = ddb.get_paginator("scan")
    for page in paginator.paginate(TableName="eta-current"):
        for item in page.get("Items", []):
            predicted[item["order_id"]["S"]] = float(item["eta_minutes"]["N"])

    matched_order_ids = set(ground_truth) & set(predicted)
    if not matched_order_ids:
        return {
            "n_orders_ground_truth": len(ground_truth),
            "n_orders_scored": len(predicted),
            "n_matched": 0,
            "mae_minutes": None,
        }

    errors = [abs(predicted[oid] - ground_truth[oid]) for oid in matched_order_ids]
    mae = sum(errors) / len(errors)

    return {
        "n_orders_ground_truth": len(ground_truth),
        "n_orders_scored": len(predicted),
        "n_matched": len(matched_order_ids),
        "mae_minutes": round(mae, 2),
        "max_error_minutes": round(max(errors), 2),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", default="data/events.jsonl")
    parser.add_argument(
        "--write",
        action="store_true",
        help="also persist the zone/hour MAE breakdown to s3://dispatch-agg/eta_accuracy/",
    )
    args = parser.parse_args()

    result = compute_mae(args.events)
    if args.write:
        result["zone_hour_accuracy"] = write_accuracy_by_zone_hour(args.events)
    else:
        result["zone_hour_accuracy"] = compute_accuracy_by_zone_hour(args.events)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
