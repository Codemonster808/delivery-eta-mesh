#!/usr/bin/env python3
"""
Computes ETA MAE: the worker's predicted eta_minutes (DynamoDB
eta-current) against each order's synthetic ground-truth
actual_delivery_minutes (embedded in data_gen.py's output) — the metric
the README promised and previously never actually computed.
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import aws  # noqa: E402


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
        return {"n_orders_ground_truth": len(ground_truth), "n_orders_scored": len(predicted),
                "n_matched": 0, "mae_minutes": None}

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
    args = parser.parse_args()

    result = compute_mae(args.events)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
