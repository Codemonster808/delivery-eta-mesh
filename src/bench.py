#!/usr/bin/env python3
"""Produces benchmarks/results.json from real runs against MiniStack + the live worker."""
import argparse
import json
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import aws  # noqa: E402


def bench_worker_latency(n: int = 30) -> dict:
    sqs = aws.client("sqs")
    ddb = aws.client("dynamodb")
    url = sqs.get_queue_url(QueueName="eta-scoring-queue")["QueueUrl"]

    order_ids = [str(uuid.uuid4()) for _ in range(n)]
    start = time.perf_counter()
    for oid in order_ids:
        sqs.send_message(QueueUrl=url, MessageBody=json.dumps({"order_id": oid, "distance_km": 4.0}))

    scored = 0
    deadline = time.perf_counter() + 30
    while scored < n and time.perf_counter() < deadline:
        time.sleep(0.5)
        scored = sum(
            1 for oid in order_ids
            if "Item" in ddb.get_item(TableName="eta-current", Key={"order_id": {"S": oid}})
        )
    elapsed = time.perf_counter() - start
    return {"n_orders": n, "n_scored": scored, "total_seconds": round(elapsed, 2),
             "avg_ms_per_order": round((elapsed / n) * 1000, 1)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="benchmarks/results.json")
    args = parser.parse_args()

    print("benchmarking worker throughput (requires the worker running: cd src/worker && mvn spring-boot:run)...")
    worker_stats = bench_worker_latency()

    results = {"worker_throughput": worker_stats}
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
