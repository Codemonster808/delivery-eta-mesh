"""
End-to-end quality test: publisher -> SNS -> SQS -> Spring Boot worker
-> DynamoDB -> accuracy, plus the PySpark replay job's watermark/salting
behavior, scored on the 5 standard quality dimensions.

Requires: docker compose up -d, scripts/bootstrap.py already run, and
the worker jar built (cd src/worker && mvn -q package -DskipTests).
"""
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common import aws  # noqa: E402
from common.quality import Dimension, QualityReport  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKER_JAR = REPO_ROOT / "src" / "worker" / "target" / "eta-worker-0.0.1-SNAPSHOT.jar"
WORKER_URL = "http://localhost:8080"
N_HOURS = 2
N_RESTAURANTS = 20
ORDERS_PER_HOUR = 50


@pytest.fixture(scope="module")
def worker_process():
    if not WORKER_JAR.exists():
        pytest.skip(f"worker jar not built — run `cd src/worker && mvn -q package -DskipTests` first")
    proc = subprocess.Popen(["java", "-jar", str(WORKER_JAR)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(30):
        try:
            if requests.get(f"{WORKER_URL}/health", timeout=1).status_code == 200:
                break
        except requests.ConnectionError:
            time.sleep(0.5)
    else:
        proc.terminate()
        pytest.fail("worker did not become healthy in time")
    yield proc
    proc.terminate()
    proc.wait(timeout=5)


def _clear_table(ddb, table: str, key: str) -> None:
    for item in ddb.scan(TableName=table).get("Items", []):
        ddb.delete_item(TableName=table, Key={key: item[key]})


def test_full_pipeline_quality(worker_process):
    run_id = uuid.uuid4().hex[:8]
    data_path = REPO_ROOT / "data" / f"e2e_events_{run_id}.jsonl"

    ddb = aws.client("dynamodb")
    _clear_table(ddb, "eta-current", "order_id")

    gen = subprocess.run(
        [sys.executable, "src/data_gen.py", "--hours", str(N_HOURS), "--restaurants", str(N_RESTAURANTS),
         "--orders-per-hour", str(ORDERS_PER_HOUR), "--out", str(data_path), "--seed", "99"],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=30,
    )
    assert gen.returncode == 0, gen.stderr
    n_orders_generated = sum(
        1 for l in data_path.read_text().splitlines() if json.loads(l)["event_type"] == "order_placed"
    )

    pub = subprocess.run(
        [sys.executable, "src/publisher.py", "--in", str(data_path)],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=30,
    )
    assert pub.returncode == 0, pub.stderr

    # --- wait for the worker to score everything (or time out) ---
    t0 = time.perf_counter()
    deadline = time.time() + 30
    n_scored = 0
    while time.time() < deadline:
        n_scored = len(ddb.scan(TableName="eta-current").get("Items", []))
        if n_scored >= n_orders_generated:
            break
        time.sleep(0.5)
    worker_seconds = time.perf_counter() - t0

    accuracy = subprocess.run(
        [sys.executable, "src/accuracy.py", "--events", str(data_path)],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=15,
    )
    assert accuracy.returncode == 0, accuracy.stderr
    accuracy_result = json.loads(accuracy.stdout)

    # --- redelivery safety: resend one already-scored order, confirm no duplicate row ---
    events = [json.loads(l) for l in data_path.read_text().splitlines() if json.loads(l)["event_type"] == "order_placed"]
    redelivered_order = events[0]
    sns = aws.client("sns")
    topic_arn = sns.create_topic(Name="dispatch-events")["TopicArn"]
    sns.publish(TopicArn=topic_arn, Message=json.dumps(redelivered_order))
    time.sleep(3)
    rows_for_order = ddb.query(
        TableName="eta-current",
        KeyConditionExpression="order_id = :o",
        ExpressionAttributeValues={":o": {"S": redelivered_order["order_id"]}},
    )["Count"]

    # --- PySpark replay: watermark + salting, on the full generated file ---
    t0 = time.perf_counter()
    replay = subprocess.run(
        [sys.executable, "src/replay.py", "--in", str(data_path)],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=180,
    )
    replay_seconds = time.perf_counter() - t0
    assert replay.returncode == 0, replay.stderr
    replay_out = replay.stdout

    report = QualityReport(pipeline="delivery-eta-mesh")

    report.check(
        Dimension.COMPLETENESS, "every_published_order_gets_scored",
        measured=n_scored, threshold=n_orders_generated,
        detail=f"{n_scored}/{n_orders_generated} orders have an eta-current row",
    )
    report.check(
        Dimension.CORRECTNESS, "eta_mae_within_bound", measured=accuracy_result["mae_minutes"],
        threshold=12.0, higher_is_better=False,
        detail=f"MAE {accuracy_result['mae_minutes']} min against synthetic ground truth",
    )
    report.check(
        Dimension.CONSISTENCY, "redelivery_does_not_duplicate", measured=rows_for_order, threshold=1,
        detail="resending an already-scored order must overwrite, not add a row",
    )
    import re
    naive_match = re.search(r"naive partition balance.*imbalance_ratio': ([\d.]+)", replay_out)
    salted_match = re.search(r"salted-then-reaggregated partition balance.*imbalance_ratio': ([\d.]+)", replay_out)
    naive_ratio = float(naive_match.group(1)) if naive_match else None
    salted_ratio = float(salted_match.group(1)) if salted_match else None
    report.check(
        Dimension.VALIDITY, "salting_reduces_partition_imbalance",
        measured=(salted_ratio if salted_ratio is not None else 999),
        threshold=(naive_ratio if naive_ratio is not None else 0), higher_is_better=False,
        detail=f"naive imbalance={naive_ratio}, salted imbalance={salted_ratio}",
    )
    report.check(
        Dimension.TIMELINESS, "worker_scores_orders_promptly", measured=round(worker_seconds, 1),
        threshold=20.0, higher_is_better=False, detail=f"{n_orders_generated} orders scored",
    )
    report.check(
        Dimension.TIMELINESS, "replay_job_under_sla", measured=round(replay_seconds, 1),
        threshold=180.0, higher_is_better=False, detail="PySpark watermark + salting job wall time",
    )

    report.to_json(str(REPO_ROOT / "benchmarks" / "quality-report.json"))
    report.to_markdown(str(REPO_ROOT / "docs" / "quality-report.md"))

    data_path.unlink(missing_ok=True)

    report.assert_all_passed()
