"""
BDD wrapper around tests/integration/test_worker_idempotency.py's
redelivery-does-not-duplicate check. Reuses that test's real setup
(worker JVM subprocess, SNS publish, DynamoDB eta-current query) rather
than inventing new behavior — see that module's docstring for the full
Java-worker context and preconditions (worker jar built, MiniStack up,
scripts/bootstrap.py run).
"""

import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
import requests
from pytest_bdd import given, scenarios, then, when

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from utils import aws  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKER_JAR = REPO_ROOT / "src" / "worker" / "target" / "eta-worker-0.0.1-SNAPSHOT.jar"
WORKER_URL = "http://localhost:8080"

scenarios("../worker-redelivery.feature")


@pytest.fixture(scope="module")
def worker_process():
    if not WORKER_JAR.exists():
        pytest.skip(
            "worker jar not built — run `cd src/worker && mvn -q package -DskipTests` first"
        )
    proc = subprocess.Popen(
        ["java", "-jar", str(WORKER_JAR)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    # Spring Boot cold start (class loading, no JIT warm-up yet) has been
    # observed to take >20s in this environment, so this budget is
    # deliberately generous rather than tight.
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            if requests.get(f"{WORKER_URL}/health", timeout=1).status_code == 200:
                break
        except requests.ConnectionError:
            pass
        time.sleep(0.5)
    else:
        proc.terminate()
        pytest.fail("worker did not become healthy in time")
    yield proc
    proc.terminate()
    proc.wait(timeout=5)


@given("the eta scoring worker is running", target_fixture="order_ctx")
def order_ctx(worker_process):
    ddb = aws.client("dynamodb")
    sns = aws.client("sns")
    order_id = str(uuid.uuid4())
    order = {
        "event_type": "order_placed",
        "order_id": order_id,
        "restaurant_id": "rest_0000",
        "distance_km": 4.0,
    }
    topic_arn = sns.create_topic(Name="dispatch-events")["TopicArn"]
    return {"ddb": ddb, "sns": sns, "topic_arn": topic_arn, "order": order, "order_id": order_id}


@when("an order is published and scored")
def publish_and_wait_for_score(order_ctx):
    ddb, sns = order_ctx["ddb"], order_ctx["sns"]
    sns.publish(TopicArn=order_ctx["topic_arn"], Message=json.dumps(order_ctx["order"]))
    deadline = time.time() + 15
    while time.time() < deadline:
        key = {"order_id": {"S": order_ctx["order_id"]}}
        if "Item" in ddb.get_item(TableName="eta-current", Key=key):
            break
        time.sleep(0.5)
    else:
        pytest.fail("order was not scored by the worker in time")


@when("the same order_id is redelivered")
def redeliver(order_ctx):
    sns = order_ctx["sns"]
    sns.publish(TopicArn=order_ctx["topic_arn"], Message=json.dumps(order_ctx["order"]))
    time.sleep(3)


@then("there is exactly one eta-current row for that order_id")
def exactly_one_row(order_ctx):
    ddb = order_ctx["ddb"]
    rows_for_order = ddb.query(
        TableName="eta-current",
        KeyConditionExpression="order_id = :o",
        ExpressionAttributeValues={":o": {"S": order_ctx["order_id"]}},
    )["Count"]

    assert rows_for_order == 1, (
        f"expected exactly 1 row for a redelivered order_id, got {rows_for_order} — "
        "redelivery should overwrite, not duplicate"
    )
