"""
Focused integration test for the redelivery-does-not-duplicate guarantee
implemented in the Java worker
(src/worker/src/main/java/com/portfolio/etaworker/EtaScoringWorker.java):
resending an already-scored order over SNS -> SQS must overwrite the
existing eta-current row, not add a second one.

Extracted from tests/data_quality/test_e2e.py so README.md's demo
instructions (`pytest tests/integration/test_worker_idempotency.py`)
point at a real, standalone test instead of a check buried inside the
full pipeline-quality test. tests/data_quality/test_e2e.py keeps its own
copy of this check too, since it feeds the CONSISTENCY dimension of the
quality report that test produces.

Requires: docker compose up -d, scripts/bootstrap.py already run, and the
worker jar built (cd src/worker && mvn -q package -DskipTests) — same
preconditions as tests/data_quality/test_e2e.py. `make test` already
builds the worker jar first via the `build-worker` prerequisite.
"""

import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from utils import aws  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKER_JAR = REPO_ROOT / "src" / "worker" / "target" / "eta-worker-0.0.1-SNAPSHOT.jar"
WORKER_URL = "http://localhost:8080"


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


def test_redelivery_does_not_duplicate(worker_process):
    """Resending the same order_id via SNS must overwrite the eta-current
    row, never add a second one — the guarantee README.md's Impact line
    and the worker class's own docstring both claim."""
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

    # first delivery: wait until it's actually scored
    sns.publish(TopicArn=topic_arn, Message=json.dumps(order))
    deadline = time.time() + 15
    while time.time() < deadline:
        if "Item" in ddb.get_item(TableName="eta-current", Key={"order_id": {"S": order_id}}):
            break
        time.sleep(0.5)
    else:
        pytest.fail("order was not scored by the worker in time")

    # redelivery: resend the identical order_id
    sns.publish(TopicArn=topic_arn, Message=json.dumps(order))
    time.sleep(3)

    rows_for_order = ddb.query(
        TableName="eta-current",
        KeyConditionExpression="order_id = :o",
        ExpressionAttributeValues={":o": {"S": order_id}},
    )["Count"]

    assert rows_for_order == 1, (
        f"expected exactly 1 row for a redelivered order_id, got {rows_for_order} — "
        "redelivery should overwrite, not duplicate"
    )
