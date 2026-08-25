#!/usr/bin/env python3
"""
Deploys the ETA scoring worker as a real ECS task on MiniStack — not a
docker-compose service running next to the emulator, but launched
through ECS RunTask exactly as it would be in production. Requires the
Docker image already built (`cd src/worker && docker build -t
eta-worker:latest .`).

The container reaches MiniStack over the docker-compose network alias
"ministack:4566" (the default_default network both this container and
the MiniStack container join), not localhost — an ECS task is a
separate container with its own network namespace.
"""
import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import aws  # noqa: E402

CLUSTER_NAME = "eta-cluster"
TASK_DEF_PATH = Path(__file__).resolve().parents[1] / "ecs" / "task-definition.json"


def ensure_cluster(ecs) -> None:
    clusters = ecs.list_clusters()["clusterArns"]
    if any(CLUSTER_NAME in c for c in clusters):
        print(f"  cluster already exists: {CLUSTER_NAME}")
        return
    ecs.create_cluster(clusterName=CLUSTER_NAME)
    print(f"  created cluster: {CLUSTER_NAME}")


def register_task_definition(ecs) -> str:
    td = json.loads(TASK_DEF_PATH.read_text())
    resp = ecs.register_task_definition(family=td["family"], containerDefinitions=td["containerDefinitions"])
    family_rev = f"{resp['taskDefinition']['family']}:{resp['taskDefinition']['revision']}"
    print(f"  registered task definition: {family_rev}")
    return td["family"]


def run_worker_task(ecs, task_family: str) -> str:
    resp = ecs.run_task(cluster=CLUSTER_NAME, taskDefinition=task_family, count=1)
    if resp.get("failures"):
        raise RuntimeError(f"ECS RunTask failures: {resp['failures']}")
    task_arn = resp["tasks"][0]["taskArn"]
    print(f"  started task: {task_arn}")
    return task_arn


def wait_running(ecs, task_arn: str, timeout_s: float = 30) -> str:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        desc = ecs.describe_tasks(cluster=CLUSTER_NAME, tasks=[task_arn])
        status = desc["tasks"][0]["lastStatus"]
        if status in ("RUNNING", "STOPPED"):
            return status
        time.sleep(1)
    raise TimeoutError(f"task {task_arn} did not reach RUNNING/STOPPED in time")


def deploy() -> dict:
    ecs = aws.client("ecs")
    print("Ensuring ECS cluster:")
    ensure_cluster(ecs)
    print("Registering task definition:")
    task_family = register_task_definition(ecs)
    print("Running worker task:")
    task_arn = run_worker_task(ecs, task_family)
    status = wait_running(ecs, task_arn)
    return {"cluster": CLUSTER_NAME, "task_arn": task_arn, "status": status}


def main() -> None:
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    result = deploy()
    print(json.dumps(result, indent=2))
    if result["status"] != "RUNNING":
        raise SystemExit(f"worker task did not reach RUNNING (status={result['status']})")


if __name__ == "__main__":
    main()
