#!/usr/bin/env python3
"""
Deploys the ETA scoring worker as a real ECS task on MiniStack — not a
docker-compose service running next to the emulator, but launched
through ECS RunTask exactly as it would be in production. Requires the
Docker image already built locally (`cd src/worker && docker build -t
eta-worker:latest .`, i.e. `make docker-worker`).

Before registering the task definition, this pushes that local image to
a real ECR repository on MiniStack and resolves ecs/task-definition.json.template's
{{ECR_IMAGE_URI}} placeholder — the task then pulls from a registry, the
same as it would in production, instead of assuming a local image tag
that happens to already exist on the host (which is what this file did
before ECR was wired in).

Two different hostnames are involved, and mixing them up silently
breaks the push or the pull:
  - `localhost:4566` — reachable from the HOST, used for `docker push`
    (this process runs on the host, building/tagging/pushing).
  - `ministack:4566` — the docker-compose network alias, reachable from
    INSIDE another container. The task definition's image reference
    (and its AWS_ENDPOINT_URL) both have to use this one, because the
    ECS task itself is launched as a separate container with its own
    network namespace — it cannot reach "localhost" and mean the host.
Verified live: MiniStack's ECR accepts `docker push localhost:4566/<repo>:<tag>`
directly, no `docker login` required (a real `aws ecr get-login-password`
credential does exist but the local `docker login` step failed in this
environment on an unrelated credential-helper issue — MiniStack's
registry endpoint didn't require it for the push to succeed anyway).
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils import aws  # noqa: E402

CLUSTER_NAME = "eta-cluster"
REPO_NAME = "eta-worker"
LOCAL_IMAGE = "eta-worker:latest"
PUSH_HOST = "localhost:4566"  # reachable from the host running docker push
PULL_HOST = "ministack:4566"  # reachable from inside an ECS task container
TASK_DEF_TEMPLATE_PATH = (
    Path(__file__).resolve().parents[2] / "ecs" / "task-definition.json.template"
)


def ensure_cluster(ecs) -> None:
    clusters = ecs.list_clusters()["clusterArns"]
    if any(CLUSTER_NAME in c for c in clusters):
        print(f"  cluster already exists: {CLUSTER_NAME}")
        return
    ecs.create_cluster(clusterName=CLUSTER_NAME)
    print(f"  created cluster: {CLUSTER_NAME}")


def ensure_ecr_repo(ecr) -> None:
    existing = [r["repositoryName"] for r in ecr.describe_repositories().get("repositories", [])]
    if REPO_NAME in existing:
        print(f"  repo already exists: {REPO_NAME}")
        return
    ecr.create_repository(repositoryName=REPO_NAME)
    print(f"  created repo: {REPO_NAME}")


def push_image() -> str:
    """Tags the locally-built image for MiniStack's ECR and pushes it.
    Returns the image reference the task definition should use to pull
    it (PULL_HOST, not PUSH_HOST — see module docstring)."""
    push_ref = f"{PUSH_HOST}/{REPO_NAME}:latest"
    pull_ref = f"{PULL_HOST}/{REPO_NAME}:latest"
    subprocess.run(["docker", "tag", LOCAL_IMAGE, push_ref], check=True)
    subprocess.run(["docker", "push", push_ref], check=True)
    print(f"  pushed: {push_ref}")
    return pull_ref


def register_task_definition(ecs, image_uri: str) -> str:
    rendered = TASK_DEF_TEMPLATE_PATH.read_text().replace("{{ECR_IMAGE_URI}}", image_uri)
    td = json.loads(rendered)
    resp = ecs.register_task_definition(
        family=td["family"], containerDefinitions=td["containerDefinitions"]
    )
    family_rev = f"{resp['taskDefinition']['family']}:{resp['taskDefinition']['revision']}"
    print(f"  registered task definition: {family_rev} (image={image_uri})")
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
    ecr = aws.client("ecr")
    print("Ensuring ECS cluster:")
    ensure_cluster(ecs)
    print("Ensuring ECR repo:")
    ensure_ecr_repo(ecr)
    print("Pushing image:")
    image_uri = push_image()
    print("Registering task definition:")
    task_family = register_task_definition(ecs, image_uri)
    print("Running worker task:")
    task_arn = run_worker_task(ecs, task_family)
    status = wait_running(ecs, task_arn)
    return {"cluster": CLUSTER_NAME, "task_arn": task_arn, "status": status, "image": image_uri}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.parse_args()
    result = deploy()
    print(json.dumps(result, indent=2))
    if result["status"] != "RUNNING":
        raise SystemExit(f"worker task did not reach RUNNING (status={result['status']})")


if __name__ == "__main__":
    main()
