#!/usr/bin/env python3
"""Idempotent creation of the AWS resources this repo needs, against MiniStack."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from utils import aws  # noqa: E402

BUCKETS = ["dispatch-raw", "dispatch-agg"]
QUEUE_NAME = "eta-scoring-queue"
DLQ_NAME = "eta-scoring-dlq"
ETA_TABLE = "eta-current"


def ensure_bucket(s3, name: str) -> None:
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if name not in existing:
        s3.create_bucket(Bucket=name)
        print(f"  created bucket: {name}")
    else:
        print(f"  bucket already exists: {name}")


def ensure_queue(sqs, name: str, redrive_to: str | None = None) -> str:
    try:
        url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
        print(f"  queue already exists: {name}")
        return url
    except sqs.exceptions.QueueDoesNotExist:
        attrs = {}
        if redrive_to:
            dlq_arn = sqs.get_queue_attributes(
                QueueUrl=sqs.get_queue_url(QueueName=redrive_to)["QueueUrl"],
                AttributeNames=["QueueArn"],
            )["Attributes"]["QueueArn"]
            import json

            attrs["RedrivePolicy"] = json.dumps(
                {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "3"}
            )
        url = sqs.create_queue(QueueName=name, Attributes=attrs)["QueueUrl"]
        print(f"  created queue: {name}")
        return url


def ensure_table(dynamodb, table_name: str, key_name: str) -> None:
    existing = dynamodb.list_tables()["TableNames"]
    if table_name in existing:
        print(f"  table already exists: {table_name}")
        return
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": key_name, "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": key_name, "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    print(f"  created table: {table_name}")


def ensure_subscription(sns, sqs, topic_arn: str, queue_url: str) -> None:
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    existing = sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
    if any(s["Endpoint"] == queue_arn for s in existing):
        print(f"  subscription already exists: {queue_arn}")
        return
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
        Attributes={"RawMessageDelivery": "true"},
    )
    print(f"  subscribed {queue_arn} -> {topic_arn}")


def main() -> None:
    print("S3 buckets:")
    s3 = aws.client("s3")
    for bucket in BUCKETS:
        ensure_bucket(s3, bucket)

    print("SQS queues:")
    sqs = aws.client("sqs")
    ensure_queue(sqs, DLQ_NAME)
    scoring_queue_url = ensure_queue(sqs, QUEUE_NAME, redrive_to=DLQ_NAME)

    print("SNS topic + subscription:")
    sns = aws.client("sns")
    arn = sns.create_topic(Name="dispatch-events")["TopicArn"]
    print(f"  topic ready: {arn}")
    ensure_subscription(sns, sqs, arn, scoring_queue_url)

    print("DynamoDB table:")
    ensure_table(aws.client("dynamodb"), ETA_TABLE, "order_id")

    print("Bootstrap complete.")


if __name__ == "__main__":
    main()
