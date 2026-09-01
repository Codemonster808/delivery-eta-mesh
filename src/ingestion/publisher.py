#!/usr/bin/env python3
"""
Publishes order_placed events to SNS (-> the eta-scoring-queue the
worker consumes). courier_gps events are NOT published here — they feed
the nightly PySpark replay (src/transformation/replay.py) directly from
S3, not the real-time scoring path. This is the piece the README's
`producers -> SNS -> SQS` diagram was missing: data_gen.py only ever
wrote a local file.
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils import aws  # noqa: E402

TOPIC_NAME = "dispatch-events"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="in_path", required=True)
    args = parser.parse_args()

    sns = aws.client("sns")
    topic_arn = sns.create_topic(Name=TOPIC_NAME)["TopicArn"]

    n = 0
    with open(args.in_path) as f:
        for line in f:
            event = json.loads(line)
            if event.get("event_type") != "order_placed":
                continue
            sns.publish(TopicArn=topic_arn, Message=json.dumps(event))
            n += 1

    print(f"published {n} order_placed events to {topic_arn}")


if __name__ == "__main__":
    main()
