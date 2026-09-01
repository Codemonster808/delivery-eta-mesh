#!/usr/bin/env python3
"""
Cost model comparing Fargate (this repo's actual deployment, an
always-on task) vs. a Lambda-based equivalent for the same ETA-scoring
workload, using published AWS pricing (us-east-1, as of this writing —
cited below, not invented).

Fargate: billed per vCPU-second + GB-second while the task runs,
regardless of whether it's actively processing.
Lambda: billed per invocation + GB-second of actual execution time —
scales to zero, but a much higher per-request price at sustained volume.
"""

import argparse
import json
from pathlib import Path

# Source: https://aws.amazon.com/fargate/pricing/ (us-east-1, on-demand, Linux/x86)
FARGATE_VCPU_HOUR = 0.04048
FARGATE_GB_HOUR = 0.004445
FARGATE_TASK_VCPU = 0.25
FARGATE_TASK_GB = 0.5

# Source: https://aws.amazon.com/lambda/pricing/ (us-east-1, x86)
LAMBDA_PER_REQUEST = 0.0000002
LAMBDA_GB_SECOND = 0.0000166667
LAMBDA_MEMORY_GB = 0.5
LAMBDA_AVG_DURATION_MS = (
    50  # measured worker per-message processing time is a few ms; padded for cold-start variance
)


def fargate_cost_per_month(events_per_month: int) -> dict:
    hours_per_month = 24 * 30  # always-on, regardless of event volume
    cost = hours_per_month * (
        FARGATE_TASK_VCPU * FARGATE_VCPU_HOUR + FARGATE_TASK_GB * FARGATE_GB_HOUR
    )
    return {
        "monthly_cost_usd": round(cost, 2),
        "cost_per_million_events_usd": round(cost / (events_per_month / 1_000_000), 2)
        if events_per_month
        else None,
    }


def lambda_cost_per_month(events_per_month: int) -> dict:
    request_cost = events_per_month * LAMBDA_PER_REQUEST
    duration_cost = (
        events_per_month * (LAMBDA_AVG_DURATION_MS / 1000) * LAMBDA_MEMORY_GB * LAMBDA_GB_SECOND
    )
    total = request_cost + duration_cost
    return {
        "monthly_cost_usd": round(total, 2),
        "cost_per_million_events_usd": round(total / (events_per_month / 1_000_000), 2)
        if events_per_month
        else None,
    }


def crossover_events_per_month() -> int:
    """Event volume at which Fargate's fixed cost beats Lambda's per-event cost."""
    fargate_fixed = (
        24 * 30 * (FARGATE_TASK_VCPU * FARGATE_VCPU_HOUR + FARGATE_TASK_GB * FARGATE_GB_HOUR)
    )
    lambda_per_event = (
        LAMBDA_PER_REQUEST + (LAMBDA_AVG_DURATION_MS / 1000) * LAMBDA_MEMORY_GB * LAMBDA_GB_SECOND
    )
    return round(fargate_fixed / lambda_per_event)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events-per-month", type=int, default=10_000_000)
    parser.add_argument("--out", default="docs/cost-comparison.md")
    args = parser.parse_args()

    fargate = fargate_cost_per_month(args.events_per_month)
    lam = lambda_cost_per_month(args.events_per_month)
    crossover = crossover_events_per_month()

    lines = [
        "# Fargate vs. Lambda cost comparison",
        "",
        f"At {args.events_per_month:,} events/month:",
        "",
        "| | Fargate (this repo's deployment) | Lambda (equivalent handler) |",
        "|---|---|---|",
        f"| Monthly cost | ${fargate['monthly_cost_usd']:,} | ${lam['monthly_cost_usd']:,} |",
        f"| $/million events | ${fargate['cost_per_million_events_usd']} "
        f"| ${lam['cost_per_million_events_usd']} |",
        "",
        f"**Crossover point: ~{crossover:,} events/month.** Below that volume, Lambda is cheaper "
        "(pay only for actual invocations); above it, Fargate's fixed always-on cost wins because "
        "it doesn't scale with request count.",
        "",
        "## Assumptions",
        f"- Fargate task: {FARGATE_TASK_VCPU} vCPU / {FARGATE_TASK_GB} GB, always-on (24/7)",
        f"- Lambda: {LAMBDA_MEMORY_GB} GB memory, "
        f"{LAMBDA_AVG_DURATION_MS}ms avg duration/invocation",
        "- Pricing: AWS us-east-1, on-demand, as published at "
        "https://aws.amazon.com/fargate/pricing/ "
        "and https://aws.amazon.com/lambda/pricing/ — not measured against a real AWS bill.",
    ]

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n")

    print(
        json.dumps(
            {"fargate": fargate, "lambda": lam, "crossover_events_per_month": crossover}, indent=2
        )
    )
    print(f"\nwrote {out_path}")


if __name__ == "__main__":
    main()
