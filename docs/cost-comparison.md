# Fargate vs. Lambda cost comparison

At 10,000,000 events/month:

| | Fargate (this repo's deployment) | Lambda (equivalent handler) |
|---|---|---|
| Monthly cost | $8.89 | $6.17 |
| $/million events | $0.89 | $0.62 |

**Crossover point: ~14,410,683 events/month.** Below that volume, Lambda is cheaper (pay only for actual invocations); above it, Fargate's fixed always-on cost wins because it doesn't scale with request count.

## Assumptions
- Fargate task: 0.25 vCPU / 0.5 GB, always-on (24/7)
- Lambda: 0.5 GB memory, 50ms avg duration/invocation
- Pricing: AWS us-east-1, on-demand, as published at https://aws.amazon.com/fargate/pricing/ and https://aws.amazon.com/lambda/pricing/ — not measured against a real AWS bill.
