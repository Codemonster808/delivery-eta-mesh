# Architecture

```mermaid
flowchart LR
    SIM[EC2: courier GPS simulator\nlong-running daemon] --> SNS[SNS: dispatch-events]
    ORD[Order events] --> SNS
    SNS --> SQS[SQS: scoring queue + DLQ]
    SQS --> WORKER[Spring Boot worker on Fargate]
    WORKER --> DDB[(DynamoDB\ncurrent ETA)]
    SNS --> S3RAW[(S3 raw, partitioned)]
    NIGHTLY[Nightly PySpark] --> S3RAW
    NIGHTLY -->|watermark late events\nsalt hot keys| S3AGG[(S3 aggregates)]
    S3AGG --> RS[(Redshift)]
    RS --> API[FastAPI: /eta/id /accuracy/daily]
    DDB --> API
```

## Data flow notes

- The Fargate worker only ever writes the *current* ETA — it never touches history. History correction (for late-arriving GPS events) happens only in the nightly Spark replay.
- Salting: hot restaurant keys are split into N sub-keys during the Spark shuffle, then re-aggregated — this is the fix for the skew challenge described in the README.
