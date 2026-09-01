# Build Guide — delivery-eta-mesh

Estimated total: ~24 hours across 2-3 weeks of evenings. This is the repo most likely to run long — if time gets tight, it's the safest one to trim (skip EC2 simulator, use a Python script instead — see step 2 note).

## Glossary

- **Watermark**: a rule saying "I'll wait up to N minutes for late-arriving events before I consider a time window closed."
- **Key salting**: splitting a "hot" key (e.g. one very busy restaurant) into several fake sub-keys so Spark can process it in parallel instead of on one worker.
- **Fargate**: a way to run a container on AWS without managing the underlying server.

## 0. Before you start (30 min)

```bash
docker --version   # native Docker Engine, not Docker Desktop
python3 --version  # 3.12+
java --version      # 17 (Spring Boot 3.4 target)
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

```bash
docker compose up -d
curl http://localhost:4566/_health
```

## 1. Get the environment running (1 h) → checkpoint: `make check-env`

```bash
docker compose up -d
python3 scripts/bootstrap.py
make check-env
```

## 2. Generate synthetic data (2 h) → checkpoint: `make check-data`

Generate a synthetic dispatch day: orders, courier GPS pings (some deliberately 5-15 minutes late), with a skewed restaurant distribution (5% of restaurant IDs generate 60% of orders).

```bash
python3 src/ingestion/data_gen.py --hours 24 --out data/events.jsonl
make check-data   # "OK: skew confirmed (top 5% restaurants = ~60% of events), 12% events marked late"
```

**Note:** the EC2 "simulator daemon" can just be this same script run as a long-lived process publishing to SNS on a timer — you do not need a real EC2 instance to satisfy this step; MiniStack's EC2 emulation is for architecture credibility, not for actually hosting the daemon.

## 3. Build the scoring worker (5-6 h) → checkpoint: `make check-worker`

Write the Spring Boot app (`src/worker/`): SQS listener, a simple ETA heuristic (distance / avg speed + prep time), write to DynamoDB.

```bash
cd src/worker && ./mvnw test
make check-worker   # replays 1000 events, asserts a DynamoDB ETA row exists for each order
```

**Troubleshooting**
- Worker doesn't pick up messages → check the MiniStack SQS endpoint is set via `AWS_ENDPOINT_URL`, not the real AWS endpoint.

## 4. Build the FastAPI live layer (1-2 h) → checkpoint: `make check-api`

```bash
uvicorn src.serving.api:app --reload
curl localhost:8000/eta/order_123
```

## 5. Build the nightly Spark replay + watermarking (4-5 h) → checkpoint: `make check-watermark`

Write `src/transformation/replay.py`: read the full day, apply a watermark (events arriving >10 min after their window closes are marked `late_correction` instead of updating the live row), and reconcile the final daily accuracy numbers.

```bash
make check-watermark   # asserts late events are captured in a separate correction table, not silently merged
```

## 6. Fix Spark skew (3-4 h) → checkpoint: `make check-skew`

First, measure the job's runtime with the naturally skewed data (`make bench` before). Then implement salting: append a random suffix (0-N) to hot restaurant IDs before the shuffle, aggregate, then strip the suffix and re-aggregate.

```bash
make check-skew   # measured speedup must be documented in benchmarks/skew_before_after.json
```

## 7. Build the cost comparison (2-3 h) → checkpoint: `docs/cost-comparison.md`

Run the same 10M-event synthetic workload through the Fargate worker path and a Lambda-based equivalent (a simplified handler). Compute $/million events for each using published AWS pricing.

```bash
python3 scripts/cost_compare.py --events 10000000 --out docs/cost-comparison.md
```

## 8. Measure, model, ship (3 h)

```bash
make bench
```

Fill `docs/impact-model.md` and both README metric tables.

## Troubleshooting index

| Symptom | Likely cause | Fix |
|---|---|---|
| Spring Boot worker OOMs under batch load | SQS batch size too large for local heap | lower `maxNumberOfMessages` in `application.yml` |
| Salting doesn't improve runtime | suffix range too small, still concentrating on few partitions | increase salt range, re-check partition sizes |

## Total estimated effort: ~24 hours (2-3 weeks of evenings)
