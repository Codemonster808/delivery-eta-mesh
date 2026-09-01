# CLAUDE.md — delivery-eta-mesh

Operating constitution. See `docs/architecture.md` and `docs/adr/`.

## 1. Domain context

ETA mesh for food dispatch. "Correct" means:

- **Late GPS** (`arrival_delay_min` > `WATERMARK_MINUTES=10`) does not
  update live ETA; it is kept for nightly replay
  (`src/transformation/replay.py:apply_watermark`).
- **Skew:** top restaurants dominate. Salt `restaurant_id` with
  `SALT_BUCKETS=8` before the shuffle; naive vs salted totals must match;
  partition imbalance must drop (measured 11.52× → 1.7×).
- **Worker redelivery:** same `order_id` overwrites `eta-current`, never
  a second row (`tests/integration/test_worker_idempotency.py`).
- **Java worker stays in `src/worker/`** (Maven). Do not move it into
  `ingestion/` — pom, Dockerfile, ECS task def, and CI all assume that
  path.

Scoring rule (worker): prep 12 min + distance / 22 km/h.

## 2. Exact commands

Recipes run under `source ./env.sh`. Default local endpoint
`http://localhost:4584` (parallel MiniStack). Gate/worker share **:8080**
with the fintech Go gate — one at a time, or `scripts/run_with_bg.sh`.

```bash
source env.sh
docker compose up -d
make check-env
make build-worker
make demo
make test              # pytest + mvn test
make e2e
make replay
make accuracy          # scripts/accuracy.py --write (zone/hour MAE)
make cost-compare
make deploy-ecs
```

## 3. Naming conventions

SNS `dispatch-events`; SQS scoring queue + DLQ; DDB `eta-current`;
S3 raw events + `s3://dispatch-agg/order_counts/` and accuracy aggregates.

## 4. Schema and data rules

Event types `order_placed` and `courier_gps`. Synthetic `--seed`. MAE
target in spec: ≤12 min (`docs/specs/spec-accuracy-mae.md`).

## 5. Do not touch without asking

`.env`; MiniStack teardown by hand; changing `WATERMARK_MINUTES` /
`SALT_BUCKETS` without updating specs, README measured table, and tests.

## 6. Specs and features

`docs/specs/`, `docs/adr/`, `features/*.feature`.
