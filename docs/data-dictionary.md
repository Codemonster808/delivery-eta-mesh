# Data dictionary — delivery-eta-mesh

| Resource | Grain / key | Lineage |
|---|---|---|
| SNS `dispatch-events` | envelope per event | `src/ingestion/publisher.py` |
| SQS scoring queue + DLQ | | fan-out from SNS |
| DDB `eta-current` | `order_id` | Java worker upsert |
| S3 raw events | partitioned ingest | publisher / worker side effects |
| `s3://dispatch-agg/order_counts/` | `restaurant_id`, `n_orders` | `replay.py:salted_order_counts` |
| accuracy aggregates | zone × hour MAE | `scripts/accuracy.py --write` |

Schemas: `order_placed`, `courier_gps` (see `data_gen.py`). No Redshift DDL
in this repo — DuckDB over Parquet (`utils/warehouse.py`).
