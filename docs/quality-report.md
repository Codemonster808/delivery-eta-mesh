# Quality report — delivery-eta-mesh

Generated: 2026-08-25T16:31:15.439002+00:00

**Overall score: 100%** (6/6 checks passed)

| Dimension | Score |
|---|---|
| completeness | 100% |
| correctness | 100% |
| consistency | 100% |
| validity | 100% |
| timeliness | 100% |

## Checks

| Dimension | Check | Measured | Threshold | Status | Detail |
|---|---|---|---|---|---|
| completeness | every_published_order_gets_scored | 100 | 100 | PASS | 100/100 orders have an eta-current row |
| correctness | eta_mae_within_bound | 5.0 | 12.0 | PASS | MAE 5.0 min against synthetic ground truth |
| consistency | redelivery_does_not_duplicate | 1 | 1 | PASS | resending an already-scored order must overwrite, not add a row |
| validity | salting_reduces_partition_imbalance | 9.67 | 77.0 | PASS | naive imbalance=77.0, salted imbalance=9.67 |
| timeliness | worker_scores_orders_promptly | 5.0 | 20.0 | PASS | 100 orders scored |
| timeliness | replay_job_under_sla | 31.3 | 180.0 | PASS | PySpark watermark + salting job wall time |
