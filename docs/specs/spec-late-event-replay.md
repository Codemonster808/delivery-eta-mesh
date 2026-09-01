# Spec: late-event replay and skew salt

## Objetivo de negocio

GPS tarde no debe mover el ETA vivo; el replay nocturno sí debe
contarlo. Hot restaurants no deben tumbar una partition.

## Fuentes de entrada

`data/events.jsonl` / S3 raw: `order_placed`, `courier_gps` con `event_ts`
vs arrival `ts`.

## Transformaciones

`apply_watermark`: delay = (ts - event_ts) en minutos.
≤ `WATERMARK_MINUTES` (10) = on-time; more than 10 = late.
`salted_order_counts`: `SALT_BUCKETS=8`. Totales naive vs salted iguales.

## Salida esperada

`s3://dispatch-agg/order_counts/`. Imbalance max/min baja vs shuffle
naive (medido 11.52× → 1.7×).

## Casos borde

Ping +2 min → live. Ping +20 min → excluido del vivo, incluido en replay.

## Criterios de aceptación

`features/late-events.feature`, `features/skew.feature`,
`tests/unit/test_replay.py`.
