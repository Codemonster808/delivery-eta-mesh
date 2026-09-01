# Spec: ETA accuracy MAE by zone/hour

## Objetivo de negocio

No basta contar pedidos. Hay que ver error de ETA donde duele (zona × hora).

## Fuentes de entrada

Eventos + ETAs escritos. `scripts/accuracy.py --write`.

## Transformaciones

MAE en minutos, agrupado por zone y hour. Target de aceptación del
portfolio: MAE ≤ 12 min (modelo sintético, no prod).

## Salida esperada

Agregados bajo `s3://dispatch-agg/` (además de `order_counts`).
`GET /accuracy/daily` en FastAPI.

## Casos borde

Sin GPS on-time, el MAE de esa zona queda indefinido / omitido — no
inventar 0.

## Criterios de aceptación

README architecture line; `make accuracy`. No hay un umbral CI hard-fail
salvo el script corra.
