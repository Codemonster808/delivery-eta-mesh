# Spec: ETA scoring (Spring Boot worker)

## Objetivo de negocio

Asignar courier con un ETA vivo, no un número de notebook.

## Fuentes de entrada

SNS `dispatch-events` → SQS. Eventos `order_placed` (y GPS para
actualizaciones). Worker Java en `src/worker/` (no se mueve).

## Transformaciones

Prep 12 min + distancia / 22 km/h. Upsert DynamoDB `eta-current` por
`order_id`.

## Salida esperada

`GET /eta/{order_id}` refleja la última escritura. Redelivery no duplica
filas.

## Casos borde

Mismo `order_id` reentregado → overwrite. Worker y fintech gate no pueden
escuchar :8080 a la vez.

## Criterios de aceptación

`features/worker-redelivery.feature`,
`tests/integration/test_worker_idempotency.py`.
