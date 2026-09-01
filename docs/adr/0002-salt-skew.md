# ADR 0002 — Salting restaurant_id vs repartition vs más shuffle.partitions

## Contexto

~5% de restaurants ~60% de orders. Un groupBy por `restaurant_id` deja
una partition enorme.

## Decisión

Salting: `_salt = rand * SALT_BUCKETS` (8), aggregate parcial, re-aggregate.
Se reporta **balance de particiones**, no speedup wall-clock en `local[2]`.

## Alternativas consideradas

- **Subir `spark.sql.shuffle.partitions`**: más slices, misma key caliente
  sigue en una task.
- **Repartition por otra key**: rompe el groupBy de negocio.
- **Broadcast**: no aplica a agregaciones grandes.

## Consecuencias

Totales deben coincidir con naive. Test: `test_replay.py`.
