# ADR 0001 — Watermark 10 min vs overwrite-always vs drop late

## Contexto

GPS llega tarde u out-of-order. Actualizar siempre el vivo miente al
dispatcher; dropear tarde pierde correcciones históricas.

## Decisión

Watermark 10 minutos: on-time actualiza estado vivo; late va al replay
nocturno.

## Alternativas consideradas

- **Overwrite always**: ETA oscila con paquetes rezagados.
- **Drop late**: el replay no puede reconstruir el día.

## Consecuencias

`WATERMARK_MINUTES` está en el código y en el spec. Cambiarlo invalida
los % late publicados en el README hasta re-medir.
