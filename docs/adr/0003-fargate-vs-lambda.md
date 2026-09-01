# ADR 0003 — Fargate/Spring Boot vs Lambda

## Contexto

Scoring es un consumer SQS de alto QPS, warm JVM.

## Decisión

Worker Spring Boot en Fargate (ECS task real en MiniStack). Lambda sería
cold-start en Python/Java por batch.

## Alternativas consideradas

- **Lambda**: más barato a volumen muy bajo; el cruce está en
  `docs/cost-comparison.md` / `scripts/cost_compare.py`.
- **Python worker**: el portfolio ya tiene Python; el JVM aquí es el
  hop de latencia, acotado — honesty note en README.

## Consecuencias

`:8080` compartido con el gate Go de fintech. `src/worker/` no se mueve.
