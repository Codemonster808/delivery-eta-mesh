# Runbook — aprender el mesh de ETA (P4)

Complementa `docs/BUILD_GUIDE.md`. Aquí aparece un **worker Spring Boot** en :8080 (mismo puerto que el gate Go de P1). Nunca dejes los dos vivos.

**Escala learn:** 2 h × 10 restaurantes × 20 órdenes/h. `make demo-full` es el día sintético del README (24×100×200).

---

## 0. Setup por terminal

```bash
cd /home/lesaint/Documentos/life_plans/delivery-eta-mesh
source env.sh
# mata leftovers de P1
lsof -ti:8080 | xargs -r kill
docker compose up -d
make check-env
python3 scripts/bootstrap.py
cd src/worker && mvn -q package -DskipTests && cd ../..
python3 scripts/aws_inspect.py all
```

---

## 1. Flujo paso a paso

### 1.1 Generar el día sintético

```bash
python3 src/data_gen.py --hours 2 --restaurants 10 --orders-per-hour 20 --out data/events.jsonl --seed 42
python3 -c "import json; from collections import Counter; c=Counter(json.loads(l)['event_type'] for l in open('data/events.jsonl')); print(c)"
```

Hay `order_placed` (van a SNS/SQS → worker) y `courier_gps` (van al replay Spark, no al worker).

### 1.2 Worker en Terminal A, publish en B

**Terminal A:**

```bash
source env.sh
java -jar src/worker/target/eta-worker-0.0.1-SNAPSHOT.jar
# espera a curl http://localhost:8080/health
```

**Terminal B:**

```bash
source env.sh
python3 src/publisher.py --in data/events.jsonl
sleep 8
python3 scripts/aws_inspect.py sqs
python3 scripts/aws_inspect.py ddb
```

**Qué inspeccionar:** `eta-scoring-queue` visible baja a 0; DynamoDB `eta-current` se llena con `order_id`. El publisher **solo** manda `order_placed`.

Atajo con cleanup: `make demo` (usa `run_with_bg.sh`; ya no deja el Java colgado).

### 1.3 Replay Spark (watermark + salt de skew)

```bash
python3 src/replay.py --in data/events.jsonl
python3 src/accuracy.py --events data/events.jsonl
make query
```

---

## 2. Explorar con AWS CLI

`aws` respeta `AWS_ENDPOINT_URL` (exportado por `env.sh`), sin flags extra. Este repo **no** usa Lambda ni Step Functions — el worker es un proceso Java normal que hace polling de SQS.

```bash
# SNS — fan-out de order_placed hacia la cola de scoring
TOPIC_ARN=$(aws sns list-topics --query "Topics[?contains(TopicArn,'dispatch-events')].TopicArn" --output text)
aws sns list-subscriptions-by-topic --topic-arn "$TOPIC_ARN"

# SQS — cuánto hay en cola y cuánto está "prestado" al worker ahora mismo
QUEUE_URL=$(aws sqs get-queue-url --queue-name eta-scoring-queue --query QueueUrl --output text)
aws sqs get-queue-attributes --queue-url "$QUEUE_URL" --attribute-names All

# DynamoDB — el ETA calculado por orden, no solo el conteo
aws dynamodb scan --table-name eta-current --max-items 5

# ECS — el path "contenedor real" (make deploy-ecs), en vez de java -jar local
aws ecs list-clusters
aws ecs list-tasks --cluster eta-cluster
aws ecs describe-tasks --cluster eta-cluster --tasks $(aws ecs list-tasks --cluster eta-cluster --query 'taskArns[0]' --output text)
```

**Qué mirar que `aws_inspect.py` no te muestra:** `ApproximateNumberOfMessagesNotVisible` en la cola mientras el worker procesa un batch (mensajes "en vuelo", no perdidos), y el `lastStatus` real de una task ECS (`PROVISIONING` → `RUNNING` → `STOPPED`) si corriste `make deploy-ecs`.

---

## 3. Romper a propósito

### Worker caído → mensajes se acumulan

Sin Terminal A:

```bash
python3 src/publisher.py --in data/events.jsonl
python3 scripts/aws_inspect.py sqs
```

`eta-scoring-queue` visible > 0. Enciende el worker y vuelve a inspeccionar: la cola drena hacia DynamoDB. Eso es “at-least-once + consumer”.

### Puerto ocupado

Si `bind: address already in use`, es P1 gate o un demo anterior:

```bash
lsof -ti:8080 | xargs -r kill
```

---

## 4. Errores

| Error | Significado |
|---|---|
| `QueueDoesNotExist` | Falta `source env.sh` o bootstrap |
| Worker no llega a `/health` | Jar no compilado (`mvn package`) o MiniStack abajo (el worker habla SQS al arrancar) |
| MAE no existe | No corriste `src/accuracy.py` |
| ECS/Fargate | `make deploy-ecs` es el path “contenedor real”; el learn path usa `java -jar` local, más fácil de ver |
| Step Functions / Lambda dan `Unsupported service` | Correcto: P4 no los usa. `docker-compose.yml` solo habilita `s3,sns,sqs,dynamodb,ec2,ecs`. |

---

## 5. Ejercicios

**1. Mide el "at-least-once" con números, no solo con la cola vacía**

Con el worker apagado, publica el día sintético; anota `ApproximateNumberOfMessages`. Enciende el worker y muestrea `get-queue-attributes` cada par de segundos mientras drena.

<details><summary>Verificar</summary>

Vas a ver `ApproximateNumberOfMessages` bajar mientras `ApproximateNumberOfMessagesNotVisible` sube y baja — esos son mensajes que el worker tomó pero aún no confirmó (`DeleteMessage`). Si matas el worker a mitad de un batch, esos mensajes "en vuelo" vuelven a `ApproximateNumberOfMessages` tras el visibility timeout: SQS los reintenta, no los pierde. Así se ve at-least-once delivery en números reales, no solo "la cola llegó a 0".
</details>

**2. Confirma con CLI que el publisher solo manda un tipo de evento**

`aws sqs receive-message --queue-url $QUEUE_URL --max-number-of-messages 5` y revisa el campo `event_type` de cada `Body`.

<details><summary>Verificar</summary>

Todos son `order_placed` — nunca `courier_gps`. `src/publisher.py` filtra antes de mandar a SNS; los eventos GPS solo los consume `src/replay.py` directo del `.jsonl`, nunca pasan por la cola. Es una decisión de diseño real: no todo evento necesita ir por streaming, algunos solo alimentan un batch job.
</details>

**3. Corre el path de contenedor real y compáralo contra `java -jar` local**

`make deploy-ecs`, luego `aws ecs describe-tasks --cluster eta-cluster --tasks <arn>` — compara `lastStatus` y `startedAt` contra cuándo arrancó tu `java -jar` en la sección 1.

<details><summary>Verificar</summary>

La task ECS pasa por `PROVISIONING` → `PENDING` → `RUNNING` con un `startedAt` real, mientras que `java -jar` local no tiene ese ciclo de vida — arranca y ya. Esa diferencia es exactamente por qué el README distingue el "learn path" (`java -jar`, más rápido de iterar) del "path contenedor real" (`make deploy-ecs`, más fiel a producción pero más lento de ciclar).
</details>

---

## 6. Quality report

```bash
make e2e
cat docs/quality-report.md
```

---

## 7. Cerrar

```bash
lsof -ti:8080 | xargs -r kill
docker compose down
```
