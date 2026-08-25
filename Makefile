SHELL := /bin/bash
.PHONY: demo demo-full test e2e bench query check-env replay build-worker docker-worker deploy-ecs inspect accuracy cost-compare

ENV := set -a && source ./env.sh --quiet && set +a

DEMO_HOURS ?= 2
DEMO_RESTAURANTS ?= 10
DEMO_OPH ?= 20
DEMO_FULL_HOURS ?= 24
DEMO_FULL_RESTAURANTS ?= 100
DEMO_FULL_OPH ?= 200

check-env:
	$(ENV) && python3 scripts/check_env.py

inspect:
	$(ENV) && python3 scripts/aws_inspect.py all

build-worker:
	cd src/worker && mvn -q package -DskipTests

docker-worker: build-worker
	cd src/worker && docker build -t eta-worker:latest .

deploy-ecs: docker-worker
	$(ENV) && python3 src/deploy_ecs.py

# Small scale. Worker always cleaned up so :8080 is free afterwards.
demo: build-worker
	$(ENV) && docker compose up -d
	$(ENV) && python3 scripts/bootstrap.py
	$(ENV) && python3 src/data_gen.py --hours $(DEMO_HOURS) --restaurants $(DEMO_RESTAURANTS) --orders-per-hour $(DEMO_OPH) --out data/events.jsonl
	$(ENV) && bash scripts/run_with_bg.sh 8080 'java -jar src/worker/target/eta-worker-0.0.1-SNAPSHOT.jar' -- \
		bash -c 'python3 src/publisher.py --in data/events.jsonl && sleep 12'

demo-full: build-worker
	$(MAKE) demo DEMO_HOURS=$(DEMO_FULL_HOURS) DEMO_RESTAURANTS=$(DEMO_FULL_RESTAURANTS) DEMO_OPH=$(DEMO_FULL_OPH)

test: build-worker
	$(ENV) && pytest tests/ -v --ignore=tests/test_e2e.py
	cd src/worker && mvn -q test

e2e: build-worker
	$(ENV) && pytest tests/test_e2e.py -v -s

bench:
	$(ENV) && python3 src/bench.py --out benchmarks/results.json

accuracy:
	$(ENV) && python3 src/accuracy.py --events data/events.jsonl

cost-compare:
	$(ENV) && python3 src/cost_compare.py --events-per-month 10000000

query:
	$(ENV) && python3 -c "import sys; sys.path.insert(0,'src'); from common import warehouse; \
	con = warehouse.connect(); \
	warehouse.read_parquet(con, 's3://dispatch-agg/order_counts/**/*.parquet', 'order_counts'); \
	print(con.execute('SELECT * FROM order_counts ORDER BY n_orders DESC LIMIT 10').fetchall())"

replay:
	$(ENV) && python3 src/replay.py --in data/events.jsonl
