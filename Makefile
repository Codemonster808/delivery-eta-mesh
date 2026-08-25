.PHONY: demo test e2e bench query check-env replay build-worker docker-worker deploy-ecs

check-env:
	python3 scripts/check_env.py

build-worker:
	cd src/worker && mvn -q package -DskipTests

docker-worker: build-worker
	cd src/worker && docker build -t eta-worker:latest .

deploy-ecs: docker-worker
	python3 src/deploy_ecs.py

demo: build-worker
	docker compose up -d
	python3 scripts/bootstrap.py
	python3 src/data_gen.py --hours 24 --restaurants 100 --orders-per-hour 200 --out data/events.jsonl
	java -jar src/worker/target/eta-worker-0.0.1-SNAPSHOT.jar & sleep 5; \
	python3 src/publisher.py --in data/events.jsonl

test: build-worker
	pytest tests/ -v --ignore=tests/test_e2e.py
	cd src/worker && mvn -q test

e2e: build-worker
	pytest tests/test_e2e.py -v -s

bench:
	python3 src/bench.py --out benchmarks/results.json

accuracy:
	python3 src/accuracy.py --events data/events.jsonl

cost-compare:
	python3 src/cost_compare.py --events-per-month 10000000

query:
	python3 -c "import sys; sys.path.insert(0,'src'); from common import warehouse; \
	con = warehouse.connect(); \
	warehouse.read_parquet(con, 's3://dispatch-agg/order_counts/**/*.parquet', 'order_counts'); \
	print(con.execute('SELECT * FROM order_counts ORDER BY n_orders DESC LIMIT 10').fetchall())"

replay:
	python3 src/replay.py --in data/events.jsonl
