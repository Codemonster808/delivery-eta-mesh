.PHONY: demo test bench query check-env

check-env:
	python3 scripts/check_env.py

demo:
	docker compose up -d
	python3 scripts/bootstrap.py
	python3 src/data_gen.py --hours 24 --restaurants 100 --orders-per-hour 200 --out data/events.jsonl
	@echo "Start the worker in another terminal: cd src/worker && mvn spring-boot:run"

test:
	pytest tests/ -v
	cd src/worker && mvn -q test

bench:
	python3 src/bench.py --out benchmarks/results.json

query:
	python3 -c "import sys; sys.path.insert(0,'src'); from common import warehouse; \
	con = warehouse.connect(); \
	warehouse.read_parquet(con, 's3://dispatch-agg/order_counts/**/*.parquet', 'order_counts'); \
	print(con.execute('SELECT * FROM order_counts ORDER BY n_orders DESC LIMIT 10').fetchall())"

replay:
	python3 src/replay.py --in data/events.jsonl
