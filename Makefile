.PHONY: init-env up down demo docker-demo test lint local-demo
init-env:
	python scripts/init_env.py

docker-demo: init-env
	docker compose up --build -d
	docker compose exec api python scripts/run_demo.py

up: init-env
	docker compose up --build -d

down:
	docker compose down

demo:
	docker compose exec api python scripts/run_demo.py

local-demo:
	python scripts/run_demo.py

test:
	python -m pytest -q

lint:
	ruff check .
