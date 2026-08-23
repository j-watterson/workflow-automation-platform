.PHONY: validate test init up down logs dag-list dag-test

validate:
	python3 scripts/validate_project.py

test:
	./scripts/run_tests.sh

init:
	docker compose up airflow-init

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs --follow airflow-scheduler

dag-list:
	docker compose exec airflow-scheduler airflow dags list

dag-test:
	docker compose exec airflow-scheduler airflow dags test \
		northwind_daily_warehouse_refresh 2026-07-29

