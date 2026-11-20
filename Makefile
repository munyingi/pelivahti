.PHONY: setup run test lint dashboard dagster kafka-up kafka-run kafka-down clean

PY ?= python
BOOTSTRAP ?= localhost:19092

setup:            ## install everything into the active virtualenv
	$(PY) -m pip install -e ".[dashboard,orchestration,dev]"

run:              ## whole pipeline without Kafka: generate -> Delta bronze -> stream -> dbt
	pelivahti run-local --fresh

test:             ## unit and recovery tests (Kafka tests skip without a broker)
	pytest -q

lint:
	ruff check . && ruff format --check .

dashboard:
	streamlit run dashboard/app.py

dagster:          ## Dagster UI on http://localhost:3000
	dagster dev -m orchestration.definitions

kafka-up:         ## start Redpanda + Console (http://localhost:8080)
	docker compose up -d --wait

kafka-run:        ## the same pipeline through a real broker
	pelivahti reset
	pelivahti generate
	pelivahti produce --bootstrap $(BOOTSTRAP)
	pelivahti land --bootstrap $(BOOTSTRAP)
	pelivahti stream --bootstrap $(BOOTSTRAP)
	pelivahti dbt build

kafka-test:
	KAFKA_BOOTSTRAP=$(BOOTSTRAP) pytest -q -m kafka

kafka-down:
	docker compose down -v

clean:
	pelivahti reset
	rm -rf dbt/target dbt/logs
