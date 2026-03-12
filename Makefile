PYTHON ?= python

install:
	$(PYTHON) -m pip install -e .[dev]

test:
	pytest

run:
	uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

migrate:
	alembic upgrade head

makemigrations:
	alembic revision --autogenerate -m "$(m)"

discover-run:
	python -m app.cli discover-latest-run --provider geoglows

ingest-sample:
	python -m app.cli ingest-return-periods --provider geoglows --reach-id 123 --reach-id 456 && \
	python -m app.cli ingest-forecast-run --provider geoglows --run-id latest --reach-id 123 --reach-id 456

summarize-sample:
	python -m app.cli summarize-run --provider geoglows --run-id latest


migrate-docker:
	docker compose exec app python -m alembic upgrade head

smoke-geoglows:
	python -m app.cli smoke-geoglows --river-id 123456789
