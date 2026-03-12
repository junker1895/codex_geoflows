# GeoFlows Forecast Ingestion Service

Backend-only, provider-agnostic flood forecast ingestion and API service. This first version ships with a GEOGLOWS provider adapter and stores provider-native reach forecasts for downstream flood workflows.

## What this service does

- Discovers latest forecast runs (provider-specific adapter)
- Ingests return period thresholds for selected provider-native reach IDs
- Ingests forecast timeseries for selected provider-native reach IDs
- Computes reach-level severity summaries
- Persists runs, return periods, timeseries, and summaries in PostgreSQL
- Exposes a FastAPI HTTP API for provider-native forecast retrieval
- Provides CLI jobs for ingestion/summarization orchestration

## What this service does NOT do yet

- HydroRIVERS crosswalk
- Flood extent/inundation generation
- Frontend/map rendering
- PMTiles integration
- Distributed job orchestration

## Architecture overview

- `app/core`: settings, logging, DB engine
- `app/db`: SQLAlchemy models and repositories
- `app/forecast/base.py`: provider contract
- `app/forecast/providers/geoglows.py`: GEOGLOWS adapter
- `app/forecast/classify.py`: pure classification logic
- `app/forecast/service.py`: orchestration/service layer
- `app/api`: API routes and dependencies
- `app/forecast/jobs` + `app/cli.py`: batch entrypoints

## Project structure

See `app/` for modular layers and `tests/` for coverage across classification, provider normalization, service, and API.

## Local setup

1. Copy env file:

```bash
cp .env.example .env
```

2. Install:

```bash
make install
```

3. Start Postgres + app with Docker:

```bash
docker compose up --build
```

> Note: the app container uses `DATABASE_URL=...@postgres...` internally via `docker-compose.yml`, while host-side commands should use `.env` with `localhost`.

## Environment variables

- `APP_ENV`
- `APP_HOST`
- `APP_PORT`
- `LOG_LEVEL`
- `DATABASE_URL`
- `FORECAST_DEFAULT_PROVIDER`
- `FORECAST_ENABLED_PROVIDERS`
- `GEOGLOWS_ENABLED`
- `GEOGLOWS_SOURCE_TYPE`
- `GEOGLOWS_DEFAULT_RUN_SELECTOR`
- `GEOGLOWS_REQUEST_TIMEOUT_SECONDS`
- `GEOGLOWS_DATA_SOURCE`
- `FORECAST_SUMMARY_DEFAULT_LIMIT`

## Migrations

If you run migrations from your **host machine**, ensure `.env` uses a host-reachable DB URL (default in `.env.example` uses `localhost`).

```bash
python -m alembic upgrade head
```

If you run migrations from inside Docker (recommended when app is containerized):

```bash
docker compose exec app python -m alembic upgrade head
```

## Running API

```bash
make run
```

## Running CLI jobs

```bash
python -m app.cli discover-latest-run --provider geoglows
python -m app.cli ingest-return-periods --provider geoglows --reach-id 123 --reach-id 456
python -m app.cli ingest-forecast-run --provider geoglows --run-id latest --reach-id 123 --reach-id 456
python -m app.cli summarize-run --provider geoglows --run-id latest
python -m app.cli smoke-geoglows --river-id 123456789
```

## Tests

```bash
make test
```

Provider health responses include capability flags such as `supports_forecast_stats_rest` and `supports_return_periods_current_backend` to make backend availability explicit.

## Current limitations

- GEOGLOWS run discovery currently uses deterministic local-hour run ID fallback and is designed to be replaced with authoritative run endpoint logic.
- `forecast_stats` is supported in REST mode (`GEOGLOWS_DATA_SOURCE=rest`) and can ingest forecasts when GEOGLOWS REST is reachable.
- `return_periods` is treated as retrospective/AWS-backed in practice; in REST mode this service fails fast with a clear operational message instead of pretending REST support.
- If retrospective/AWS access is unavailable, return-period ingest will fail and severity classification will degrade to unknown/below-threshold behavior for reaches without thresholds.
- GEOGLOWS IDs must be 9-digit numeric `river_id` values.
- Ingestion is selective by reach IDs (not full global bulk).
- No auth/rate limiting.

## Extending to future providers

1. Implement `ForecastProviderAdapter`
2. Register provider in dependency wiring
3. Reuse existing repositories/service/API contracts unchanged

## HydroRIVERS integration later

HydroRIVERS crosswalk should be added in a separate downstream service or module that maps `provider_reach_id` to HydroRIVERS IDs after ingestion. This service intentionally remains provider-native.
