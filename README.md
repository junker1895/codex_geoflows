# GeoFlows Forecast Ingestion Service

Backend-only, provider-agnostic flood forecast ingestion and API service. This first version ships with a GEOGLOWS provider adapter and stores provider-native reach forecasts for downstream flood workflows.

## What this service does

- Discovers latest forecast runs (provider-specific adapter)
- Ingests return period thresholds for selected provider-native reach IDs
- Imports local GEOGLOWS return-period datasets for offline severity classification
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
- `GEOGLOWS_RETURN_PERIOD_METHOD`
- `GEOGLOWS_RETURN_PERIOD_ZARR_PATH`
- `GEOGLOWS_RETURN_PERIOD_IMPORT_BATCH_SIZE`
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
python -m app.cli import-geoglows-return-periods-zarr
python -m app.cli import-geoglows-return-periods-zarr --method logpearson3 --batch-size 50000
python -m app.cli ingest-forecast-run --provider geoglows --run-id latest --mode rest_single --reach-id 123
python -m app.cli prepare-bulk-summaries --provider geoglows --run-id latest --filter-supported --max-blocks 20
python -m app.cli ingest-forecast-summaries --provider geoglows --run-id latest
# optional debug/detail materialization only:
python -m app.cli prepare-bulk-artifact --provider geoglows --run-id latest --filter-supported --if-present overwrite --overwrite-raw
python -m app.cli ingest-forecast-run --provider geoglows --run-id latest --mode bulk
python -m app.cli summarize-run --provider geoglows --run-id latest
python -m app.cli smoke-geoglows --river-id 123456789
```

Reach detail endpoint supports `timeseries_limit` query parameter (default 500, max 5000) to avoid oversized responses.

REST mode is debug/smoke only for one or small reach sets. Production bulk uses summary generation from public GEOGLOWS run Zarr (`prepare-bulk-summaries` + `ingest-forecast-summaries`) and intentionally does not materialize full-network per-timestep rows.
`--mode bulk` and `--reach-id` are intentionally mutually exclusive to prevent accidental fallback semantics.


## Run readiness and map-ready semantics

The backend now exposes explicit, provider-agnostic run lifecycle stages:

- `discovered`
- `raw_acquired`
- `artifact_prepared`
- `ingested`
- `summarized`
- `map_ready`

These stages are tracked in run operational metadata (`forecast_runs.metadata_json.ops`) with stage completion, failure stage/message, and last-updated timestamps.

### Map-ready definition

A run is `map_ready=true` only when all of the following are true:

1. Run exists.
2. Artifact source is prepared (`artifact_prepared`; normalized artifact exists).
3. Timeseries rows exist for the run.
4. Summary rows exist for the run.
5. `/forecast/map/reaches` can serve rows for the run (same summary row backing table).

The same rule is used by:

- `GET /forecast/health`
- `GET /forecast/runs/{provider}/{run_id}/status`
- `python -m app.cli run-status`

### Operational status fields

`GET /forecast/health?provider=...` keeps existing fields and now includes latest-run operational fields:

- `latest_run_artifact_exists`
- `latest_run_artifact_row_count`
- `latest_run_timeseries_row_count`
- `latest_run_summary_count`
- `latest_run_map_count`
- `latest_run_status`
- `latest_run_missing_stages`
- `latest_run_map_ready`
- `latest_run_failure_stage`
- `latest_run_failure_message`

Detailed run readiness endpoint:

```bash
curl "http://localhost:8000/forecast/runs/geoglows/latest/status"
```

### Operational CLI commands

Inspect artifact:

```bash
python -m app.cli inspect-run-artifact --provider geoglows --run-id latest
python -m app.cli inspect-run-artifact --provider geoglows --run-id latest --preview-limit 3
```

Print run readiness summary:

```bash
python -m app.cli run-status --provider geoglows --run-id latest

# Inspect forecast Zarr structure/chunking for a run
python -m app.cli forecast-zarr-inspect --run-id latest
```

### Failure tracking

Failures are explicitly tracked by stage with a short message for:

- raw acquisition
- normalization/artifact preparation
- bulk ingest
- summarization

Use `run-status` or `/forecast/runs/{provider}/{run_id}/status` to inspect `failure_stage` and `failure_message`.

### Rerun and idempotency notes

- `prepare-bulk-artifact` supports `--if-present skip|overwrite|error`.
  - `skip`: preserves existing artifact.
  - `overwrite`: rewrites artifact.
  - `error`: fails if artifact exists.
- `ingest-forecast-run --mode bulk` is upsert-based and safe to rerun from the same artifact.
- `summarize-run` is upsert-based and safe to rerun after ingest.
- In operational practice, run `summarize-run` after each ingest refresh to keep map rows current.


## Tests

```bash
make test
```

Provider health responses include capability flags such as `supports_forecast_stats_rest`, `supports_return_periods_current_backend`, and `local_return_periods_available` to make backend availability explicit.

## Current limitations

- GEOGLOWS run discovery uses authoritative public bucket listing (`YYYYMMDD00.zarr`) and falls back to `YYYYMMDD00` only if listing fails.
- `forecast_stats` REST mode is debug-only (`--mode rest_single`) for one/small explicit reaches.
- `return_periods` is treated as retrospective/AWS-backed in practice; in REST mode this service fails fast with a clear operational message instead of pretending REST support.
- If retrospective/AWS access is unavailable, return-period ingest will fail and severity classification will degrade to unknown/below-threshold behavior for reaches without thresholds.
- GEOGLOWS IDs must be 9-digit numeric `river_id` values.
- Bulk ingestion uses the supported-reach universe already loaded in `forecast_provider_return_periods` (typically from GEOGLOWS Zarr import), with configurable chunking via `FORECAST_BULK_INGEST_BATCH_SIZE`.
- No auth/rate limiting.

## Extending to future providers

1. Implement `ForecastProviderAdapter`
2. Register provider in dependency wiring
3. Reuse existing repositories/service/API contracts unchanged

## HydroRIVERS integration later

HydroRIVERS crosswalk should be added in a separate downstream service or module that maps `provider_reach_id` to HydroRIVERS IDs after ingestion. This service intentionally remains provider-native.


## Currently verified workflow

1. `python -m alembic upgrade head`
2. `python -m app.cli discover-latest-run --provider geoglows`
3. Optional debug smoke: `python -m app.cli ingest-forecast-run --provider geoglows --run-id latest --mode rest_single --reach-id 760021611`
4. Prepare map-summary artifact from public GEOGLOWS run Zarr: `python -m app.cli prepare-bulk-summaries --provider geoglows --run-id latest --filter-supported --max-blocks 20`
5. Ingest summary artifact: `python -m app.cli ingest-forecast-summaries --provider geoglows --run-id latest`
6. `/forecast/map/reaches` is now served from the summary table only (lightweight).
7. `curl "http://localhost:8000/forecast/reaches/geoglows/760021611?timeseries_limit=50"` (on-demand detail extraction from run Zarr if timeseries rows are absent).

Return-period ingest can run from the verified GEOGLOWS Zarr object store path for full severity classification in REST-only forecast environments.


## Local GEOGLOWS return-period import

The service imports GEOGLOWS return periods directly from the verified dataset path:

- `s3://geoglows-v2/retrospective/return-periods.zarr`

Dataset shape used by the importer:

- dims: `river_id`, `return_period`
- `return_period` coordinate values: `2, 5, 10, 25, 50, 100`
- data variables: `gumbel`, `logpearson3`, `max_simulated`

Supported methods:

- `gumbel` (default)
- `logpearson3`

Default method can be controlled by `GEOGLOWS_RETURN_PERIOD_METHOD` and the Zarr path by `GEOGLOWS_RETURN_PERIOD_ZARR_PATH`.

Import command (direct object path read, no bucket listing required):

```bash
python -m app.cli import-geoglows-return-periods-zarr
```

Optional overrides:

```bash
python -m app.cli import-geoglows-return-periods-zarr \
  --zarr-path s3://geoglows-v2/retrospective/return-periods.zarr \
  --method gumbel \
  --batch-size 10000
```

The importer reads `river_id` in batches (chunked), reshapes one reach per row into `rp_2/rp_5/rp_10/rp_25/rp_50/rp_100`, and upserts into `forecast_provider_return_periods` keyed by `provider=geoglows + provider_reach_id`.

Summary response shape remains unchanged. Before thresholds are loaded:

```json
{
  "return_period_band": "unknown",
  "severity_score": 0,
  "is_flagged": false
}
```

After thresholds are loaded and summarize-run is executed, the same fields are populated (example):

```json
{
  "return_period_band": "5",
  "severity_score": 2,
  "is_flagged": true
}
```

## Flood classification technical reference

For the detailed model-agnostic flood classification design (architecture, thresholds, peak extraction, banding, API mapping, and multi-model integration guidance), see:

- `docs/flood-classification-system.md`


### Production bulk workflow

The production bulk pipeline is intentionally split into three layers:

1. **Provider acquisition layer**: discover upstream runs and read official GEOGLOWS public forecast Zarr.
2. **Normalization/export layer**: convert provider-native records into normalized bulk artifact rows.
3. **Ingest layer**: load normalized artifact rows into `forecast_provider_reach_timeseries`.

Current normalized artifact format is JSONL with schema fields:

- `provider` (string, required)
- `run_id` (string, required)
- `provider_reach_id` (string, required)
- `forecast_time_utc` (datetime, required)
- `flow_mean_cms`, `flow_median_cms`, `flow_p25_cms`, `flow_p75_cms`, `flow_max_cms` (float, optional)
- `raw_payload_json` (object, optional)

For production mode, the artifact is the bridge between provider-native bulk acquisition and DB ingest.

GEOGLOWS preparation is optimized for the real upstream layout by reading contiguous `rivid` blocks aligned to Zarr chunk windows (observed upstream `Qout` chunking: `(52, 280, 686)`) and computing statistics vectorized over ensemble values. This avoids per-reach random-access patterns that can appear stalled on global runs.

The prepare job logs block progress at INFO level (`run_id`, source Zarr path, block index/total, `rivid` start/end, rows written, elapsed seconds, and rows/sec) so long-running runs show clear forward progress.


## GEOGLOWS bulk acquisition modes

Production mode is `aws_public_zarr` and reads official GEOGLOWS upstream runs from:

- `s3://geoglows-v2-forecasts/`
- region: `us-west-2`
- anonymous (`no-sign-request`)
- one Zarr per run: `YYYYMMDD00.zarr/`
- forecast variable: `Qout` (confirmed dims: `ensemble, time, rivid`)

Acquisition is controlled by `GEOGLOWS_BULK_ACQUISITION_MODE`:

- `aws_public_zarr` (default, production): discover latest run from bucket object names and read run Zarr directly.
- `manual_artifact_only`: backend does not acquire raw data; operators provide normalized artifact directly.
- `local_file`: backend stages a local raw JSONL file from `GEOGLOWS_BULK_RAW_SOURCE_URI`.
- `remote_http`: backend downloads raw JSONL from HTTP(S) URL in `GEOGLOWS_BULK_RAW_SOURCE_URI` (supports `{run_id}` templating).

Key production config:

- `GEOGLOWS_BULK_ACQUISITION_MODE=aws_public_zarr`
- `GEOGLOWS_FORECAST_BUCKET=geoglows-v2-forecasts`
- `GEOGLOWS_FORECAST_REGION=us-west-2`
- `GEOGLOWS_FORECAST_USE_ANON=true`
- `GEOGLOWS_FORECAST_VARIABLE=Qout`
- `GEOGLOWS_FORECAST_RUN_SUFFIX=.zarr`

Artifact/ingest config:

- `FORECAST_BULK_ARTIFACT_DIR`
- `FORECAST_BULK_ARTIFACT_WRITE_BATCH_SIZE`
- `FORECAST_BULK_INGEST_BATCH_SIZE`
- `FORECAST_BULK_ARTIFACT_RETENTION_RUNS`

Normalized artifact row mapping (`BulkForecastArtifactRowSchema`):

Required raw source fields:

- `provider_reach_id` (or `river_id`)
- `forecast_time_utc` (or `time`)

Optional raw fields:

- `flow_avg` or `flow_mean_cms` -> `flow_mean_cms`
- `flow_med` or `flow_median_cms` -> `flow_median_cms`
- `flow_25p` or `flow_p25_cms` -> `flow_p25_cms`
- `flow_75p` or `flow_p75_cms` -> `flow_p75_cms`
- `flow_max` or `flow_max_cms` -> `flow_max_cms`

Timestamp parsing uses ISO datetime conversion; invalid/missing rows are dropped during prepare and counted in logs. Raw provider record is preserved in `raw_payload_json.raw_record`.

Run-scoped paths are deterministic:

- raw staging: `${GEOGLOWS_BULK_STAGING_DIR}/geoglows/geoglows_{run_id}.jsonl`
- summary artifact (Parquet default): `${FORECAST_BULK_ARTIFACT_DIR}/geoglows/run_id={run_id}/part-000.parquet`

Prepare behavior when artifact exists is controlled with CLI `--if-present skip|overwrite|error`.


Production note: full-network per-timestep materialization is intentionally not used at global GEOGLOWS scale because `(reach × timestep)` expansion is not operationally viable; only one summary row per reach is bulk-produced.


## Safe local GEOGLOWS summary-first commands

```bash
python -m alembic upgrade head
python -m app.cli discover-latest-run --provider geoglows
python -m app.cli prepare-bulk-summaries --provider geoglows --run-id latest --filter-supported --max-blocks 20
python -m app.cli inspect-summary-artifact-schema --provider geoglows --run-id latest
python -m app.cli ingest-forecast-summaries --provider geoglows --run-id latest --replace-existing
python -m app.cli run-status --provider geoglows --run-id latest
python -m app.cli cleanup-forecast-cache
```

## Full production-style explicit run

```bash
python -m alembic upgrade head
python -m app.cli discover-latest-run --provider geoglows
python -m app.cli prepare-bulk-summaries --provider geoglows --run-id latest --filter-supported --full-run
python -m app.cli inspect-summary-artifact-schema --provider geoglows --run-id latest
python -m app.cli ingest-forecast-summaries --provider geoglows --run-id latest --replace-existing
python -m app.cli run-status --provider geoglows --run-id latest
```

Key runtime settings:
- `FORECAST_ENVIRONMENT=local|production`
- `FORECAST_CACHE_DIR`
- `FORECAST_CACHE_MAX_GB`
- `FORECAST_CLEANUP_CACHE_AFTER_RUN`
- `FORECAST_DEFAULT_MAX_REACHES`
- `FORECAST_DEFAULT_MAX_BLOCKS`
- `FORECAST_DEFAULT_MAX_SECONDS`

`/forecast/reaches/{provider}/{provider_reach_id}` remains on-demand from public Zarr (`Qout(ensemble,time,rivid)`) when timeseries rows are not materialized, and `ingest-forecast-run --mode rest_single` remains debug-only.


### Stable container acceptance mode

For final backend acceptance in Docker, run the app without auto-reload to avoid transient worker restarts during request checks.

```bash
docker compose build app
docker compose up -d app
docker compose logs -f app
```
