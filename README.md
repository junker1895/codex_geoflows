# GeoFlows Forecast Service (Backend + Frontend)

Internal developer documentation for the current state of this repository.

This project now supports **two forecast providers in the same stack**:

- **GeoGloWS** (`geoglows`)
- **GloFAS** (`glofas`)

The frontend includes a provider toggle and can query either provider through the same `/forecast/*` API surface.

---

## 1) Current capabilities (implemented)

### Backend

- FastAPI service with provider-agnostic endpoints under `/forecast`.
- Provider adapter architecture (`ForecastProviderAdapter`) with both providers registered when enabled.
- PostgreSQL persistence for:
  - runs,
  - return period thresholds,
  - timeseries,
  - reach summaries.
- Run-readiness lifecycle and status endpoints (`discovered` → `map_ready`).
- Bulk artifact workflows for production-scale ingest.

### Providers

#### GeoGloWS

- Latest run discovery.
- Bulk summary preparation from public forecast Zarr.
- Return period import from public GEOGLOWS retrospective Zarr.
- On-demand reach detail fallback from forecast Zarr when timeseries rows are absent.

#### GloFAS

- Latest run discovery.
- CDS GRIB bulk acquisition.
- Reach-to-grid mapping via crosswalk table (`reach_grid_crosswalk`).
- Bulk record + bulk summary iteration from staged GRIB.
- Return period import pipeline via:
  - official NetCDF threshold files (preferred),
  - precomputed threshold table,
  - reanalysis-derived method.

### Frontend

- Vite + MapLibre + PMTiles + Chart.js app in `frontend/`.
- Provider toggle (`GeoGloWS` / `GloFAS`) in UI.
- Fetches:
  - latest run by provider,
  - map severity by provider,
  - reach detail by provider.

---

## 2) Docker-first local development

> This project should be operated Docker-first for backend + database.

### 2.1 Prerequisites

- Docker + Docker Compose
- Node.js (only needed to run local frontend dev server)

### 2.2 Backend startup

1. Copy environment template:

```bash
cp .env.example .env
```

2. Update `.env` for your environment (see config sections below).

3. Build and start backend + Postgres:

```bash
docker compose up --build
```

Backend is available on `http://localhost:8000` by default.

> Port overrides:
>
> - API host port: `APP_PORT` (default `8000`)
> - Postgres host port: `POSTGRES_HOST_PORT` (default `5433`, chosen to avoid common local 5432 collisions)
>
> Example:
>
> ```bash
> APP_PORT=8100 POSTGRES_HOST_PORT=55432 docker compose up --build
> ```

If Compose still reports a bind to `:5432` or `:8000`, verify effective config + remove stale containers:

```bash
APP_PORT=8100 POSTGRES_HOST_PORT=55432 docker compose config | rg "published|target"
docker compose down --remove-orphans
APP_PORT=8100 POSTGRES_HOST_PORT=55432 docker compose up --build
```

### 2.3 Migrations (inside Docker)

```bash
docker compose exec app python -m alembic upgrade head
```

### 2.4 Frontend startup (local dev)

Run frontend separately (it proxies `/forecast` to backend at `localhost:8000`):

```bash
cd frontend
npm install
npm run dev
```

Frontend dev server is configured for port `4173`.

---

## 3) Provider enablement and secret handling

Use `.env` for all secrets. Do **not** hardcode credentials in docs or code.

### 3.1 Enable both providers

Set provider flags in `.env`:

- `FORECAST_ENABLED_PROVIDERS` should include both `geoglows,glofas`.
- `GEOGLOWS_ENABLED=true`
- `GLOFAS_ENABLED=true`

### 3.2 GloFAS CDS credentials

For CDS-backed GloFAS acquisition, configure the secret token in `.env`:

- `GLOFAS_CDS_KEY`

Without a CDS key, GloFAS bulk acquisition support is intentionally disabled by the provider adapter.

---

## 4) Core CLI workflows

All commands run from repository root.

### 4.1 Run discovery

```bash
python -m app.cli discover-latest-run --provider geoglows
python -m app.cli discover-latest-run --provider glofas
```

### 4.2 Crosswalk build (required for GloFAS reach mapping)

```bash
python -m app.cli build-crosswalk --provider glofas
```

### 4.3 Return period imports

GeoGloWS:

```bash
python -m app.cli import-geoglows-return-periods-zarr
```

GloFAS (choose one source mode):

```bash
python -m app.cli import-glofas-return-periods --netcdf-dir <path>
# or
python -m app.cli import-glofas-return-periods --threshold-path <path>
# or
python -m app.cli import-glofas-return-periods --reanalysis-path <path>
```

### 4.4 Bulk artifacts and ingest

GeoGloWS or GloFAS raw artifact:

```bash
python -m app.cli prepare-bulk-artifact --provider <geoglows|glofas> --run-id latest
python -m app.cli ingest-forecast-run --provider <geoglows|glofas> --run-id latest --mode bulk
```

Summary-first workflow (map-oriented):

```bash
python -m app.cli prepare-bulk-summaries --provider <geoglows|glofas> --run-id latest
python -m app.cli ingest-forecast-summaries --provider <geoglows|glofas> --run-id latest --replace-existing
```

### 4.5 Run status and health

```bash
python -m app.cli run-status --provider <geoglows|glofas> --run-id latest
curl "http://localhost:8000/forecast/health?provider=geoglows"
curl "http://localhost:8000/forecast/health?provider=glofas"
```

---

## 5) API endpoints used by frontend

- `GET /forecast/runs/latest?provider=<provider>`
- `GET /forecast/map/severity?provider=<provider>&run_id=<run_id>&min_severity_score=<n>`
- `GET /forecast/reaches/<provider>/<reach_id>?run_id=<run_id>&timeseries_limit=<n>`

Additional operational endpoints:

- `GET /forecast/providers`
- `GET /forecast/map/reaches`
- `GET /forecast/summary`
- `GET /forecast/runs/{provider}/{run_id}/status`

---

## 6) What changed vs older docs

This repository has moved beyond an older GeoGloWS-only phase.

Implemented changes now reflected in docs:

- GloFAS adapter is implemented and wired.
- Crosswalk table + migrations are present.
- GloFAS return-period import paths are implemented.
- Frontend provider toggle exists and uses provider-specific API queries.

Some workflows remain operationally heavy and should still be treated as production-in-progress (especially global-scale runs), but the provider integration itself is now real and active.

---

## 7) Testing

Run project tests:

```bash
pytest
```

Targeted checks:

```bash
pytest tests/test_geoglows_provider.py tests/test_forecast_service.py tests/test_api_forecast.py
```

---

## 8) Repository map

- `app/api/` — FastAPI routes + dependency wiring
- `app/forecast/` — provider interfaces, providers, classification, jobs, service
- `app/db/` — SQLAlchemy models/repositories
- `alembic/` — migrations
- `frontend/` — Vite/MapLibre frontend
- `docs/` — implementation and classification design notes
