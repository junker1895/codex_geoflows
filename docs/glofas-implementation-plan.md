# GloFAS Integration — Implementation Status (Updated)

This document replaces the original "plan-only" draft with the **current implementation state**.

## Summary

GloFAS has been integrated as a second provider alongside GeoGloWS in this repository.

What is now in place:

- Provider registration + runtime enablement via settings.
- `GlofasForecastProvider` adapter implementation.
- CDS download wrapper for forecast and reanalysis access.
- Reach-grid crosswalk table and migrations.
- Crosswalk build CLI command.
- GloFAS return-period import CLI and service pipeline.
- Frontend provider toggle with provider-aware API requests.

---

## 1) Implemented backend components

### 1.1 Provider registration

`ForecastService` wiring now supports both providers when enabled:

- `geoglows`
- `glofas`

This is active in both API dependency wiring and CLI service construction.

### 1.2 GloFAS adapter (`app/forecast/providers/glofas.py`)

Implemented capabilities include:

- `discover_latest_run()`
- `fetch_forecast_timeseries()` from staged GRIB + crosswalk mapping
- `summarize_reach()` using shared classifier
- bulk acquisition hooks:
  - `supports_bulk_acquisition()`
  - `bulk_acquisition_mode()`
  - `acquire_bulk_raw_source()`
  - `iter_raw_bulk_records()`
  - `iter_bulk_summary_records()`

Implementation note:

- `fetch_return_periods()` is intentionally not on-demand for GloFAS and raises backend-unavailable guidance to use offline import.

### 1.3 CDS wrapper (`app/forecast/providers/glofas_cds.py`)

Implemented helpers support:

- forecast downloads via CDS API,
- reanalysis downloads,
- GRIB open helpers.

### 1.4 Crosswalk table + schema evolution

Migration-backed table exists:

- `reach_grid_crosswalk`

Migrations include:

- initial table creation,
- expanded diagnostics fields for hydrologic matching quality.

### 1.5 Crosswalk generation (`app/forecast/providers/glofas_crosswalk.py`)

A full build pipeline exists and persists entries into `reach_grid_crosswalk`, with scoring and diagnostics.

CLI entrypoint:

```bash
python -m app.cli build-crosswalk --provider glofas
```

### 1.6 GloFAS return periods (`app/forecast/providers/glofas_return_periods.py`)

Implemented import sources:

- official GloFAS v4 threshold NetCDF directory (preferred),
- precomputed threshold table,
- reanalysis-derived extraction.

Service method:

- `ForecastService.import_glofas_return_periods(...)`

CLI entrypoint:

```bash
python -m app.cli import-glofas-return-periods --netcdf-dir <path>
```

(Alternative source flags are also supported.)

---

## 2) Frontend integration status

Provider switch is implemented in `frontend/index.html` + `frontend/src/main.js`.

Current frontend behavior:

- User selects GeoGloWS or GloFAS.
- Frontend requests latest run for selected provider.
- Frontend reloads severity map and reach detail scoped to selected provider.
- Hydrograph panel uses provider-specific detail payload.

---

## 3) Operational workflow (current)

For GloFAS end-to-end operation:

1. Build crosswalk.
2. Import GloFAS return periods.
3. Discover run.
4. Prepare bulk artifact or summary artifact.
5. Ingest forecast rows or summary rows.
6. Serve via `/forecast/*` endpoints and frontend toggle.

Representative command sequence:

```bash
python -m app.cli build-crosswalk --provider glofas
python -m app.cli import-glofas-return-periods --netcdf-dir <path>
python -m app.cli discover-latest-run --provider glofas
python -m app.cli prepare-bulk-summaries --provider glofas --run-id latest
python -m app.cli ingest-forecast-summaries --provider glofas --run-id latest --replace-existing
python -m app.cli run-status --provider glofas --run-id latest
```

---

## 4) Differences from the original plan

The original document described a future roadmap. Current implementation differs in a few details:

- Return period handling is import-first (not on-demand fetch) for GloFAS.
- Crosswalk diagnostics were expanded beyond the original minimal table fields.
- Bulk summary workflows are emphasized for map-scale operational use.
- Frontend provider switch is now implemented (not planned).

---

## 5) Remaining production-hardening items

Even though core integration is implemented, typical hardening still applies:

- performance tuning for full global runs,
- operator runbooks for data refresh/recovery,
- monitoring and alerting around run lifecycle stages,
- stricter production secrets and credential rotation processes.

These are operations concerns rather than missing core integration features.
