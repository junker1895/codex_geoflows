# GloFAS Integration — Implementation Plan

## Goal

Add GloFAS (Global Flood Awareness System) as a second forecast provider alongside
GeoGloWS, using the same PMTiles river network. Users can switch between the two
forecast sources on the map and in the hydrograph panel.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│  Frontend (MapLibre + Chart.js)                              │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  Provider toggle: [GeoGloWS] [GloFAS]                   │ │
│  │  Same PMTiles rivers → severity overlay per provider     │ │
│  │  Click reach → hydrograph from selected provider         │ │
│  └─────────────────────────────────────────────────────────┘ │
├──────────────────────────────────────────────────────────────┤
│  API (FastAPI) — already provider-parameterized              │
│  GET /forecast/map/severity?provider=glofas                  │
│  GET /forecast/reaches/glofas/{reach_id}                     │
├──────────────────────────────────────────────────────────────┤
│  Service Layer — provider adapter pattern (no changes)       │
├──────────────────────────────────────────────────────────────┤
│  GloFAS Provider Adapter (NEW)                               │
│  ├── CDS API download (GRIB → xarray)                        │
│  ├── Grid-to-reach crosswalk lookup                          │
│  └── Return periods from GloFAS reanalysis                   │
├──────────────────────────────────────────────────────────────┤
│  Database — same tables, scoped by provider="glofas"         │
│  ┌──────────────────────────────────────────────────────────┐│
│  │ forecast_runs              (provider="glofas")           ││
│  │ forecast_provider_return_periods  (provider="glofas")    ││
│  │ forecast_provider_reach_timeseries (provider="glofas")   ││
│  │ forecast_provider_reach_summaries  (provider="glofas")   ││
│  └──────────────────────────────────────────────────────────┘│
├──────────────────────────────────────────────────────────────┤
│  NEW TABLE: reach_grid_crosswalk                             │
│  Maps GeoGloWS reach_id → GloFAS grid cell (lat, lon)        │
└──────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Reach-to-Grid Crosswalk Table

**Why this is the critical piece:** GeoGloWS uses vector reaches with unique IDs
(matching PMTiles features). GloFAS uses a 0.05° grid. To show GloFAS data on
GeoGloWS rivers, we need a mapping from each reach ID to the nearest GloFAS grid
cell.

### Step 1.1 — New DB table + migration

```python
# app/db/models.py
class ReachGridCrosswalk(Base):
    __tablename__ = "reach_grid_crosswalk"
    __table_args__ = (
        UniqueConstraint("reach_id", "target_provider", name="uq_crosswalk_reach_provider"),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    reach_id: Mapped[str]           # GeoGloWS reach ID (matches PMTiles feature ID)
    target_provider: Mapped[str]    # "glofas"
    grid_lat: Mapped[float]         # nearest GloFAS grid cell latitude
    grid_lon: Mapped[float]         # nearest GloFAS grid cell longitude
    upstream_area_km2: Mapped[float | None]  # for quality filtering
    distance_km: Mapped[float | None]        # snap distance for QA
```

### Step 1.2 — Build the crosswalk (one-time CLI command)

1. Download GeoGloWS metadata table from S3:
   `s3://geoglows-v2/tables/package-metadata-table.parquet` (~250 MB)
   Contains reach ID + centroid lat/lon for all ~7M reaches.

2. Build a GloFAS river grid mask from the reanalysis:
   Download one timestep of `cems-glofas-historical` to get the discharge grid.
   Cells with upstream area > threshold are "river cells."

3. For each GeoGloWS reach centroid, snap to the nearest GloFAS river grid cell.
   Use a KD-tree for efficient spatial lookup.

4. Store mappings in `reach_grid_crosswalk` with `target_provider="glofas"`.

5. Filter: only keep mappings where snap distance < 10 km and upstream areas
   are within a reasonable ratio (avoids mapping a tributary to a main stem).

**Files to create:**
- `app/forecast/providers/glofas_crosswalk.py` — crosswalk builder
- `alembic/versions/0004_add_reach_grid_crosswalk.py` — migration
- CLI command: `python -m app.cli build-crosswalk --provider glofas`

---

## Phase 2: GloFAS Provider Adapter

### Step 2.1 — CDS API client wrapper

```python
# app/forecast/providers/glofas_cds.py

import cdsapi
import xarray as xr

def download_glofas_forecast(date: str, leadtimes: list[int], area: list[float] | None,
                             target_path: str, data_format: str = "grib") -> str:
    """Download GloFAS forecast GRIB from EWDS CDS API."""
    client = cdsapi.Client()
    client.retrieve("cems-glofas-forecast", {
        "system_version": "operational",
        "hydrological_model": "lisflood",
        "product_type": ["control_forecast", "ensemble_perturbed_forecasts"],
        "variable": "river_discharge_in_the_last_24_hours",
        "year": date[:4],
        "month": date[4:6],
        "day": date[6:8],
        "leadtime_hour": [str(h) for h in leadtimes],
        "area": area,  # [N, W, S, E] or None for global
        "data_format": data_format,
    }, target_path)
    return target_path

def open_glofas_forecast(path: str) -> xr.Dataset:
    """Open downloaded GRIB as xarray Dataset."""
    return xr.open_dataset(path, engine="cfgrib")
```

**Config additions (`app/core/config.py`):**
```python
glofas_enabled: bool = Field(default=False, alias="GLOFAS_ENABLED")
glofas_cds_url: str = Field(default="https://ewds.climate.copernicus.eu/api", alias="GLOFAS_CDS_URL")
glofas_cds_key: str | None = Field(default=None, alias="GLOFAS_CDS_KEY")
glofas_system_version: str = Field(default="operational", alias="GLOFAS_SYSTEM_VERSION")
glofas_bulk_staging_dir: str = Field(default="./data/glofas_raw", alias="GLOFAS_BULK_STAGING_DIR")
glofas_grid_resolution: float = Field(default=0.05, alias="GLOFAS_GRID_RESOLUTION")
glofas_crosswalk_min_upstream_area_km2: float = Field(default=500.0, alias="GLOFAS_CROSSWALK_MIN_UPSTREAM_AREA")
glofas_crosswalk_max_snap_distance_km: float = Field(default=10.0, alias="GLOFAS_CROSSWALK_MAX_SNAP_DISTANCE")
```

### Step 2.2 — GloFAS provider adapter

```python
# app/forecast/providers/glofas.py

class GlofasForecastProvider(ForecastProviderAdapter):
    """GloFAS forecast provider using CDS API + reach-grid crosswalk."""

    def get_provider_name(self) -> str:
        return "glofas"

    def discover_latest_run(self) -> ForecastRunSchema:
        # GloFAS publishes daily; latest = yesterday (processing delay)
        # Check CDS API for most recent available date
        ...

    def fetch_return_periods(self, reach_ids: list[str | int]) -> list[ReturnPeriodSchema]:
        # Look up crosswalk → get grid cells for these reaches
        # Read return periods from pre-computed GloFAS reanalysis thresholds
        # GloFAS uses 2, 5, 20-year RPs (map to rp_2, rp_5, rp_25 in our schema)
        ...

    def fetch_forecast_timeseries(self, run_id, reach_ids) -> list[TimeseriesPointSchema]:
        # Look up crosswalk → grid cells
        # Extract timeseries from downloaded GRIB for those cells
        # Compute ensemble stats (mean, median, p25, p75, max) from 51 members
        ...

    def summarize_reach(self, run_id, reach_id, timeseries, return_periods) -> ReachSummarySchema:
        # Same logic as GeoGloWS — peak detection + classification
        # Reuse classify_peak_flow() from app/forecast/classify.py
        ...

    # Bulk methods
    def supports_bulk_acquisition(self) -> bool:
        return True

    def bulk_acquisition_mode(self) -> str:
        return "cds_grib"

    def acquire_bulk_raw_source(self, run_id, overwrite=False) -> str:
        # Download full global GRIB for this run date
        # All leadtimes (24h to 720h in 24h steps = 30 days)
        ...

    def iter_bulk_summary_records(self, run_id, ...) -> Iterator[dict]:
        # Open GRIB, iterate over crosswalk-mapped grid cells
        # Compute ensemble stats + peak + classification per reach
        ...
```

### Step 2.3 — Return periods from GloFAS reanalysis

GloFAS computes flood thresholds by fitting a Gumbel distribution to annual maxima
from the ERA5 reanalysis (1979–present). Their standard thresholds are **2, 5, and
20-year** return periods.

**Approach:**
1. Download `cems-glofas-historical` annual maxima (or a single representative year
   to get the grid structure, then download pre-computed thresholds if available).
2. For each crosswalk grid cell, compute/store RP-2, RP-5, RP-20 thresholds.
3. Map into our schema: `rp_2=RP2, rp_5=RP5, rp_25=RP20` (closest match).
   Leave `rp_10, rp_50, rp_100` as NULL for GloFAS.

**Alternative:** ECMWF may publish pre-computed threshold grids. Check if available
via CDS to avoid recomputing from 40+ years of reanalysis.

**Files to create:**
- `app/forecast/providers/glofas.py` — main adapter
- `app/forecast/providers/glofas_cds.py` — CDS API wrapper
- `app/forecast/providers/glofas_return_periods.py` — threshold computation

---

## Phase 3: Provider Registration + CLI Commands

### Step 3.1 — Register in service layer

```python
# app/api/deps.py
from app.forecast.providers.glofas import GlofasForecastProvider

def get_forecast_service(db: Session) -> ForecastService:
    settings = get_settings()
    providers = {}
    if settings.geoglows_enabled and "geoglows" in settings.forecast_enabled_providers:
        providers["geoglows"] = GeoglowsForecastProvider(settings)
    if settings.glofas_enabled and "glofas" in settings.forecast_enabled_providers:
        providers["glofas"] = GlofasForecastProvider(settings)
    return ForecastService(db=db, settings=settings, providers=providers)
```

### Step 3.2 — CLI commands (app/cli.py)

Add GloFAS-specific commands:
- `build-crosswalk` — one-time crosswalk generation
- Existing commands (`discover-latest-run`, `prepare-bulk-artifact`,
  `ingest-forecast-run`, `prepare-bulk-summaries`) already accept `--provider`
  and will work for `glofas` once the adapter is registered.

### Step 3.3 — Environment config (.env)

```env
FORECAST_ENABLED_PROVIDERS=geoglows,glofas
GLOFAS_ENABLED=true
GLOFAS_CDS_URL=https://ewds.climate.copernicus.eu/api
GLOFAS_CDS_KEY=<your-personal-access-token>
```

---

## Phase 4: Frontend — Provider Switcher

### Step 4.1 — Provider toggle UI

Add a toggle in the top bar or legend area:

```html
<div id="provider-toggle">
  <button class="provider-btn active" data-provider="geoglows">GeoGloWS</button>
  <button class="provider-btn" data-provider="glofas">GloFAS</button>
</div>
```

### Step 4.2 — Frontend state changes

- `PROVIDER` becomes a mutable variable (currently hardcoded to `"geoglows"`)
- Switching provider:
  1. Clears `forecastIndex` and feature states
  2. Re-fetches `loadRunId()` for the new provider
  3. Re-fetches `loadSeverityMap()` for the new provider
  4. Rebuilds the highlight layer
- Click handler uses the active provider for the detail API call
- Hydrograph title shows which provider's forecast is displayed

### Step 4.3 — Dual-provider hydrograph (stretch goal)

When clicking a reach, optionally show both providers' forecasts overlaid on the
same chart for comparison. Different line styles per provider.

---

## Phase 5: Ingestion Pipeline (Production)

### Daily scheduled ingestion

```
CRON (daily ~06:00 UTC, after GloFAS publishes):

1. python -m app.cli discover-latest-run --provider glofas
2. python -m app.cli prepare-bulk-artifact --provider glofas --run-id latest
3. python -m app.cli prepare-bulk-summaries --provider glofas --run-id latest
4. python -m app.cli ingest-forecast-summaries --provider glofas --run-id latest
```

This mirrors the existing GeoGloWS pipeline. Both can run in parallel.

---

## Dependencies to Add

```toml
# pyproject.toml
cdsapi = ">=0.7.0"
cfgrib = ">=0.9.12"      # GRIB engine for xarray
eccodes = ">=2.36.0"      # ECMWF GRIB codec (cfgrib dependency)
scipy = ">=1.11.0"        # for KD-tree in crosswalk builder (may already be present)
```

---

## Data Flow Summary

```
CDS API (EWDS)                    GeoGloWS S3 (Zarr)
     │                                  │
     ▼                                  ▼
 GRIB download                    Zarr download
     │                                  │
     ▼                                  ▼
 xarray + cfgrib                  xarray
     │                                  │
     ▼                                  ▼
 Grid cell → reach ID             Reach ID (native)
 (via crosswalk table)                  │
     │                                  │
     ▼                                  ▼
 ┌──────────────────────────────────────────┐
 │  Shared DB tables                        │
 │  provider="glofas"  │  provider="geoglows"│
 └──────────────────────────────────────────┘
                    │
                    ▼
              Shared API
         (parameterized by provider)
                    │
                    ▼
        Frontend (provider toggle)
         Same PMTiles rivers
```

---

## Key Differences: GloFAS vs GeoGloWS

| Aspect              | GeoGloWS                    | GloFAS                        |
|---------------------|-----------------------------|-------------------------------|
| Grid/Vector         | Vector (reach-based)         | Grid (0.05° / ~5 km)         |
| Reach IDs           | 9-digit LINKNO              | Grid cell (lat, lon)          |
| Ensemble members    | 52                          | 51 (1 control + 50 perturbed) |
| Forecast horizon    | 15 days                     | 30 days                       |
| Return periods      | 2, 5, 10, 25, 50, 100-year | 2, 5, 20-year                 |
| Data format         | Zarr (S3)                   | GRIB (CDS API)                |
| Auth required       | No (anonymous S3)           | Yes (CDS API key)             |
| Update frequency    | Daily                       | Daily                         |
| Resolution          | ~12m DEM derived            | ~5 km grid                    |

---

## Risks and Mitigations

1. **Crosswalk quality** — Grid-to-reach snapping may be wrong in dense river
   networks. Mitigate with upstream area ratio filtering and QA visualization.

2. **CDS API rate limits** — Global downloads are large (~GB). Mitigate by
   downloading only the regions/leadtimes needed, or use area subsetting.

3. **Return period mismatch** — GloFAS has 2/5/20-yr, GeoGloWS has 2/5/10/25/50/100-yr.
   UI should indicate which thresholds are available per provider. Classification
   still works — just fewer bands for GloFAS.

4. **CDS API key management** — Needs secure storage for production. Use env vars
   or a secrets manager, never commit to repo.

5. **Data volume** — Global GloFAS GRIB at 0.05° with 51 members × 30 days is
   substantial. Consider regional subsetting for initial deployment, expand later.

---

## Implementation Order

1. **Crosswalk table + migration** (foundation for everything)
2. **CDS API wrapper** (download + open GRIB)
3. **GloFAS provider adapter** (5 required methods)
4. **Return periods** (from reanalysis or pre-computed thresholds)
5. **Provider registration + CLI** (plug into existing pipeline)
6. **Frontend provider toggle** (switch between forecasts)
7. **Production scheduling** (daily cron)
