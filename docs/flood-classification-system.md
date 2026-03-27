# Flood Classification System (Current Multi-Provider Design)

This document describes the classification model as currently implemented in this codebase.

## 1) Core pattern

The repository uses:

1. **Provider-specific ingestion** (GeoGloWS, GloFAS),
2. **normalized storage schemas**,
3. **provider-agnostic classification logic**.

This keeps API semantics stable while allowing different upstream data sources.

---

## 2) Provider ingestion and normalization

### GeoGloWS

- Forecasts: public GEOGLOWS run Zarr and/or REST debug path.
- Return periods: imported from GEOGLOWS retrospective Zarr (`gumbel` or `logpearson3`).

### GloFAS

- Forecasts: CDS GRIB download + reach-grid crosswalk mapping.
- Return periods: imported offline (NetCDF thresholds / threshold file / reanalysis path), then stored in the same normalized return-period table.

Both providers emit normalized rows for:

- timeseries (`flow_mean_cms`, `flow_median_cms`, `flow_p25_cms`, `flow_p75_cms`, `flow_max_cms`),
- return periods (`rp_2`, `rp_5`, `rp_10`, `rp_25`, `rp_50`, `rp_100`),
- reach summaries.

---

## 3) Peak extraction and fallback strategy

During summarization, peak selection is deterministic:

- `peak_max_cms = max(flow_max_cms)` if available,
- else fallback to mean/median-based maxima as needed.

Classification uses a single selected peak flow with fallback order:

1. `peak_max_cms`
2. `peak_mean_cms`
3. `peak_median_cms`

`first_exceedance_time_utc` is computed as first timestep where row-level fallback flow reaches/exceeds `rp_2`.

---

## 4) Classification algorithm

`classify_peak_flow(peak_flow, thresholds)` produces:

- `return_period_band`
- `severity_score`
- `is_flagged`

Rules:

1. Missing peak or missing threshold context → `unknown`, severity `0`, not flagged.
2. If thresholds exist and `peak < rp_2` → `below_2`, severity `0`, not flagged.
3. Otherwise threshold bands are:
   - `2`  (>= rp_2, < rp_5)   → severity `1`
   - `5`  (>= rp_5, < rp_10)  → severity `2`
   - `10` (>= rp_10, < rp_25) → severity `3`
   - `25` (>= rp_25, < rp_50) → severity `4`
   - `50` (>= rp_50, < rp_100)→ severity `5`
   - `100` (>= rp_100)        → severity `6`

---

## 5) GloFAS threshold mapping note

GloFAS threshold products may provide fewer canonical return periods than the internal six-band schema. The import pipeline normalizes available values into `rp_*` fields so the shared classifier and API response format remain unchanged.

In practice, if a threshold band is missing for a provider/reach, classification degrades gracefully according to available thresholds.

---

## 6) API semantics

Frontend and downstream clients consume provider-neutral fields from:

- `GET /forecast/reaches/{provider}/{reach_id}`
- `GET /forecast/map/severity`
- `GET /forecast/summary`

This enables direct side-by-side map and hydrograph behavior for GeoGloWS and GloFAS without per-provider response contracts.

---

## 7) Why this still works as providers evolve

As long as each provider adapter maps upstream data to normalized timeseries + thresholds, the core classifier and API shape do not need redesign.

That is the main architectural guarantee of this system.
