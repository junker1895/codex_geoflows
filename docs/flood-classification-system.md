# Flood Classification System (Model-Agnostic Design)

## 1) Architecture overview

This repository separates **provider-specific hydrology ingestion** from **provider-agnostic flood classification**.

At a high level:

1. A provider adapter (e.g., GEOGLOWS) discovers runs and fetches raw forecast/threshold data.
2. Data is normalized into common schemas (`TimeseriesPointSchema`, `ReturnPeriodSchema`, `ReachSummarySchema`).
3. The service layer stores normalized rows in provider-keyed tables.
4. Summary generation computes forecast peaks and classifies against return-period thresholds.
5. API endpoints expose run metadata, timeseries, thresholds, and normalized summary fields.

Core modules:

- Provider contract: `ForecastProviderAdapter` in `app/forecast/base.py`.
- Orchestration: `ForecastService` in `app/forecast/service.py`.
- Classification logic: `classify_peak_flow` in `app/forecast/classify.py`.
- Provider implementation: `app/forecast/providers/geoglows.py`.
- GEOGLOWS threshold import from Zarr: `app/forecast/providers/geoglows_return_periods.py`.
- Persistence models: `app/db/models.py`.
- API surface: `app/api/routes/forecast.py`.

## 2) Hydrological return periods: what and why

A return period threshold (e.g., 2-year, 5-year, 10-year discharge) is an estimate of the flow magnitude associated with a long-term exceedance frequency.

Why this project uses return periods for normalization:

- **Cross-basin comparability:** absolute discharge (cms) is not directly comparable between small and large rivers.
- **Risk semantics:** thresholds map naturally to interpretable impact levels.
- **Provider abstraction:** each model can provide thresholds differently, but the summary classification can remain uniform.

So the system converts raw discharge peaks into normalized threshold bands (`below_2`, `2`, `5`, `10`, `25`, `50`, `100`) plus a scalar severity score.

## 3) How GEOGLOWS thresholds are derived and stored

GEOGLOWS thresholds are imported from the public retrospective Zarr dataset.

- Default source path: `s3://geoglows-v2/retrospective/return-periods.zarr`.
- Access method is intentionally anonymous/public (`anon=True`) with explicit `us-west-2` region.
- Import supports methods `gumbel` and `logpearson3`.
- Import iterates by `river_id` in chunks and maps `return_period` coordinates (`2,5,10,25,50,100`) into row fields `rp_2..rp_100`.

Relevant implementation:

- Batch iterator/open/config: `iter_geoglows_return_periods_from_zarr`, `open_geoglows_public_return_periods_zarr`, `build_geoglows_public_zarr_storage_options`.
- Upsert target table: `forecast_provider_return_periods` keyed by `(provider, provider_reach_id)`.
- Repository method: `upsert_return_periods`.

This means threshold storage is provider-scoped but schema-consistent.

## 4) How forecast peaks are extracted from time series

During summarization (`ForecastService.summarize_run`), each reach is processed with:

- Forecast timeseries rows for `(provider, run_id, reach_id)`.
- Optional return-period row for `(provider, reach_id)`.

GEOGLOWS summary extraction computes:

- `peak_max_cms` = max of `flow_max_cms` values.
- `peak_mean_cms` = max of `flow_mean_cms` values.
- `peak_median_cms` = max of `flow_median_cms` values.
- Classification `peak_flow` fallback order: `peak_max_cms`, then `peak_mean_cms`, then `peak_median_cms`.

This gives deterministic, consistent peak selection independent of how complete each model’s output fields are.

## 5) Threshold exceedance algorithm

Current classification function: `classify_peak_flow(peak_flow, thresholds)`.

Decision rules:

1. If `peak_flow` is missing OR no threshold record exists -> `unknown`, severity `0`, not flagged.
2. If `rp_2` is unavailable -> `unknown`, severity `0`, not flagged.
3. If `peak < rp_2` -> `below_2`, severity `0`, not flagged.
4. Else compare upward:
   - `rp_2 <= peak < rp_5` -> band `2`, severity `1`, flagged.
   - `rp_5 <= peak < rp_10` -> band `5`, severity `2`, flagged.
   - `rp_10 <= peak < rp_25` -> band `10`, severity `3`, flagged.
   - `rp_25 <= peak < rp_50` -> band `25`, severity `4`, flagged.
   - `rp_50 <= peak < rp_100` -> band `50`, severity `5`, flagged.
   - `peak >= rp_100` -> band `100`, severity `6`, flagged.

`first_exceedance_time_utc` is independently computed as the first forecast timestamp where the row-level flow fallback (`max`, then `mean`, then `median`) reaches/exceeds `rp_2`.

## 6) Meaning of `return_period_band` and `severity_score`

`return_period_band` is a categorical normalized threshold class. `severity_score` is an ordered numeric ranking suitable for filtering/sorting.

| return_period_band | meaning | severity_score | is_flagged |
|---|---|---:|---|
| `unknown` | no usable peak or threshold context | 0 | false |
| `below_2` | threshold exists; peak below 2-year flow | 0 | false |
| `2` | reached/exceeded 2-year, below 5-year | 1 | true |
| `5` | reached/exceeded 5-year, below 10-year | 2 | true |
| `10` | reached/exceeded 10-year, below 25-year | 3 | true |
| `25` | reached/exceeded 25-year, below 50-year | 4 | true |
| `50` | reached/exceeded 50-year, below 100-year | 5 | true |
| `100` | reached/exceeded 100-year | 6 | true |

## 7) Why this supports additional models (e.g., GloFAS)

The key extensibility mechanism is the provider adapter interface.

A new provider only needs to implement:

- run discovery,
- timeseries fetch/normalization,
- threshold fetch/normalization (or local import path),
- summary generation using shared schemas.

Because classification consumes normalized peak + normalized thresholds, model-specific data access can vary without changing API shape or downstream consumers.

Practically, onboarding GloFAS would be:

1. Add a `GlofasForecastProvider` implementing `ForecastProviderAdapter`.
2. Map GloFAS identifiers to `provider_reach_id` for provider-scoped joins.
3. Persist `rp_2..rp_100` equivalents into `forecast_provider_return_periods`.
4. Reuse `classify_peak_flow` and existing summary/API schemas.

## 8) Deterministic thresholds vs probabilistic forecast systems

### GEOGLOWS in this repository

- Uses deterministic per-reach return-period thresholds from retrospective fitting (`gumbel` / `logpearson3`).
- Classification is based on a single representative peak per reach (with fallback order).

### GloFAS-style ensemble context (conceptual integration)

- Forecasts are often ensemble/probabilistic (multiple members, exceedance probabilities).
- Two integration strategies are typical:
  1. Derive a deterministic representative peak (e.g., ensemble median/quantile/max) before classification.
  2. Keep probabilities in metadata, but still publish the normalized deterministic band for consistency.

This architecture supports either approach as long as the provider adapter outputs normalized peak + thresholds for the shared classifier.

## 9) API exposure: forecast, thresholds, classification

Main forecast endpoints:

- `GET /forecast/reaches/{provider}/{reach_id}` returns:
  - `run` metadata,
  - `timeseries` rows,
  - `return_periods` (`rp_2..rp_100`),
  - `summary` with `return_period_band`, `severity_score`, `is_flagged`, peaks, exceedance time.
- `GET /forecast/summary` returns reach summaries and supports `severity_min` filtering.
- `GET /forecast/health` includes provider capability/availability indicators.

Because schema fields are provider-neutral, clients can compare GEOGLOWS and future providers consistently without per-provider response branching.

---

## Design takeaway

The repository’s flood-risk normalization strategy is:

- **Provider-specific ingestion + normalization at edges**,
- **Provider-agnostic classification in the core**,
- **Stable API schema for multi-model comparability**.

That separation is what enables adding models like GloFAS without redesigning the classification logic or response contract.
