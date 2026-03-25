"""GloFAS forecast provider adapter.

Uses the Copernicus CDS API to download GloFAS ensemble forecasts (GRIB),
and a reach-grid crosswalk table to map GloFAS 0.05° grid cells back to
GeoGloWS reach IDs (for display on the same PMTiles river network).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.forecast.base import ForecastProviderAdapter
from app.forecast.classify import classify_peak_flow
from app.forecast.exceptions import (
    ProviderBackendUnavailableError,
    ProviderOperationalError,
)
from app.forecast.schemas import (
    ForecastRunSchema,
    ReachSummarySchema,
    ReturnPeriodSchema,
    TimeseriesPointSchema,
)

logger = logging.getLogger(__name__)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
        if f != f:  # NaN check
            return None
        return f
    except (TypeError, ValueError):
        return None


def _first_not_none(*values: Any) -> float | None:
    for v in values:
        if v is not None:
            return v
    return None


class GlofasForecastProvider(ForecastProviderAdapter):
    """GloFAS provider using CDS API + reach-grid crosswalk."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._crosswalk_cache: dict[str, tuple[float, float]] | None = None
        self._supported_reach_filter: set[str] | None = None

    def get_provider_name(self) -> str:
        return "glofas"

    # ------------------------------------------------------------------
    # Run discovery
    # ------------------------------------------------------------------

    def discover_latest_run(self) -> ForecastRunSchema:
        # GloFAS publishes daily with ~24h delay; latest available = yesterday
        run_date = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        # Try yesterday first (most reliable), then today
        from datetime import timedelta

        run_date = run_date - timedelta(days=1)
        run_id = run_date.strftime("%Y%m%d00")

        return ForecastRunSchema(
            provider=self.get_provider_name(),
            run_id=run_id,
            run_date_utc=run_date,
            issued_at_utc=run_date,
            source_type="glofas_cds",
            ingest_status="pending",
            metadata_json={
                "selector": "latest",
                "system_version": self.settings.glofas_system_version,
                "cds_url": self.settings.glofas_cds_url,
            },
        )

    # ------------------------------------------------------------------
    # Return periods
    # ------------------------------------------------------------------

    def fetch_return_periods(self, reach_ids: list[str | int]) -> list[ReturnPeriodSchema]:
        # GloFAS return periods must be pre-imported via the CLI
        # (computed from GloFAS-ERA5 reanalysis annual maxima).
        # This method is a no-op stub — bulk import is the intended path.
        raise ProviderBackendUnavailableError(
            "GloFAS return periods must be pre-imported via "
            "'import-glofas-return-periods' CLI command. "
            "They cannot be fetched on-demand like GeoGloWS."
        )

    # ------------------------------------------------------------------
    # Forecast timeseries (per-reach, via downloaded GRIB + crosswalk)
    # ------------------------------------------------------------------

    def fetch_forecast_timeseries(
        self, run_id: str, reach_ids: list[str | int]
    ) -> list[TimeseriesPointSchema]:
        """Extract timeseries for specific reaches from a downloaded GloFAS GRIB.

        This reads the staged GRIB file for the given run and extracts
        data at the grid cells mapped to the requested reach IDs via the crosswalk.
        """
        grib_path = self._staged_grib_path(run_id)
        if not grib_path.exists():
            raise ProviderBackendUnavailableError(
                f"GloFAS GRIB not found at {grib_path}. "
                f"Run 'prepare-bulk-artifact --provider glofas --run-id {run_id}' first."
            )

        crosswalk = self._load_crosswalk_for_reaches(reach_ids)
        if not crosswalk:
            return []

        import numpy as np
        import xarray as xr

        datasets = self._open_grib_datasets(str(grib_path))

        rows: list[TimeseriesPointSchema] = []
        for reach_id_str, (lat, lon) in crosswalk.items():
            for ds in datasets:
                try:
                    cell = ds.sel(latitude=lat, longitude=lon, method="nearest")
                except Exception:
                    continue

                discharge_var = self._find_discharge_var(ds)
                if discharge_var is None:
                    continue

                cell_data = cell[discharge_var]

                # Get time dimension
                time_dim = "time" if "time" in cell_data.dims else "step"
                if time_dim not in cell_data.dims:
                    continue

                times = cell_data[time_dim].values

                # Handle ensemble dimension
                ensemble_dim = None
                for dim in cell_data.dims:
                    if dim not in (time_dim, "latitude", "longitude"):
                        ensemble_dim = dim
                        break

                for t_idx, t_val in enumerate(times):
                    dt = self._to_utc_datetime(t_val, ds)

                    if ensemble_dim and ensemble_dim in cell_data.dims:
                        members = cell_data.isel(**{time_dim: t_idx}).values
                        members = members[~np.isnan(members)]
                        if len(members) == 0:
                            continue
                        rows.append(
                            TimeseriesPointSchema(
                                provider=self.get_provider_name(),
                                run_id=run_id,
                                provider_reach_id=reach_id_str,
                                forecast_time_utc=dt,
                                flow_mean_cms=_safe_float(np.mean(members)),
                                flow_median_cms=_safe_float(np.median(members)),
                                flow_p25_cms=_safe_float(np.percentile(members, 25)),
                                flow_p75_cms=_safe_float(np.percentile(members, 75)),
                                flow_max_cms=_safe_float(np.max(members)),
                            )
                        )
                    else:
                        val = float(cell_data.isel(**{time_dim: t_idx}).values)
                        if val != val:  # NaN
                            continue
                        rows.append(
                            TimeseriesPointSchema(
                                provider=self.get_provider_name(),
                                run_id=run_id,
                                provider_reach_id=reach_id_str,
                                forecast_time_utc=dt,
                                flow_mean_cms=_safe_float(val),
                                flow_median_cms=None,
                                flow_p25_cms=None,
                                flow_p75_cms=None,
                                flow_max_cms=None,
                            )
                        )
        return rows

    # ------------------------------------------------------------------
    # Summarize
    # ------------------------------------------------------------------

    def summarize_reach(
        self,
        run_id: str,
        reach_id: str | int,
        timeseries_rows: list[TimeseriesPointSchema],
        return_period_row: ReturnPeriodSchema | None,
    ) -> ReachSummarySchema:
        if not timeseries_rows:
            return ReachSummarySchema(
                provider=self.get_provider_name(),
                run_id=run_id,
                provider_reach_id=str(reach_id),
            )

        peak_mean = max(
            (r.flow_mean_cms for r in timeseries_rows if r.flow_mean_cms is not None),
            default=None,
        )
        peak_median = max(
            (r.flow_median_cms for r in timeseries_rows if r.flow_median_cms is not None),
            default=None,
        )
        peak_max = max(
            (r.flow_max_cms for r in timeseries_rows if r.flow_max_cms is not None),
            default=None,
        )

        peak_row = max(
            timeseries_rows,
            key=lambda r: _first_not_none(r.flow_max_cms, r.flow_mean_cms, r.flow_median_cms, -1.0),
            default=None,
        )

        peak_flow = _first_not_none(peak_max, peak_mean, peak_median)
        classification = classify_peak_flow(peak_flow, return_period_row)

        first_exceedance = None
        if return_period_row and return_period_row.rp_2 is not None:
            for row in sorted(timeseries_rows, key=lambda r: r.forecast_time_utc):
                candidate = _first_not_none(row.flow_max_cms, row.flow_mean_cms, row.flow_median_cms)
                if candidate is not None and candidate >= return_period_row.rp_2:
                    first_exceedance = row.forecast_time_utc
                    break

        return ReachSummarySchema(
            provider=self.get_provider_name(),
            run_id=run_id,
            provider_reach_id=str(reach_id),
            peak_time_utc=None if peak_row is None else peak_row.forecast_time_utc,
            first_exceedance_time_utc=first_exceedance,
            peak_mean_cms=peak_mean,
            peak_median_cms=peak_median,
            peak_max_cms=peak_max,
            return_period_band=classification.return_period_band,
            severity_score=classification.severity_score,
            is_flagged=classification.is_flagged,
            metadata_json={"points": len(timeseries_rows), "source": "glofas_cds"},
        )

    # ------------------------------------------------------------------
    # Bulk acquisition
    # ------------------------------------------------------------------

    def supports_bulk_acquisition(self) -> bool:
        return bool(self.settings.glofas_cds_key)

    def bulk_acquisition_mode(self) -> str:
        return "cds_grib"

    def acquire_bulk_raw_source(self, run_id: str, overwrite: bool = False) -> str:
        from app.forecast.providers.glofas_cds import download_glofas_forecast

        dest = self._staged_grib_path(run_id)
        if dest.exists() and not overwrite:
            logger.info("GloFAS GRIB already staged: %s", dest)
            return str(dest)

        date_str = run_id[:8]
        max_lt = self.settings.glofas_forecast_max_leadtime_hours
        leadtimes = list(range(24, max_lt + 1, 24))

        download_glofas_forecast(
            date=date_str,
            leadtime_hours=leadtimes,
            target_path=str(dest),
            data_format=self.settings.glofas_data_format,
            system_version=self.settings.glofas_system_version,
            cds_url=self.settings.glofas_cds_url,
            cds_key=self.settings.glofas_cds_key,
        )
        return str(dest)

    def iter_raw_bulk_records(self, run_id: str, staged_raw_path: str) -> Iterator[dict]:
        """Iterate over all crosswalk reaches and yield per-reach/per-timestep records from the GRIB.

        Groups reaches by their nearest grid cell so each cell's data is
        extracted only once, regardless of how many reaches map to it.
        """
        from collections import defaultdict

        import numpy as np

        grib_path = Path(staged_raw_path)
        if not grib_path.exists():
            raise ProviderOperationalError(f"GloFAS GRIB not found: {staged_raw_path}")

        crosswalk = self._load_all_crosswalk()
        if not crosswalk:
            logger.warning("No crosswalk entries found for GloFAS — nothing to iterate")
            return

        logger.info("Loaded %d crosswalk entries for bulk iteration", len(crosswalk))
        datasets = self._open_grib_datasets(str(grib_path))

        for ds in datasets:
            discharge_var = self._find_discharge_var(ds)
            if discharge_var is None:
                continue

            data_arr = ds[discharge_var]

            time_dim = "time" if "time" in data_arr.dims else "step"
            if time_dim not in data_arr.dims:
                continue

            # Identify ensemble dimension (if any)
            ensemble_dim = None
            for dim in data_arr.dims:
                if dim not in (time_dim, "latitude", "longitude"):
                    ensemble_dim = dim
                    break

            grid_lats = ds["latitude"].values
            grid_lons = ds["longitude"].values

            # Compute nearest grid index per reach using searchsorted
            # (O(N log G) instead of O(N*G) broadcasting)
            lat_sorted_idx = np.argsort(grid_lats)
            lon_sorted_idx = np.argsort(grid_lons)
            sorted_lats = grid_lats[lat_sorted_idx]
            sorted_lons = grid_lons[lon_sorted_idx]

            # Group reaches by their nearest (lat_idx, lon_idx) grid cell
            cell_to_reaches: dict[tuple[int, int], list[str]] = defaultdict(list)
            for reach_id, (want_lat, want_lon) in crosswalk.items():
                li = lat_sorted_idx[min(np.searchsorted(sorted_lats, want_lat), len(sorted_lats) - 1)]
                # Refine: check neighbor
                li_s = np.searchsorted(sorted_lats, want_lat)
                if li_s > 0 and (li_s >= len(sorted_lats) or abs(sorted_lats[li_s - 1] - want_lat) < abs(sorted_lats[li_s] - want_lat)):
                    li = lat_sorted_idx[li_s - 1]
                else:
                    li = lat_sorted_idx[min(li_s, len(sorted_lats) - 1)]

                lo_s = np.searchsorted(sorted_lons, want_lon)
                if lo_s > 0 and (lo_s >= len(sorted_lons) or abs(sorted_lons[lo_s - 1] - want_lon) < abs(sorted_lons[lo_s] - want_lon)):
                    lo = lon_sorted_idx[lo_s - 1]
                else:
                    lo = lon_sorted_idx[min(lo_s, len(sorted_lons) - 1)]

                cell_to_reaches[(int(li), int(lo))].append(reach_id)

            logger.info(
                "Grouped %d reaches into %d unique grid cells",
                len(crosswalk), len(cell_to_reaches),
            )

            # Load full data array into memory once
            all_data = data_arr.values
            times = ds[time_dim].values

            # Resolve dimension order for indexing
            dim_names = list(data_arr.dims)
            lat_axis = dim_names.index("latitude")
            lon_axis = dim_names.index("longitude")
            remaining_dims = [d for i, d in enumerate(dim_names) if i not in (lat_axis, lon_axis)]
            time_pos = remaining_dims.index(time_dim)
            has_ensemble = ensemble_dim is not None and ensemble_dim in dim_names

            logger.info(
                "Processing dataset: shape=%s, dims=%s, cells=%d, timesteps=%d",
                all_data.shape, dim_names, len(cell_to_reaches), len(times),
            )

            # Precompute datetimes for all timesteps
            datetimes = [self._to_utc_datetime(t, ds) for t in times]

            for (li, lo), reach_id_list in cell_to_reaches.items():
                # Extract this cell's full timeseries (once per cell)
                idx = [slice(None)] * len(dim_names)
                idx[lat_axis] = li
                idx[lon_axis] = lo
                cell_data = all_data[tuple(idx)]

                for t_idx, dt in enumerate(datetimes):
                    t_slice = [slice(None)] * len(remaining_dims)
                    t_slice[time_pos] = t_idx

                    if has_ensemble:
                        members = np.asarray(cell_data[tuple(t_slice)]).ravel()
                        members = members[~np.isnan(members)]
                        if len(members) == 0:
                            continue
                        record = {
                            "forecast_time_utc": dt,
                            "flow_mean_cms": _safe_float(np.mean(members)),
                            "flow_median_cms": _safe_float(np.median(members)),
                            "flow_p25_cms": _safe_float(np.percentile(members, 25)),
                            "flow_p75_cms": _safe_float(np.percentile(members, 75)),
                            "flow_max_cms": _safe_float(np.max(members)),
                        }
                    else:
                        val = float(cell_data[tuple(t_slice)])
                        if val != val:  # NaN
                            continue
                        record = {
                            "forecast_time_utc": dt,
                            "flow_mean_cms": _safe_float(val),
                        }

                    for reach_id_str in reach_id_list:
                        yield {**record, "provider_reach_id": reach_id_str}

    def normalize_bulk_record(self, run_id: str, record: dict) -> BulkForecastArtifactRowSchema | None:
        from app.forecast.schemas import BulkForecastArtifactRowSchema

        reach_id = str(record.get("provider_reach_id", "")).strip()
        if not reach_id:
            return None
        dt = record.get("forecast_time_utc")
        if dt is None:
            return None

        return BulkForecastArtifactRowSchema(
            provider=self.get_provider_name(),
            run_id=run_id,
            provider_reach_id=reach_id,
            forecast_time_utc=dt,
            flow_mean_cms=record.get("flow_mean_cms"),
            flow_median_cms=record.get("flow_median_cms"),
            flow_p25_cms=record.get("flow_p25_cms"),
            flow_p75_cms=record.get("flow_p75_cms"),
            flow_max_cms=record.get("flow_max_cms"),
            raw_payload_json={"source": "glofas_grib_bulk"},
        )

    def set_supported_reach_filter(self, reach_ids: set[str] | set[int] | None) -> None:
        self._supported_reach_filter = None if reach_ids is None else {str(x).strip() for x in reach_ids}

    def iter_bulk_summary_records(
        self,
        run_id: str,
        *,
        max_reaches: int | None = None,
        max_blocks: int | None = None,
        max_seconds: int | None = None,
        full_run: bool = False,
    ) -> Iterator[dict]:
        """Yield one summary dict per reach from a staged GloFAS GRIB file."""
        from collections import defaultdict

        import numpy as np

        grib_path = self._staged_grib_path(run_id)
        if not grib_path.exists():
            raise ProviderOperationalError(
                f"GloFAS GRIB not found at {grib_path}. "
                f"Run 'prepare-bulk-artifact --provider glofas --run-id {run_id}' first."
            )

        crosswalk = self._load_all_crosswalk()
        if not crosswalk:
            logger.warning("No crosswalk entries found for GloFAS — nothing to iterate")
            return

        supported_reaches = getattr(self, "_supported_reach_filter", None)
        if supported_reaches is not None:
            crosswalk = {k: v for k, v in crosswalk.items() if k in supported_reaches}
            if not crosswalk:
                logger.warning("No crosswalk entries match the supported reach filter")
                return

        logger.info("Loaded %d crosswalk entries for bulk summary iteration", len(crosswalk))
        datasets = self._open_grib_datasets(str(grib_path))

        start = datetime.now(UTC)
        emitted = 0

        for ds in datasets:
            discharge_var = self._find_discharge_var(ds)
            if discharge_var is None:
                continue

            data_arr = ds[discharge_var]
            time_dim = "time" if "time" in data_arr.dims else "step"
            if time_dim not in data_arr.dims:
                continue

            ensemble_dim = None
            for dim in data_arr.dims:
                if dim not in (time_dim, "latitude", "longitude"):
                    ensemble_dim = dim
                    break

            grid_lats = ds["latitude"].values
            grid_lons = ds["longitude"].values
            lat_sorted_idx = np.argsort(grid_lats)
            lon_sorted_idx = np.argsort(grid_lons)
            sorted_lats = grid_lats[lat_sorted_idx]
            sorted_lons = grid_lons[lon_sorted_idx]

            cell_to_reaches: dict[tuple[int, int], list[str]] = defaultdict(list)
            for reach_id, (want_lat, want_lon) in crosswalk.items():
                li_s = np.searchsorted(sorted_lats, want_lat)
                if li_s > 0 and (li_s >= len(sorted_lats) or abs(sorted_lats[li_s - 1] - want_lat) < abs(sorted_lats[li_s] - want_lat)):
                    li = lat_sorted_idx[li_s - 1]
                else:
                    li = lat_sorted_idx[min(li_s, len(sorted_lats) - 1)]

                lo_s = np.searchsorted(sorted_lons, want_lon)
                if lo_s > 0 and (lo_s >= len(sorted_lons) or abs(sorted_lons[lo_s - 1] - want_lon) < abs(sorted_lons[lo_s] - want_lon)):
                    lo = lon_sorted_idx[lo_s - 1]
                else:
                    lo = lon_sorted_idx[min(lo_s, len(sorted_lons) - 1)]

                cell_to_reaches[(int(li), int(lo))].append(reach_id)

            logger.info(
                "Grouped %d reaches into %d unique grid cells for summary",
                len(crosswalk), len(cell_to_reaches),
            )

            all_data = data_arr.values
            times = ds[time_dim].values
            dim_names = list(data_arr.dims)
            lat_axis = dim_names.index("latitude")
            lon_axis = dim_names.index("longitude")
            remaining_dims = [d for i, d in enumerate(dim_names) if i not in (lat_axis, lon_axis)]
            time_pos = remaining_dims.index(time_dim)
            has_ensemble = ensemble_dim is not None and ensemble_dim in dim_names

            datetimes = [self._to_utc_datetime(t, ds) for t in times]

            for (li, lo), reach_id_list in cell_to_reaches.items():
                if max_seconds is not None and (datetime.now(UTC) - start).total_seconds() >= max_seconds:
                    return
                if max_reaches is not None and emitted >= max_reaches:
                    return

                idx = [slice(None)] * len(dim_names)
                idx[lat_axis] = li
                idx[lon_axis] = lo
                cell_data = all_data[tuple(idx)]

                # Build per-timestep mean/max arrays
                n_times = len(datetimes)
                mean_series = np.empty(n_times, dtype=np.float32)
                max_series = np.empty(n_times, dtype=np.float32)

                for t_idx in range(n_times):
                    t_slice = [slice(None)] * len(remaining_dims)
                    t_slice[time_pos] = t_idx

                    if has_ensemble:
                        members = np.asarray(cell_data[tuple(t_slice)]).ravel()
                        members = members[~np.isnan(members)]
                        if len(members) == 0:
                            mean_series[t_idx] = np.nan
                            max_series[t_idx] = np.nan
                        else:
                            mean_series[t_idx] = np.mean(members)
                            max_series[t_idx] = np.max(members)
                    else:
                        val = float(cell_data[tuple(t_slice)])
                        mean_series[t_idx] = val
                        max_series[t_idx] = val

                has_finite_max = np.isfinite(max_series).any()
                peak_idx = int(np.nanargmax(max_series)) if has_finite_max else None

                for reach_id_str in reach_id_list:
                    if max_reaches is not None and emitted >= max_reaches:
                        return
                    emitted += 1
                    yield {
                        "provider_reach_id": reach_id_str,
                        "peak_time_utc": None if peak_idx is None else datetimes[peak_idx].isoformat(),
                        "peak_mean_cms": None if not np.isfinite(mean_series).any() else _safe_float(np.nanmax(mean_series)),
                        "peak_median_cms": None,
                        "peak_max_cms": None if not has_finite_max else _safe_float(np.nanmax(max_series)),
                        "now_mean_cms": _safe_float(mean_series[0]),
                        "now_max_cms": _safe_float(max_series[0]),
                        "raw_payload_json": {"source": "glofas_grib_bulk_summary"},
                    }

            elapsed = (datetime.now(UTC) - start).total_seconds()
            logger.info(
                "GloFAS bulk summary progress",
                extra={
                    "run_id": run_id,
                    "emitted_reaches": emitted,
                    "elapsed_seconds": round(elapsed, 2),
                },
            )
            # Use only the first dataset (cf) for summaries — processing
            # the perturbed forecast (pf) too would duplicate reaches and
            # risks OOM due to its ~50-member ensemble size.
            if emitted > 0:
                break

    def normalize_bulk_summary_record(self, run_id: str, record: dict) -> Any:
        from app.forecast.schemas import BulkForecastSummaryArtifactRowSchema

        reach_id = str(record.get("provider_reach_id", "")).strip()
        if not reach_id:
            return None

        peak_time = record.get("peak_time_utc")
        peak_time_utc = None
        if peak_time:
            peak_time_utc = peak_time if isinstance(peak_time, datetime) else datetime.fromisoformat(str(peak_time)).replace(tzinfo=UTC)

        payload = record.get("raw_payload_json") if isinstance(record.get("raw_payload_json"), dict) else {"source": "glofas_grib_bulk_summary"}
        return BulkForecastSummaryArtifactRowSchema(
            provider=self.get_provider_name(),
            run_id=run_id,
            provider_reach_id=reach_id,
            peak_time_utc=peak_time_utc,
            peak_mean_cms=_safe_float(record.get("peak_mean_cms")),
            peak_median_cms=_safe_float(record.get("peak_median_cms")),
            peak_max_cms=_safe_float(record.get("peak_max_cms")),
            now_mean_cms=_safe_float(record.get("now_mean_cms")),
            now_max_cms=_safe_float(record.get("now_max_cms")),
            raw_payload_json=payload,
        )

    def cleanup_old_raw_staging(self) -> int:
        keep_latest = self.settings.glofas_bulk_raw_retention_runs
        if keep_latest < 1:
            return 0
        staging_dir = Path(self.settings.glofas_bulk_staging_dir)
        if not staging_dir.exists():
            return 0
        files = sorted(staging_dir.glob("*.grib"), key=lambda p: p.stat().st_mtime, reverse=True)
        removed = 0
        for path in files[keep_latest:]:
            logger.info("Removing old GloFAS GRIB: %s", path)
            path.unlink(missing_ok=True)
            removed += 1
        return removed

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _staged_grib_path(self, run_id: str) -> Path:
        return Path(self.settings.glofas_bulk_staging_dir) / f"{run_id}.grib"

    def _load_all_crosswalk(self) -> dict[str, tuple[float, float]]:
        """Load all crosswalk entries for GloFAS.

        Returns a dict mapping reach_id (str) -> (grid_lat, grid_lon).
        """
        from sqlalchemy import select

        from app.core.database import SessionLocal
        from app.db.models import ReachGridCrosswalk

        db = SessionLocal()
        try:
            rows = db.execute(
                select(
                    ReachGridCrosswalk.reach_id,
                    ReachGridCrosswalk.grid_lat,
                    ReachGridCrosswalk.grid_lon,
                ).where(ReachGridCrosswalk.target_provider == "glofas")
                .where(ReachGridCrosswalk.is_valid_match.is_(True))
                .where(ReachGridCrosswalk.grid_lat.is_not(None))
                .where(ReachGridCrosswalk.grid_lon.is_not(None))
            ).all()
            return {r.reach_id: (r.grid_lat, r.grid_lon) for r in rows}
        finally:
            db.close()

    def _load_crosswalk_for_reaches(
        self, reach_ids: list[str | int]
    ) -> dict[str, tuple[float, float]]:
        """Load crosswalk entries for the given reach IDs.

        Returns a dict mapping reach_id (str) -> (grid_lat, grid_lon).
        """
        from sqlalchemy import and_, select

        from app.core.database import SessionLocal
        from app.db.models import ReachGridCrosswalk

        str_ids = [str(r) for r in reach_ids]
        db = SessionLocal()
        try:
            rows = db.execute(
                select(
                    ReachGridCrosswalk.reach_id,
                    ReachGridCrosswalk.grid_lat,
                    ReachGridCrosswalk.grid_lon,
                ).where(
                    and_(
                        ReachGridCrosswalk.target_provider == "glofas",
                        ReachGridCrosswalk.is_valid_match.is_(True),
                        ReachGridCrosswalk.grid_lat.is_not(None),
                        ReachGridCrosswalk.grid_lon.is_not(None),
                        ReachGridCrosswalk.reach_id.in_(str_ids),
                    )
                )
            ).all()
            return {r.reach_id: (r.grid_lat, r.grid_lon) for r in rows}
        finally:
            db.close()

    def _open_grib_datasets(self, path: str) -> list:
        """Open GRIB file, handling multi-message files."""
        from app.forecast.providers.glofas_cds import open_glofas_grib_ensemble

        return open_glofas_grib_ensemble(path)

    @staticmethod
    def _find_discharge_var(ds) -> str | None:
        """Find the river discharge variable in a GloFAS dataset."""
        for var in ds.data_vars:
            long_name = ds[var].attrs.get("long_name", "").lower()
            if "discharge" in long_name or "dis" in str(var).lower():
                return var
        # Fallback: first data var
        data_vars = list(ds.data_vars)
        return data_vars[0] if data_vars else None

    @staticmethod
    def _to_utc_datetime(time_val: Any, ds: Any) -> datetime:
        """Convert a numpy datetime64 or timedelta to a UTC datetime."""
        import numpy as np
        import pandas as pd

        if isinstance(time_val, np.timedelta64):
            # step-based: base_time + step
            base = pd.Timestamp(ds.attrs.get("time", ds.attrs.get("dataDate", "2000-01-01")))
            return (base + pd.Timedelta(time_val)).to_pydatetime().replace(tzinfo=UTC)

        ts = pd.Timestamp(time_val)
        return ts.to_pydatetime().replace(tzinfo=UTC)
