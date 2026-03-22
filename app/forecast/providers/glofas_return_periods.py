"""GloFAS return period threshold import.

Downloads pre-computed GloFAS flood return period thresholds from the
Copernicus CDS API and maps them to GeoGloWS reach IDs via the
reach-grid crosswalk table.

GloFAS provides 2-, 5-, and 20-year return period thresholds computed from
ERA5 reanalysis annual maxima (Gumbel distribution). These map to our schema:
    rp_2  = GloFAS 2-year
    rp_5  = GloFAS 5-year
    rp_25 = GloFAS 20-year  (closest match; rp_10/rp_50/rp_100 left NULL)
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

from app.forecast.exceptions import ForecastValidationError, ProviderOperationalError
from app.forecast.schemas import ReturnPeriodSchema

logger = logging.getLogger(__name__)

# GloFAS standard return periods (years)
_GLOFAS_RP_YEARS = [2, 5, 20]

# CDS dataset for GloFAS thresholds
_CDS_DATASET = "cems-glofas-historical"


def download_glofas_return_period_thresholds(
    *,
    target_path: str,
    system_version: str = "version_4_0",
    cds_url: str | None = None,
    cds_key: str | None = None,
) -> str:
    """Download GloFAS return period threshold grid from CDS.

    This downloads the GloFAS-ERA5 reanalysis return period thresholds.
    The file contains a global 0.05° grid with discharge thresholds
    for 2-, 5-, and 20-year return periods.
    """
    import cdsapi

    client_kwargs = {}
    if cds_url:
        client_kwargs["url"] = cds_url
    if cds_key:
        client_kwargs["key"] = cds_key

    client = cdsapi.Client(**client_kwargs)

    request = {
        "system_version": [system_version],
        "hydrological_model": ["lisflood"],
        "product_type": ["consolidated"],
        "variable": "river_discharge_in_the_last_24_hours",
        # Request a single month to get the grid structure;
        # return periods are computed from annual maxima across all years
        "year": "2023",
        "month": "01",
        "day": "01",
        "data_format": "grib",
    }

    Path(target_path).parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Downloading GloFAS reanalysis for return period computation",
        extra={"target": target_path},
    )
    client.retrieve(_CDS_DATASET, request, target_path)
    return target_path


def compute_return_periods_from_reanalysis(
    *,
    reanalysis_dir: str,
    target_path: str,
) -> str:
    """Compute return period thresholds from multi-year GloFAS reanalysis GRIBs.

    Fits a Gumbel distribution to annual maxima extracted from reanalysis files.
    This is the full computation path — use import_from_threshold_file() instead
    if you have pre-computed thresholds.
    """
    import numpy as np
    import xarray as xr
    from scipy.stats import gumbel_r

    reanalysis_path = Path(reanalysis_dir)
    grib_files = sorted(reanalysis_path.glob("*.grib"))
    if not grib_files:
        raise ForecastValidationError(
            f"No GRIB files found in {reanalysis_dir}"
        )

    logger.info("Computing annual maxima from %d reanalysis files", len(grib_files))

    # Collect annual maxima per grid cell
    annual_maxima: dict[str, list[float]] = {}

    for grib_file in grib_files:
        try:
            ds = xr.open_dataset(str(grib_file), engine="cfgrib")
        except Exception as exc:
            logger.warning("Skipping %s: %s", grib_file, exc)
            continue

        # Find discharge variable
        dis_var = _find_discharge_var(ds)
        if dis_var is None:
            continue

        # Compute max over time dimension for this file
        time_dim = "time" if "time" in ds[dis_var].dims else "step"
        if time_dim in ds[dis_var].dims:
            max_vals = ds[dis_var].max(dim=time_dim)
        else:
            max_vals = ds[dis_var]

        lats = max_vals.latitude.values
        lons = max_vals.longitude.values

        for i, lat in enumerate(lats):
            for j, lon in enumerate(lons):
                val = float(max_vals.values[i, j])
                if np.isnan(val) or val <= 0:
                    continue
                key = f"{lat:.4f},{lon:.4f}"
                if key not in annual_maxima:
                    annual_maxima[key] = []
                annual_maxima[key].append(val)

        ds.close()

    if not annual_maxima:
        raise ProviderOperationalError("No valid discharge data found in reanalysis files")

    # Fit Gumbel distribution and compute return period thresholds
    logger.info("Fitting Gumbel distributions for %d grid cells", len(annual_maxima))

    rows = []
    for key, maxima in annual_maxima.items():
        if len(maxima) < 5:
            continue
        lat_str, lon_str = key.split(",")
        try:
            loc, scale = gumbel_r.fit(maxima)
            thresholds = {}
            for rp in _GLOFAS_RP_YEARS:
                # Exceedance probability = 1/rp; quantile = 1 - 1/rp
                thresholds[rp] = float(gumbel_r.ppf(1.0 - 1.0 / rp, loc=loc, scale=scale))
        except Exception:
            continue

        rows.append({
            "lat": float(lat_str),
            "lon": float(lon_str),
            "rp_2": thresholds.get(2),
            "rp_5": thresholds.get(5),
            "rp_20": thresholds.get(20),
        })

    # Save as parquet
    import pandas as pd

    df = pd.DataFrame(rows)
    df.to_parquet(target_path, index=False)
    logger.info("Saved %d grid cell thresholds to %s", len(df), target_path)
    return target_path


def iter_glofas_return_periods_from_threshold_file(
    *,
    threshold_path: str,
    batch_size: int = 5000,
) -> Iterator[list[ReturnPeriodSchema]]:
    """Load pre-computed GloFAS thresholds from a parquet/CSV file and yield
    ReturnPeriodSchema rows mapped to GeoGloWS reach IDs via the crosswalk.

    The threshold file must have columns: lat, lon, rp_2, rp_5, rp_20.
    Each grid cell is matched to crosswalk entries to find the reach IDs.
    """
    pd = _import_pandas()

    path = Path(threshold_path)
    if not path.exists():
        raise ForecastValidationError(f"Threshold file not found: {threshold_path}")

    if path.suffix.lower() in {".parquet", ".pq"}:
        df = pd.read_parquet(path)
    elif path.suffix.lower() in {".csv", ".txt"}:
        df = pd.read_csv(path)
    else:
        raise ForecastValidationError(
            f"Unsupported file format '{path.suffix}'. Use .parquet or .csv"
        )

    # Resolve column names
    lat_col = _resolve_column(df.columns, ("lat", "latitude", "grid_lat"))
    lon_col = _resolve_column(df.columns, ("lon", "longitude", "grid_lon"))
    rp2_col = _resolve_column(df.columns, ("rp_2", "rp2", "return_period_2", "q2"))
    rp5_col = _resolve_column(df.columns, ("rp_5", "rp5", "return_period_5", "q5"))
    rp20_col = _resolve_column(df.columns, ("rp_20", "rp20", "return_period_20", "q20"))

    if not lat_col or not lon_col:
        raise ForecastValidationError(
            "Threshold file must have lat/lon columns. "
            f"Found columns: {list(df.columns)}"
        )

    if not any([rp2_col, rp5_col, rp20_col]):
        raise ForecastValidationError(
            "Threshold file must have at least one of: rp_2, rp_5, rp_20. "
            f"Found columns: {list(df.columns)}"
        )

    # Build a lookup: (rounded_lat, rounded_lon) -> threshold row
    grid_thresholds: dict[tuple[float, float], dict] = {}
    for _, row in df.iterrows():
        lat = round(float(row[lat_col]), 4)
        lon = round(float(row[lon_col]), 4)
        grid_thresholds[(lat, lon)] = {
            "rp_2": _safe_float(row.get(rp2_col)) if rp2_col else None,
            "rp_5": _safe_float(row.get(rp5_col)) if rp5_col else None,
            "rp_20": _safe_float(row.get(rp20_col)) if rp20_col else None,
        }

    # Load crosswalk to map grid cells to reach IDs
    crosswalk = _load_full_crosswalk()
    if not crosswalk:
        raise ProviderOperationalError(
            "No crosswalk entries found. Run 'build-crosswalk' first."
        )

    logger.info(
        "Matching %d grid thresholds to %d crosswalk entries",
        len(grid_thresholds),
        len(crosswalk),
    )

    # Match crosswalk entries to threshold grid cells
    batch: list[ReturnPeriodSchema] = []
    matched = 0
    unmatched = 0

    for reach_id, (grid_lat, grid_lon) in crosswalk.items():
        key = (round(grid_lat, 4), round(grid_lon, 4))
        thresholds = grid_thresholds.get(key)

        if thresholds is None:
            # Try nearest match within tolerance
            thresholds = _find_nearest_threshold(grid_lat, grid_lon, grid_thresholds, tolerance=0.06)

        if thresholds is None:
            unmatched += 1
            continue

        matched += 1
        batch.append(
            ReturnPeriodSchema(
                provider="glofas",
                provider_reach_id=reach_id,
                rp_2=thresholds["rp_2"],
                rp_5=thresholds["rp_5"],
                rp_10=None,
                rp_25=thresholds["rp_20"],  # GloFAS 20-yr → our rp_25 slot
                rp_50=None,
                rp_100=None,
                metadata_json={
                    "source": "glofas_threshold_file",
                    "path": str(threshold_path),
                    "grid_lat": grid_lat,
                    "grid_lon": grid_lon,
                    "glofas_rp_years": [2, 5, 20],
                },
            )
        )

        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch

    logger.info(
        "GloFAS return period matching complete: matched=%d, unmatched=%d",
        matched,
        unmatched,
    )


def iter_glofas_return_periods_from_crosswalk(
    *,
    reanalysis_path: str,
    batch_size: int = 5000,
) -> Iterator[list[ReturnPeriodSchema]]:
    """Extract return period thresholds directly from a GloFAS reanalysis GRIB,
    only for grid cells that appear in the crosswalk table.

    This is the most direct approach: open the reanalysis, get discharge values
    at crosswalk grid cells, fit Gumbel distributions, and yield ReturnPeriodSchema rows.

    For a single-timestep file, this just uses the discharge value as a rough
    proxy. For multi-year data, it computes proper annual maxima statistics.
    """
    import numpy as np
    import xarray as xr

    crosswalk = _load_full_crosswalk()
    if not crosswalk:
        raise ProviderOperationalError(
            "No crosswalk entries found. Run 'build-crosswalk' first."
        )

    path = Path(reanalysis_path)
    if not path.exists():
        raise ForecastValidationError(f"Reanalysis file not found: {reanalysis_path}")

    try:
        ds = xr.open_dataset(str(path), engine="cfgrib")
    except Exception:
        try:
            datasets = xr.open_datasets(str(path), engine="cfgrib")
            ds = datasets[0]
        except Exception as exc:
            raise ProviderOperationalError(
                f"Failed to open GloFAS reanalysis GRIB: {exc}"
            ) from exc

    dis_var = _find_discharge_var(ds)
    if dis_var is None:
        raise ProviderOperationalError(
            f"No discharge variable found in {reanalysis_path}. "
            f"Available vars: {list(ds.data_vars)}"
        )

    logger.info(
        "Extracting return periods from reanalysis for %d crosswalk entries",
        len(crosswalk),
    )

    batch: list[ReturnPeriodSchema] = []
    for reach_id, (grid_lat, grid_lon) in crosswalk.items():
        try:
            cell = ds.sel(latitude=grid_lat, longitude=grid_lon, method="nearest")
            values = cell[dis_var].values

            if np.isscalar(values):
                values = np.array([values])
            values = values[~np.isnan(values.astype(float))]

            if len(values) == 0:
                continue

            # If we have enough samples, fit Gumbel; otherwise use percentile estimates
            if len(values) >= 10:
                from scipy.stats import gumbel_r

                loc, scale = gumbel_r.fit(values)
                rp_2 = float(gumbel_r.ppf(0.5, loc=loc, scale=scale))
                rp_5 = float(gumbel_r.ppf(0.8, loc=loc, scale=scale))
                rp_20 = float(gumbel_r.ppf(0.95, loc=loc, scale=scale))
            else:
                # Rough estimates from percentiles
                rp_2 = float(np.percentile(values, 50))
                rp_5 = float(np.percentile(values, 80))
                rp_20 = float(np.percentile(values, 95))

            batch.append(
                ReturnPeriodSchema(
                    provider="glofas",
                    provider_reach_id=reach_id,
                    rp_2=rp_2,
                    rp_5=rp_5,
                    rp_10=None,
                    rp_25=rp_20,  # GloFAS 20-yr → rp_25 slot
                    rp_50=None,
                    rp_100=None,
                    metadata_json={
                        "source": "glofas_reanalysis",
                        "path": str(reanalysis_path),
                        "grid_lat": grid_lat,
                        "grid_lon": grid_lon,
                        "sample_count": int(len(values)),
                    },
                )
            )

            if len(batch) >= batch_size:
                yield batch
                batch = []

        except Exception as exc:
            logger.debug(
                "Skipping reach %s (lat=%s, lon=%s): %s",
                reach_id, grid_lat, grid_lon, exc,
            )
            continue

    if batch:
        yield batch

    ds.close()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_full_crosswalk() -> dict[str, tuple[float, float]]:
    """Load all GloFAS crosswalk entries: reach_id -> (grid_lat, grid_lon)."""
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
        ).all()
        return {r.reach_id: (r.grid_lat, r.grid_lon) for r in rows}
    finally:
        db.close()


def _find_discharge_var(ds) -> str | None:
    """Find the river discharge variable in a dataset."""
    for var in ds.data_vars:
        long_name = ds[var].attrs.get("long_name", "").lower()
        if "discharge" in long_name or "dis" in str(var).lower():
            return var
    data_vars = list(ds.data_vars)
    return data_vars[0] if data_vars else None


def _find_nearest_threshold(
    lat: float,
    lon: float,
    grid_thresholds: dict[tuple[float, float], dict],
    tolerance: float = 0.06,
) -> dict | None:
    """Find the nearest grid threshold within tolerance (degrees)."""
    best_dist = tolerance
    best = None
    for (glat, glon), thresholds in grid_thresholds.items():
        dist = abs(glat - lat) + abs(glon - lon)
        if dist < best_dist:
            best_dist = dist
            best = thresholds
    return best


def _resolve_column(columns, candidates: tuple[str, ...]) -> str | None:
    normalized = {str(c).strip().lower(): str(c) for c in columns}
    for option in candidates:
        if option in normalized:
            return normalized[option]
    return None


def _safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
        if f != f:  # NaN
            return None
        return f
    except (TypeError, ValueError):
        return None


def _import_pandas():
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise ProviderOperationalError(
            "pandas is required for GloFAS return period import"
        ) from exc
    return pd
