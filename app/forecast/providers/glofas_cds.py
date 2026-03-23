"""CDS API wrapper for downloading GloFAS forecast and reanalysis data."""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def download_glofas_forecast(
    *,
    date: str,
    leadtime_hours: list[int] | None = None,
    area: list[float] | None = None,
    target_path: str,
    data_format: str = "grib",
    system_version: str = "operational",
    cds_url: str | None = None,
    cds_key: str | None = None,
) -> str:
    """Download GloFAS forecast GRIB/NetCDF from the Copernicus EWDS CDS API.

    Parameters
    ----------
    date : str
        Forecast date in YYYYMMDD format.
    leadtime_hours : list[int] | None
        Lead times to download (e.g. [24, 48, ..., 720]).
        Defaults to 24h steps up to 720h (30 days).
    area : list[float] | None
        Bounding box [North, West, South, East] or None for global.
    target_path : str
        Local path to save the downloaded file.
    data_format : str
        "grib" or "netcdf".
    system_version : str
        GloFAS system version (e.g. "operational").
    cds_url : str | None
        CDS API URL override.
    cds_key : str | None
        CDS API key override.
    """
    import cdsapi

    if leadtime_hours is None:
        leadtime_hours = list(range(24, 721, 24))

    client_kwargs = {}
    if cds_url:
        client_kwargs["url"] = cds_url
    if cds_key:
        client_kwargs["key"] = cds_key

    client = cdsapi.Client(**client_kwargs)

    request = {
        "system_version": [system_version],
        "hydrological_model": ["lisflood"],
        "product_type": ["control_forecast", "ensemble_perturbed_forecasts"],
        "variable": "river_discharge_in_the_last_24_hours",
        "year": date[:4],
        "month": date[4:6],
        "day": date[6:8],
        "leadtime_hour": [str(h) for h in leadtime_hours],
        "data_format": data_format,
    }

    if area:
        request["area"] = area

    Path(target_path).parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Downloading GloFAS forecast",
        extra={"date": date, "leadtimes": len(leadtime_hours), "target": target_path},
    )
    client.retrieve("cems-glofas-forecast", request, target_path)

    return target_path


def download_glofas_reanalysis(
    *,
    year: str,
    month: str,
    target_path: str,
    system_version: str = "version_4_0",
    cds_url: str | None = None,
    cds_key: str | None = None,
) -> str:
    """Download GloFAS ERA5 reanalysis (historical discharge) for return period computation."""
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
        "year": year,
        "month": month,
        "day": [str(d).zfill(2) for d in range(1, 32)],
        "data_format": "grib",
    }

    Path(target_path).parent.mkdir(parents=True, exist_ok=True)
    client.retrieve("cems-glofas-historical", request, target_path)

    return target_path


def open_glofas_grib(path: str):
    """Open a GloFAS GRIB file as an xarray Dataset."""
    import xarray as xr

    return xr.open_dataset(path, engine="cfgrib")


def open_glofas_grib_ensemble(path: str):
    """Open a GloFAS ensemble GRIB as a list of xarray Datasets.

    GloFAS GRIB files contain multiple product types (control forecast 'cf'
    and perturbed forecast 'pf') that must be opened separately using
    cfgrib filter_by_keys.
    """
    import xarray as xr

    datasets: list = []
    for data_type in ("cf", "pf"):
        try:
            ds = xr.open_dataset(
                path,
                engine="cfgrib",
                backend_kwargs={"filter_by_keys": {"dataType": data_type}},
            )
            datasets.append(ds)
        except Exception:
            logger.debug("No '%s' messages in GRIB %s", data_type, path)

    if datasets:
        return datasets

    # Fallback: try opening without filters (single-type GRIB)
    return [xr.open_dataset(path, engine="cfgrib")]
