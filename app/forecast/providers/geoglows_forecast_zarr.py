from __future__ import annotations

from datetime import UTC, datetime
import re
from typing import Any

import numpy as np

from app.forecast.exceptions import ForecastValidationError, ProviderOperationalError

RUN_ID_PATTERN = re.compile(r"^(\d{10})$")


def build_geoglows_public_forecast_storage_options(region: str = "us-west-2", use_anon: bool = True) -> dict[str, Any]:
    return {
        "anon": bool(use_anon),
        "client_kwargs": {"region_name": region},
    }


def build_geoglows_forecast_run_zarr_uri(bucket: str, run_id: str, run_suffix: str = ".zarr") -> str:
    return f"s3://{bucket}/{run_id}{run_suffix}"


def parse_run_id_from_entry(entry: str, run_suffix: str = ".zarr") -> str | None:
    if not entry:
        return None
    name = str(entry).strip().split("/")[-1]
    if not name:
        return None
    if run_suffix and name.endswith(run_suffix):
        name = name[: -len(run_suffix)]
    if RUN_ID_PATTERN.match(name):
        return name
    return None


def list_forecast_run_ids_from_paths(paths: list[str], run_suffix: str = ".zarr") -> list[str]:
    parsed = {run_id for path in paths if (run_id := parse_run_id_from_entry(path, run_suffix))}
    return sorted(parsed)


def discover_latest_forecast_run_id(
    *,
    s3fs_module: Any,
    bucket: str,
    region: str,
    use_anon: bool,
    run_suffix: str = ".zarr",
) -> str:
    fs = s3fs_module.S3FileSystem(**build_geoglows_public_forecast_storage_options(region=region, use_anon=use_anon))
    entries = fs.ls(bucket, detail=False)
    run_ids = list_forecast_run_ids_from_paths(entries, run_suffix=run_suffix)
    if not run_ids:
        raise ProviderOperationalError(
            f"No GEOGLOWS forecast runs were found in bucket s3://{bucket}/ with suffix '{run_suffix}'."
        )
    return run_ids[-1]


def run_exists(
    *,
    s3fs_module: Any,
    bucket: str,
    region: str,
    use_anon: bool,
    run_id: str,
    run_suffix: str = ".zarr",
) -> bool:
    fs = s3fs_module.S3FileSystem(**build_geoglows_public_forecast_storage_options(region=region, use_anon=use_anon))
    return fs.exists(f"{bucket}/{run_id}{run_suffix}")


def open_geoglows_public_forecast_run_zarr(
    *,
    xr: Any,
    run_id: str,
    bucket: str,
    region: str,
    use_anon: bool,
    run_suffix: str = ".zarr",
) -> Any:
    uri = build_geoglows_forecast_run_zarr_uri(bucket=bucket, run_id=run_id, run_suffix=run_suffix)
    storage_options = build_geoglows_public_forecast_storage_options(region=region, use_anon=use_anon)
    return xr.open_zarr(uri, storage_options=storage_options)


def detect_forecast_structure(ds: Any, forecast_variable: str) -> dict[str, Any]:
    if forecast_variable not in ds.data_vars:
        raise ForecastValidationError(
            f"Forecast variable '{forecast_variable}' not found. Available variables: {sorted(ds.data_vars.keys())}"
        )

    qout = ds[forecast_variable]
    dims = list(qout.dims)
    if not dims:
        raise ForecastValidationError(f"Forecast variable '{forecast_variable}' has no dimensions.")

    time_dim = _detect_time_dim(ds, qout)
    reach_dim = _detect_reach_dim(ds, qout, exclude={time_dim})
    ensemble_dims = [dim for dim in dims if dim not in {time_dim, reach_dim}]

    return {
        "forecast_variable": forecast_variable,
        "dims": dims,
        "time_dim": time_dim,
        "reach_dim": reach_dim,
        "ensemble_dims": ensemble_dims,
        "chunking": dataarray_chunking(qout),
        "coords": sorted(ds.coords.keys()),
        "attrs": dict(ds.attrs),
    }


def dataarray_chunking(data_array: Any) -> dict[str, list[int]]:
    chunks = getattr(data_array, "chunks", None)
    if not chunks:
        return {}
    return {
        dim: [int(x) for x in dim_chunks]
        for dim, dim_chunks in zip(data_array.dims, chunks, strict=False)
    }


def chunk_aligned_windows(dim_size: int, chunk_sizes: list[int] | tuple[int, ...] | None) -> list[tuple[int, int]]:
    if dim_size <= 0:
        return []
    if not chunk_sizes:
        return [(0, dim_size)]

    windows: list[tuple[int, int]] = []
    start = 0
    for chunk in chunk_sizes:
        if start >= dim_size:
            break
        width = int(chunk)
        if width <= 0:
            continue
        end = min(start + width, dim_size)
        windows.append((start, end))
        start = end

    if start < dim_size:
        windows.append((start, dim_size))
    return windows


def describe_forecast_dataset(ds: Any, forecast_variable: str) -> dict[str, Any]:
    structure = detect_forecast_structure(ds, forecast_variable)
    qout = ds[forecast_variable]
    return {
        "dims": {k: int(v) for k, v in ds.sizes.items()},
        "coords": sorted(ds.coords.keys()),
        "data_vars": sorted(ds.data_vars.keys()),
        "forecast_variable": forecast_variable,
        "forecast_dims": list(qout.dims),
        "chunking": dataarray_chunking(qout),
        "detected_time_dim": structure["time_dim"],
        "detected_reach_dim": structure["reach_dim"],
        "detected_ensemble_dims": structure["ensemble_dims"],
    }


def _detect_time_dim(ds: Any, qout: Any) -> str:
    candidates: list[str] = []
    for dim in qout.dims:
        coord = ds.coords.get(dim)
        dtype = getattr(getattr(coord, "dtype", None), "kind", "")
        if dtype in {"M", "m"}:
            candidates.append(dim)
    if candidates:
        return candidates[0]

    for dim in qout.dims:
        lowered = dim.lower()
        if "time" in lowered or lowered in {"datetime", "date"}:
            return dim
    raise ForecastValidationError(f"Could not detect time dimension in Qout dims={qout.dims}.")


def _detect_reach_dim(ds: Any, qout: Any, exclude: set[str]) -> str:
    preferred_tokens = ("river", "reach", "rivid", "comid", "feature_id", "id")
    for dim in qout.dims:
        if dim in exclude:
            continue
        lowered = dim.lower()
        if any(token in lowered for token in preferred_tokens):
            return dim

    for dim in qout.dims:
        if dim not in exclude:
            return dim

    raise ForecastValidationError(f"Could not detect reach dimension in Qout dims={qout.dims}.")


def to_utc_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, np.datetime64):
        seconds = value.astype("datetime64[s]").astype(int)
        return datetime.fromtimestamp(int(seconds), tz=UTC)
    return datetime.fromisoformat(str(value)).replace(tzinfo=UTC)
