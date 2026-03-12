from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

from app.forecast.exceptions import ForecastValidationError, ProviderOperationalError
from app.forecast.schemas import ReturnPeriodSchema

_VALID_RETURN_PERIODS = (2, 5, 10, 25, 50, 100)
_VALID_METHODS = {"gumbel", "logpearson3"}


def iter_geoglows_return_periods_from_zarr(
    zarr_path: str,
    method: str = "gumbel",
    batch_size: int = 10000,
) -> Iterator[list[ReturnPeriodSchema]]:
    normalized_method = _normalize_method(method)
    if batch_size <= 0:
        raise ForecastValidationError("batch_size must be > 0")

    xr = _import_xarray()
    try:
        ds = xr.open_zarr(zarr_path, consolidated=False)
    except Exception as exc:
        raise ProviderOperationalError(f"Failed to open GEOGLOWS Zarr dataset at '{zarr_path}': {exc}") from exc

    _validate_zarr_dataset(ds, normalized_method)

    total_reaches = int(ds.sizes["river_id"])
    for start in range(0, total_reaches, batch_size):
        end = min(start + batch_size, total_reaches)
        chunk = ds.isel(river_id=slice(start, end))
        yield _chunk_to_return_period_rows(
            chunk=chunk,
            method=normalized_method,
            zarr_path=zarr_path,
            start=start,
            end=end,
        )


def load_geoglows_return_periods_from_path(dataset_path: str | Path) -> list[ReturnPeriodSchema]:
    """Backward-compatible local flat-file loader for smaller datasets."""
    path = Path(dataset_path)
    if not path.exists():
        raise ForecastValidationError(f"Return-period dataset path does not exist: {path}")

    pd = _import_pandas()
    if path.suffix.lower() in {".parquet", ".pq"}:
        df = pd.read_parquet(path)
    elif path.suffix.lower() in {".csv", ".txt"}:
        df = pd.read_csv(path)
    else:
        raise ForecastValidationError(
            f"Unsupported return-period dataset extension '{path.suffix}'. Use .csv or .parquet"
        )

    return _parse_geoglows_return_period_dataframe(df, path)


def _chunk_to_return_period_rows(
    chunk: Any,
    method: str,
    zarr_path: str,
    start: int,
    end: int,
) -> list[ReturnPeriodSchema]:
    river_ids = chunk["river_id"].values
    return_periods = [int(x) for x in chunk["return_period"].values.tolist()]

    values = chunk[method].transpose("river_id", "return_period").values
    max_simulated_values = None
    if "max_simulated" in chunk.data_vars:
        max_simulated_values = chunk["max_simulated"].values

    return _rows_from_matrix(
        river_ids=river_ids,
        return_periods=return_periods,
        matrix=values,
        method=method,
        zarr_path=zarr_path,
        max_simulated_values=max_simulated_values,
        start_index=start,
        end_index=end,
    )


def _rows_from_matrix(
    river_ids: Any,
    return_periods: list[int],
    matrix: Any,
    method: str,
    zarr_path: str,
    max_simulated_values: Any,
    start_index: int,
    end_index: int,
) -> list[ReturnPeriodSchema]:
    if tuple(sorted(return_periods)) != _VALID_RETURN_PERIODS:
        raise ForecastValidationError(
            "Unexpected return_period coordinate values. Expected exactly: 2, 5, 10, 25, 50, 100"
        )

    rp_index = {period: idx for idx, period in enumerate(return_periods)}
    rows: list[ReturnPeriodSchema] = []
    for idx, reach_id in enumerate(river_ids):
        provider_reach_id = _normalize_reach_id(reach_id)
        if provider_reach_id is None:
            continue

        metadata = {
            "source": "geoglows_zarr",
            "path": zarr_path,
            "method": method,
            "chunk_start": start_index,
            "chunk_end": end_index,
        }
        if max_simulated_values is not None:
            metadata["max_simulated"] = _safe_float(max_simulated_values[idx])

        rows.append(
            ReturnPeriodSchema(
                provider="geoglows",
                provider_reach_id=provider_reach_id,
                rp_2=_safe_float(matrix[idx][rp_index[2]]),
                rp_5=_safe_float(matrix[idx][rp_index[5]]),
                rp_10=_safe_float(matrix[idx][rp_index[10]]),
                rp_25=_safe_float(matrix[idx][rp_index[25]]),
                rp_50=_safe_float(matrix[idx][rp_index[50]]),
                rp_100=_safe_float(matrix[idx][rp_index[100]]),
                metadata_json=metadata,
            )
        )
    return rows


def _validate_zarr_dataset(ds: Any, method: str) -> None:
    if "river_id" not in ds.dims or "return_period" not in ds.dims:
        raise ForecastValidationError(
            "GEOGLOWS Zarr dataset must include dimensions 'river_id' and 'return_period'."
        )

    if method not in ds.data_vars:
        raise ForecastValidationError(
            f"Method '{method}' not found in dataset variables. Available variables: {sorted(ds.data_vars.keys())}"
        )


def _normalize_method(method: str) -> str:
    value = str(method).strip().lower()
    if value not in _VALID_METHODS:
        raise ForecastValidationError(
            f"Invalid return period method '{method}'. Supported methods: gumbel, logpearson3"
        )
    return value


def _import_xarray():
    try:
        import xarray as xr
    except ModuleNotFoundError as exc:
        raise ProviderOperationalError(
            "xarray is required to import GEOGLOWS return periods from Zarr."
        ) from exc
    return xr


def _import_pandas():
    import pandas as pd

    return pd


def _parse_geoglows_return_period_dataframe(df: Any, source_path: Path) -> list[ReturnPeriodSchema]:
    reach_id_column = _resolve_column(df.columns, ("provider_reach_id", "river_id", "rivid", "reach_id"))
    if not reach_id_column:
        raise ForecastValidationError(
            "Unable to find GEOGLOWS reach ID column. Supported names: provider_reach_id, river_id, rivid, reach_id"
        )

    rp_columns = {
        "rp_2": _resolve_column(df.columns, ("rp_2", "return_period_2", "rp2", "q2")),
        "rp_5": _resolve_column(df.columns, ("rp_5", "return_period_5", "rp5", "q5")),
        "rp_10": _resolve_column(df.columns, ("rp_10", "return_period_10", "rp10", "q10")),
        "rp_25": _resolve_column(df.columns, ("rp_25", "return_period_25", "rp25", "q25")),
        "rp_50": _resolve_column(df.columns, ("rp_50", "return_period_50", "rp50", "q50")),
        "rp_100": _resolve_column(df.columns, ("rp_100", "return_period_100", "rp100", "q100")),
    }

    rows: list[ReturnPeriodSchema] = []
    for idx, row in df.iterrows():
        reach_id = _normalize_reach_id(row.get(reach_id_column))
        if reach_id is None:
            continue
        rows.append(
            ReturnPeriodSchema(
                provider="geoglows",
                provider_reach_id=reach_id,
                rp_2=_safe_float(row.get(rp_columns["rp_2"])) if rp_columns["rp_2"] else None,
                rp_5=_safe_float(row.get(rp_columns["rp_5"])) if rp_columns["rp_5"] else None,
                rp_10=_safe_float(row.get(rp_columns["rp_10"])) if rp_columns["rp_10"] else None,
                rp_25=_safe_float(row.get(rp_columns["rp_25"])) if rp_columns["rp_25"] else None,
                rp_50=_safe_float(row.get(rp_columns["rp_50"])) if rp_columns["rp_50"] else None,
                rp_100=_safe_float(row.get(rp_columns["rp_100"])) if rp_columns["rp_100"] else None,
                metadata_json={"source": "local_file", "path": str(source_path), "row_number": int(idx) + 1},
            )
        )
    return rows


def _resolve_column(columns: Any, candidates: tuple[str, ...]) -> str | None:
    normalized = {str(c).strip().lower(): str(c) for c in columns}
    for option in candidates:
        if option in normalized:
            return normalized[option]
    return None


def _normalize_reach_id(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"", "nan", "none", "null"}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
