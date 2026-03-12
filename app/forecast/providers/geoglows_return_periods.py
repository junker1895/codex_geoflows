from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.forecast.exceptions import ForecastValidationError
from app.forecast.schemas import ReturnPeriodSchema


_REACH_ID_CANDIDATES = ("provider_reach_id", "river_id", "rivid", "reach_id")
_RETURN_PERIOD_COLUMN_CANDIDATES = {
    "rp_2": ("rp_2", "return_period_2", "rp2", "q2"),
    "rp_5": ("rp_5", "return_period_5", "rp5", "q5"),
    "rp_10": ("rp_10", "return_period_10", "rp10", "q10"),
    "rp_25": ("rp_25", "return_period_25", "rp25", "q25"),
    "rp_50": ("rp_50", "return_period_50", "rp50", "q50"),
    "rp_100": ("rp_100", "return_period_100", "rp100", "q100"),
}


def load_geoglows_return_periods_from_path(dataset_path: str | Path) -> list[ReturnPeriodSchema]:
    path = Path(dataset_path)
    if not path.exists():
        raise ForecastValidationError(f"Return-period dataset path does not exist: {path}")

    if path.suffix.lower() in {".parquet", ".pq"}:
        df = pd.read_parquet(path)
    elif path.suffix.lower() in {".csv", ".txt"}:
        df = pd.read_csv(path)
    else:
        raise ForecastValidationError(
            f"Unsupported return-period dataset extension '{path.suffix}'. Use .csv or .parquet"
        )

    return _parse_geoglows_return_period_dataframe(df, path)


def _parse_geoglows_return_period_dataframe(df: pd.DataFrame, source_path: Path) -> list[ReturnPeriodSchema]:
    reach_id_column = _resolve_column(df, _REACH_ID_CANDIDATES)
    if not reach_id_column:
        raise ForecastValidationError(
            "Unable to find GEOGLOWS reach ID column. Supported names: "
            + ", ".join(_REACH_ID_CANDIDATES)
        )

    rp_column_map = {
        key: _resolve_column(df, options) for key, options in _RETURN_PERIOD_COLUMN_CANDIDATES.items()
    }
    if not any(rp_column_map.values()):
        raise ForecastValidationError(
            "Unable to find GEOGLOWS return-period columns. Supported names include: "
            "return_period_2, return_period_5, return_period_10, return_period_25, return_period_50, return_period_100"
        )

    rows: list[ReturnPeriodSchema] = []
    for idx, row in df.iterrows():
        reach_id = _normalize_reach_id(row.get(reach_id_column))
        if reach_id is None:
            continue

        metadata = {
            "source": "local_file",
            "path": str(source_path),
            "row_number": int(idx) + 1,
        }

        rows.append(
            ReturnPeriodSchema(
                provider="geoglows",
                provider_reach_id=reach_id,
                rp_2=_safe_float(row.get(rp_column_map["rp_2"])) if rp_column_map["rp_2"] else None,
                rp_5=_safe_float(row.get(rp_column_map["rp_5"])) if rp_column_map["rp_5"] else None,
                rp_10=_safe_float(row.get(rp_column_map["rp_10"])) if rp_column_map["rp_10"] else None,
                rp_25=_safe_float(row.get(rp_column_map["rp_25"])) if rp_column_map["rp_25"] else None,
                rp_50=_safe_float(row.get(rp_column_map["rp_50"])) if rp_column_map["rp_50"] else None,
                rp_100=_safe_float(row.get(rp_column_map["rp_100"])) if rp_column_map["rp_100"] else None,
                metadata_json=metadata,
            )
        )
    return rows


def _resolve_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    normalized = {str(c).strip().lower(): str(c) for c in df.columns}
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
