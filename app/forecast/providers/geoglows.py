from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import shutil

import json
import math
import pandas as pd

from app.core.config import Settings
from app.forecast.base import ForecastProviderAdapter
from app.forecast.classify import classify_peak_flow
from app.forecast.exceptions import (
    ForecastValidationError,
    ProviderBackendUnavailableError,
    ProviderOperationalError,
)
from app.forecast.schemas import (
    BulkForecastArtifactRowSchema,
    ForecastRunSchema,
    ReachSummarySchema,
    ReturnPeriodSchema,
    TimeseriesPointSchema,
)


@dataclass(frozen=True)
class GeoglowsCapabilities:
    supports_forecast_stats_rest: bool = True
    supports_forecast_stats_aws: bool = True
    supports_return_periods_rest: bool = False
    supports_return_periods_aws: bool = True


class GeoglowsForecastProvider(ForecastProviderAdapter):
    def __init__(self, settings: Settings, geoglows_module: Any | None = None) -> None:
        self.settings = settings
        self._geoglows = geoglows_module
        self.capabilities = GeoglowsCapabilities()

    def get_provider_name(self) -> str:
        return "geoglows"

    def discover_latest_run(self) -> ForecastRunSchema:
        run_date = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        run_id = run_date.strftime("%Y%m%d%H")
        return ForecastRunSchema(
            provider=self.get_provider_name(),
            run_id=run_id,
            run_date_utc=run_date,
            issued_at_utc=run_date,
            source_type=self.settings.geoglows_source_type,
            ingest_status="pending",
            metadata_json={"selector": self.settings.geoglows_default_run_selector},
        )

    def fetch_return_periods(self, reach_ids: list[str | int]) -> list[ReturnPeriodSchema]:
        normalized_ids = _validate_geoglows_reach_ids(reach_ids)
        forecast_source = self.settings.geoglows_data_source.lower()
        if forecast_source == "rest" and not self.capabilities.supports_return_periods_rest:
            raise ProviderBackendUnavailableError(
                "GEOGLOWS return periods are not supported in REST mode. "
                "This operation requires retrospective/AWS-backed access."
            )

        fn = self._resolve_geoglows_callable("return_periods")
        try:
            data = fn(river_id=normalized_ids)
        except Exception as exc:
            if _looks_like_network_error(exc):
                raise ProviderBackendUnavailableError(
                    "GEOGLOWS return periods require retrospective/AWS access, but that backend is unreachable "
                    "from this environment."
                ) from exc
            raise ProviderOperationalError(f"GEOGLOWS return_periods failed: {exc}") from exc

        if isinstance(data, pd.Series):
            data = data.to_frame().T

        output: list[ReturnPeriodSchema] = []
        for idx, row in data.iterrows():
            reach_id = str(row.get("rivid", row.get("river_id", idx)))
            output.append(
                ReturnPeriodSchema(
                    provider=self.get_provider_name(),
                    provider_reach_id=reach_id,
                    rp_2=_safe_float(row.get("return_period_2")),
                    rp_5=_safe_float(row.get("return_period_5")),
                    rp_10=_safe_float(row.get("return_period_10")),
                    rp_25=_safe_float(row.get("return_period_25")),
                    rp_50=_safe_float(row.get("return_period_50")),
                    rp_100=_safe_float(row.get("return_period_100")),
                    metadata_json={"source": "geoglows.return_periods", "backend": "retrospective_aws"},
                )
            )
        return output

    def fetch_forecast_timeseries(
        self, run_id: str, reach_ids: list[str | int]
    ) -> list[TimeseriesPointSchema]:
        normalized_ids = _validate_geoglows_reach_ids(reach_ids)
        source = self.settings.geoglows_data_source.lower()
        if source not in {"rest", "aws"}:
            raise ForecastValidationError(
                f"Invalid GEOGLOWS_DATA_SOURCE '{self.settings.geoglows_data_source}'. Use 'rest' or 'aws'."
            )

        fn = self._resolve_geoglows_callable("forecast_stats")
        rows: list[TimeseriesPointSchema] = []
        for reach_id in normalized_ids:
            try:
                df = fn(river_id=reach_id, data_source=source)
            except Exception as exc:
                if _looks_like_network_error(exc):
                    raise ProviderBackendUnavailableError(
                        "GEOGLOWS forecast_stats backend is unreachable from this environment."
                    ) from exc
                raise ProviderOperationalError(f"GEOGLOWS forecast_stats failed for river_id={reach_id}: {exc}") from exc

            if isinstance(df.index, pd.DatetimeIndex):
                iter_rows = df.reset_index(names="forecast_time_utc").to_dict(orient="records")
            else:
                iter_rows = df.to_dict(orient="records")

            for item in iter_rows:
                dt = item.get("forecast_time_utc") or item.get("time")
                if not isinstance(dt, datetime):
                    dt = datetime.fromisoformat(str(dt)).replace(tzinfo=UTC)
                rows.append(
                    TimeseriesPointSchema(
                        provider=self.get_provider_name(),
                        run_id=run_id,
                        provider_reach_id=str(reach_id),
                        forecast_time_utc=dt,
                        flow_mean_cms=_safe_float(item.get("flow_avg")),
                        flow_median_cms=_safe_float(item.get("flow_med")),
                        flow_p25_cms=_safe_float(item.get("flow_25p")),
                        flow_p75_cms=_safe_float(item.get("flow_75p")),
                        flow_max_cms=_safe_float(item.get("flow_max")),
                        raw_payload_json={
                            "provider_row": {k: str(v) for k, v in item.items()},
                            "source": source,
                            "flow_min": _safe_float(item.get("flow_min")),
                            "high_res": _safe_float(item.get("high_res")),
                        },
                    )
                )
        return rows

    def supports_bulk_acquisition(self) -> bool:
        mode = self.bulk_acquisition_mode()
        if mode == "manual_artifact_only":
            return False
        if mode == "local_file":
            source = self._bulk_raw_source_uri()
            return bool(source and Path(source).exists())
        if mode == "remote_http":
            return bool(self.settings.geoglows_bulk_raw_source_uri)
        if mode == "remote_object_store":
            return bool(self.settings.geoglows_bulk_raw_source_uri)
        return False

    def bulk_acquisition_mode(self) -> str:
        return (self.settings.geoglows_bulk_acquisition_mode or "manual_artifact_only").strip().lower()

    def is_bulk_source_reachable(self) -> bool | None:
        mode = self.bulk_acquisition_mode()
        source = self._bulk_raw_source_uri()
        if mode == "manual_artifact_only":
            return None
        if not source:
            return False
        if mode == "local_file":
            return Path(source).exists()
        if mode == "remote_http":
            return source.startswith("http://") or source.startswith("https://")
        if mode == "remote_object_store":
            return source.startswith("s3://")
        return False

    def acquire_bulk_raw_source(self, run_id: str, overwrite: bool = False) -> str:
        mode = self.bulk_acquisition_mode()
        source = self._bulk_raw_source_uri()
        if mode == "manual_artifact_only":
            raise ProviderBackendUnavailableError(
                "GEOGLOWS acquisition mode is manual_artifact_only. Provide normalized artifact directly and skip acquisition."
            )
        if not source:
            raise ProviderBackendUnavailableError("GEOGLOWS_BULK_RAW_SOURCE_URI must be configured for acquisition modes.")

        destination = self._staged_raw_path(run_id)
        destination.parent.mkdir(parents=True, exist_ok=True)

        if destination.exists() and not overwrite and not self.settings.geoglows_bulk_overwrite_existing_raw:
            return str(destination)

        if mode == "local_file":
            source_path = Path(source)
            if not source_path.exists():
                raise ProviderOperationalError(f"GEOGLOWS local raw source does not exist: {source}")
            shutil.copyfile(source_path, destination)
        elif mode == "remote_http":
            self._download_http_source(run_id=run_id, source=source, destination=destination)
        elif mode == "remote_object_store":
            raise ProviderOperationalError(
                "GEOGLOWS remote_object_store acquisition mode is declared but not yet implemented in this runtime. "
                "Use local_file/remote_http or pre-stage data and switch to local_file/manual_artifact_only."
            )
        else:
            raise ProviderOperationalError(
                f"Unsupported GEOGLOWS acquisition mode '{mode}'. Supported: manual_artifact_only, local_file, remote_http, remote_object_store"
            )

        return str(destination)

    def iter_raw_bulk_records(self, run_id: str, staged_raw_path: str) -> Iterator[dict]:
        _ = run_id
        source = Path(staged_raw_path)
        if not source.exists():
            raise ProviderOperationalError(f"Staged GEOGLOWS raw source does not exist: {staged_raw_path}")

        with source.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                text = line.strip()
                if not text:
                    continue
                try:
                    item = json.loads(text)
                except json.JSONDecodeError as exc:
                    raise ProviderOperationalError(
                        f"Invalid GEOGLOWS raw JSON at line {line_number}: {exc}"
                    ) from exc
                item["_line_number"] = line_number
                yield item

    def normalize_bulk_record(self, run_id: str, record: dict) -> BulkForecastArtifactRowSchema | None:
        reach_id = str(record.get("provider_reach_id", record.get("river_id", ""))).strip()
        if not reach_id:
            return None

        dt = record.get("forecast_time_utc") or record.get("time")
        if dt is None:
            return None
        if isinstance(dt, datetime):
            forecast_time = dt
        else:
            forecast_time = datetime.fromisoformat(str(dt)).replace(tzinfo=UTC)

        return BulkForecastArtifactRowSchema(
            provider=self.get_provider_name(),
            run_id=run_id,
            provider_reach_id=reach_id,
            forecast_time_utc=forecast_time,
            flow_mean_cms=_safe_float(record.get("flow_avg", record.get("flow_mean_cms"))),
            flow_median_cms=_safe_float(record.get("flow_med", record.get("flow_median_cms"))),
            flow_p25_cms=_safe_float(record.get("flow_25p", record.get("flow_p25_cms"))),
            flow_p75_cms=_safe_float(record.get("flow_75p", record.get("flow_p75_cms"))),
            flow_max_cms=_safe_float(record.get("flow_max", record.get("flow_max_cms"))),
            raw_payload_json={
                "source": "geoglows_raw_bulk",
                "line_number": record.get("_line_number"),
                "raw_record": record,
            },
        )

    def cleanup_old_raw_staging(self) -> int:
        keep_latest = self.settings.geoglows_bulk_raw_retention_runs
        if keep_latest < 1:
            return 0
        base = Path(self.settings.geoglows_bulk_staging_dir)
        provider_dir = base / self.get_provider_name()
        if not provider_dir.exists():
            return 0
        files = sorted(provider_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
        removed = 0
        for path in files[keep_latest:]:
            path.unlink(missing_ok=True)
            removed += 1
        return removed

    def _staged_raw_path(self, run_id: str) -> Path:
        base = Path(self.settings.geoglows_bulk_staging_dir)
        provider_dir = base / self.get_provider_name()
        provider_dir.mkdir(parents=True, exist_ok=True)
        safe_run_id = run_id.replace("/", "_")
        return provider_dir / f"{self.get_provider_name()}_{safe_run_id}.jsonl"

    def _download_http_source(self, run_id: str, source: str, destination: Path) -> None:
        resolved_source = source.format(run_id=run_id)
        parsed = urlparse(resolved_source)
        if parsed.scheme not in {"http", "https"}:
            raise ProviderOperationalError(f"Invalid GEOGLOWS HTTP source URL: {resolved_source}")

        headers = {"User-Agent": "codex-geoflows-bulk-acquisition/1.0"}
        if self.settings.geoglows_bulk_remote_auth_token:
            headers["Authorization"] = f"Bearer {self.settings.geoglows_bulk_remote_auth_token}"

        attempts = self.settings.geoglows_bulk_download_max_retries + 1
        last_exc: Exception | None = None
        for _ in range(attempts):
            try:
                req = Request(resolved_source, headers=headers)
                with urlopen(req, timeout=self.settings.geoglows_bulk_download_timeout_seconds) as resp:
                    with destination.open("wb") as out:
                        shutil.copyfileobj(resp, out)
                return
            except Exception as exc:
                last_exc = exc
        raise ProviderOperationalError(
            f"Failed downloading GEOGLOWS bulk source from {resolved_source} after {attempts} attempt(s): {last_exc}"
        ) from last_exc

    def _bulk_raw_source_uri(self) -> str | None:
        return self.settings.geoglows_bulk_raw_source_uri or self.settings.geoglows_bulk_forecast_source

    def summarize_reach(
        self,
        run_id: str,
        reach_id: str | int,
        timeseries_rows: list[TimeseriesPointSchema],
        return_period_row: ReturnPeriodSchema | None,
    ) -> ReachSummarySchema:
        peak_row = max(
            timeseries_rows,
            key=lambda r: _first_not_none(r.flow_max_cms, r.flow_mean_cms, r.flow_median_cms, -1.0),
            default=None,
        )

        peak_mean = max((r.flow_mean_cms for r in timeseries_rows if r.flow_mean_cms is not None), default=None)
        peak_median = max((r.flow_median_cms for r in timeseries_rows if r.flow_median_cms is not None), default=None)
        peak_max = max((r.flow_max_cms for r in timeseries_rows if r.flow_max_cms is not None), default=None)

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
            metadata_json={"points": len(timeseries_rows)},
        )

    def _get_geoglows(self) -> Any:
        if self._geoglows is not None:
            return self._geoglows
        try:
            import geoglows as geoglows_module
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise ProviderOperationalError("geoglows package is required for GEOGLOWS ingestion.") from exc
        self._geoglows = geoglows_module
        return geoglows_module

    def _resolve_geoglows_callable(self, function_name: str) -> Callable[..., Any]:
        geoglows = self._get_geoglows()
        candidates = [geoglows, getattr(geoglows, "streamflow", None), getattr(geoglows, "data", None)]
        for candidate in candidates:
            if candidate is None:
                continue
            fn = getattr(candidate, function_name, None)
            if callable(fn):
                return fn
        raise ProviderOperationalError(
            f"GEOGLOWS package does not expose '{function_name}' in expected namespaces (top-level, streamflow, data)."
        )


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip().lower()
        if stripped in {"", "nan", "none", "null"}:
            return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number):
        return None
    return number


def _first_not_none(*values: float | None) -> float | None:
    for v in values:
        if v is not None:
            return v
    return None


def _validate_geoglows_reach_ids(reach_ids: list[str | int]) -> list[int]:
    normalized: list[int] = []
    for raw in reach_ids:
        text = str(raw).strip()
        if not text.isdigit() or len(text) != 9:
            raise ForecastValidationError(
                f"Invalid GEOGLOWS river_id '{raw}'. GEOGLOWS IDs must be 9-digit numeric values."
            )
        normalized.append(int(text))
    return normalized


def _looks_like_network_error(exc: Exception) -> bool:
    text = str(exc).lower()
    tokens = [
        "could not connect to the endpoint url",
        "getaddrinfo failed",
        "name or service not known",
        "temporary failure in name resolution",
        "non-existent domain",
        "nxdomain",
        "clientconnectordnserror",
    ]
    return any(token in text for token in tokens)
