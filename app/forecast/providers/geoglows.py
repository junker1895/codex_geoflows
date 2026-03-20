from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
import logging
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import shutil

import json
import math
import numpy as np
import pandas as pd

from app.core.config import Settings
from app.forecast.base import ForecastProviderAdapter
from app.forecast.classify import classify_peak_flow
from app.forecast.exceptions import (
    ForecastValidationError,
    ProviderBackendUnavailableError,
    ProviderOperationalError,
)
from app.forecast.providers.geoglows_forecast_zarr import (
    build_geoglows_forecast_run_zarr_uri,
    chunk_aligned_windows,
    describe_forecast_dataset,
    dataarray_chunking,
    detect_forecast_structure,
    discover_latest_forecast_run_id,
    open_geoglows_public_forecast_run_zarr,
    run_exists,
    to_utc_datetime,
)

logger = logging.getLogger(__name__)
from app.forecast.schemas import (
    BulkForecastArtifactRowSchema,
    BulkForecastSummaryArtifactRowSchema,
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
        self._supported_reach_filter: set[str] | None = None

    def get_provider_name(self) -> str:
        return "geoglows"

    def discover_latest_run(self) -> ForecastRunSchema:
        run_id = self.get_latest_upstream_run_id()
        run_date = datetime.strptime(run_id, "%Y%m%d%H").replace(tzinfo=UTC)
        return ForecastRunSchema(
            provider=self.get_provider_name(),
            run_id=run_id,
            run_date_utc=run_date,
            issued_at_utc=run_date,
            source_type=self.settings.geoglows_source_type,
            ingest_status="pending",
            metadata_json={
                "selector": self.settings.geoglows_default_run_selector,
                "upstream": {
                    "bucket": self.settings.geoglows_forecast_bucket,
                    "region": self.settings.geoglows_forecast_region,
                    "source_zarr_path": self.build_source_zarr_path(run_id),
                },
            },
        )

    def get_latest_upstream_run_id(self) -> str:
        if self.bulk_acquisition_mode() == "aws_public_zarr":
            try:
                return discover_latest_forecast_run_id(
                    s3fs_module=self._import_s3fs(),
                    bucket=self.settings.geoglows_forecast_bucket,
                    region=self.settings.geoglows_forecast_region,
                    use_anon=self.settings.geoglows_forecast_use_anon,
                    run_suffix=self.settings.geoglows_forecast_run_suffix,
                )
            except Exception:
                pass
        # fallback only when discovery is unavailable
        return datetime.now(UTC).strftime("%Y%m%d") + "00"

    def build_source_zarr_path(self, run_id: str) -> str:
        return build_geoglows_forecast_run_zarr_uri(
            bucket=self.settings.geoglows_forecast_bucket,
            run_id=run_id,
            run_suffix=self.settings.geoglows_forecast_run_suffix,
        )

    def upstream_run_exists(self, run_id: str) -> bool | None:
        if self.bulk_acquisition_mode() != "aws_public_zarr":
            return None
        try:
            return run_exists(
                s3fs_module=self._import_s3fs(),
                bucket=self.settings.geoglows_forecast_bucket,
                region=self.settings.geoglows_forecast_region,
                use_anon=self.settings.geoglows_forecast_use_anon,
                run_id=run_id,
                run_suffix=self.settings.geoglows_forecast_run_suffix,
            )
        except Exception:
            return None

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

    def fetch_forecast_timeseries(self, run_id: str, reach_ids: list[str | int]) -> list[TimeseriesPointSchema]:
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
        if mode == "aws_public_zarr":
            return True
        if mode == "manual_artifact_only":
            return False
        if mode == "local_file":
            source = self._bulk_raw_source_uri()
            return bool(source and Path(source).exists())
        if mode in {"remote_http", "remote_object_store"}:
            return bool(self.settings.geoglows_bulk_raw_source_uri)
        return False

    def bulk_acquisition_mode(self) -> str:
        return (self.settings.geoglows_bulk_acquisition_mode or "manual_artifact_only").strip().lower()

    def is_bulk_source_reachable(self) -> bool | None:
        mode = self.bulk_acquisition_mode()
        source = self._bulk_raw_source_uri()
        if mode == "manual_artifact_only":
            return None
        if mode == "aws_public_zarr":
            latest = self.get_latest_upstream_run_id()
            return self.upstream_run_exists(latest)
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

        if mode == "aws_public_zarr":
            exists = self.upstream_run_exists(run_id)
            if exists is False:
                raise ProviderOperationalError(
                    f"GEOGLOWS upstream run does not exist: {self.build_source_zarr_path(run_id)}"
                )
            return self.build_source_zarr_path(run_id)

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
            raise ProviderOperationalError("GEOGLOWS remote_object_store is not implemented. Use aws_public_zarr instead.")
        else:
            raise ProviderOperationalError(
                "Unsupported GEOGLOWS acquisition mode. Supported: aws_public_zarr, manual_artifact_only, local_file, remote_http"
            )

        return str(destination)

    def iter_raw_bulk_records(self, run_id: str, staged_raw_path: str) -> Iterator[dict]:
        if self.bulk_acquisition_mode() == "aws_public_zarr":
            yield from self._iter_records_from_public_zarr(run_id)
            return

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

    def _iter_records_from_public_zarr(self, run_id: str) -> Iterator[dict]:
        xr = self._import_xarray()
        source_zarr_path = self.build_source_zarr_path(run_id)
        ds = open_geoglows_public_forecast_run_zarr(
            xr=xr,
            run_id=run_id,
            bucket=self.settings.geoglows_forecast_bucket,
            region=self.settings.geoglows_forecast_region,
            use_anon=self.settings.geoglows_forecast_use_anon,
            run_suffix=self.settings.geoglows_forecast_run_suffix,
        )
        structure = detect_forecast_structure(ds, self.settings.geoglows_forecast_variable)
        summary = describe_forecast_dataset(ds, self.settings.geoglows_forecast_variable)

        qout = ds[self.settings.geoglows_forecast_variable]
        time_values = ds[structure["time_dim"]].values
        reach_coord = ds[structure["reach_dim"]]
        reach_values = reach_coord.values
        ensemble_dims = structure["ensemble_dims"]

        # Confirmed upstream shape for GEOGLOWS runs is (ensemble, time, rivid), and
        # real dask chunks are currently observed as (52, 280, 686). We inspect chunking
        # dynamically and iterate contiguous reach windows aligned to chunk boundaries.
        chunking = dataarray_chunking(qout)
        reach_chunks = chunking.get(structure["reach_dim"])
        reach_windows = chunk_aligned_windows(len(reach_values), reach_chunks)

        ordered_dims = [*ensemble_dims, structure["time_dim"], structure["reach_dim"]]
        qout_view = qout.transpose(*ordered_dims)
        high_res_index = self._detect_high_res_member_index(qout_view=qout_view, ensemble_dims=ensemble_dims)

        start = datetime.now(UTC)
        rows_written = 0
        logger.info(
            "GEOGLOWS public Zarr artifact preparation started",
            extra={
                "run_id": run_id,
                "source_zarr_path": source_zarr_path,
                "total_rivid_count": int(len(reach_values)),
                "chunking": summary["chunking"],
                "detected_time_dim": structure["time_dim"],
                "detected_reach_dim": structure["reach_dim"],
                "detected_ensemble_dims": ensemble_dims,
                "total_blocks": len(reach_windows),
            },
        )

        supported_reaches = self._supported_reach_filter
        cumulative_matched_reaches = 0
        for block_idx, (reach_start, reach_end) in enumerate(reach_windows, start=1):
            block = qout_view.isel({structure["reach_dim"]: slice(reach_start, reach_end)})
            values = np.asarray(block.values, dtype=np.float32)

            if ensemble_dims:
                ensemble_axes = tuple(range(len(ensemble_dims)))
                mean_values = np.nanmean(values, axis=ensemble_axes)
                median_values = np.nanmedian(values, axis=ensemble_axes)
                p25_values = np.nanpercentile(values, 25, axis=ensemble_axes)
                p75_values = np.nanpercentile(values, 75, axis=ensemble_axes)
                max_values = np.nanmax(values, axis=ensemble_axes)
                high_res_values = (
                    None
                    if high_res_index is None
                    else np.asarray(np.take(values, indices=high_res_index, axis=0), dtype=np.float32)
                )
            else:
                mean_values = median_values = p25_values = p75_values = max_values = values
                high_res_values = values

            block_reach_ids = np.asarray(reach_values[reach_start:reach_end]).astype(str)
            reach_mask = np.ones(block_reach_ids.shape[0], dtype=bool)
            if supported_reaches is not None:
                reach_mask = np.fromiter((rid in supported_reaches for rid in block_reach_ids), dtype=bool)

            selected_idx = np.flatnonzero(reach_mask)
            matched_reach_count = int(selected_idx.size)
            cumulative_matched_reaches += matched_reach_count
            if selected_idx.size == 0:
                elapsed = (datetime.now(UTC) - start).total_seconds()
                logger.info(
                    "GEOGLOWS public Zarr artifact preparation progress",
                    extra={
                        "run_id": run_id,
                        "source_zarr_path": source_zarr_path,
                        "block_index": block_idx,
                        "total_blocks": len(reach_windows),
                        "rivid_start": reach_start,
                        "rivid_end": reach_end,
                        "block_reach_count": int(reach_end - reach_start),
                        "matched_supported_reaches_block": matched_reach_count,
                        "matched_supported_reaches_cumulative": cumulative_matched_reaches,
                        "rows_emitted_block": 0,
                        "rows_written_so_far": rows_written,
                        "elapsed_seconds": round(elapsed, 2),
                        "rows_per_second": round(rows_written / elapsed, 2) if elapsed > 0 else None,
                    },
                )
                continue

            block_mean = mean_values[:, selected_idx]
            block_median = median_values[:, selected_idx]
            block_p25 = p25_values[:, selected_idx]
            block_p75 = p75_values[:, selected_idx]
            block_max = max_values[:, selected_idx]
            block_high_res = None if high_res_values is None else high_res_values[:, selected_idx]
            selected_reaches = block_reach_ids[selected_idx]

            rows_before_block = rows_written
            for time_idx, forecast_time in enumerate(time_values):
                forecast_time_utc = to_utc_datetime(forecast_time).isoformat()
                for rid_pos, reach_id in enumerate(selected_reaches):
                    mean_v = _safe_float(block_mean[time_idx, rid_pos])
                    med_v = _safe_float(block_median[time_idx, rid_pos])
                    p25_v = _safe_float(block_p25[time_idx, rid_pos])
                    p75_v = _safe_float(block_p75[time_idx, rid_pos])
                    max_v = _safe_float(block_max[time_idx, rid_pos])
                    if all(v is None for v in (mean_v, med_v, p25_v, p75_v, max_v)):
                        continue

                    rows_written += 1
                    yield {
                        "provider_reach_id": str(reach_id),
                        "forecast_time_utc": forecast_time_utc,
                        "flow_mean_cms": mean_v,
                        "flow_median_cms": med_v,
                        "flow_p25_cms": p25_v,
                        "flow_p75_cms": p75_v,
                        "flow_max_cms": max_v,
                        "raw_payload_json": {
                            "source": "geoglows_public_forecast_zarr",
                            "zarr_path": source_zarr_path,
                            "forecast_variable": self.settings.geoglows_forecast_variable,
                            "forecast_dims": structure["dims"],
                            "time_dim": structure["time_dim"],
                            "reach_dim": structure["reach_dim"],
                            "ensemble_dims": ensemble_dims,
                            "ensemble_count": int(np.prod([values.shape[i] for i in range(len(ensemble_dims))])) if ensemble_dims else 1,
                            "high_res": None if block_high_res is None else _safe_float(block_high_res[time_idx, rid_pos]),
                            "block_index": block_idx,
                            "block_start": reach_start,
                            "block_end": reach_end,
                        },
                    }

            elapsed = (datetime.now(UTC) - start).total_seconds()
            rows_emitted_block = rows_written - rows_before_block
            logger.info(
                "GEOGLOWS public Zarr artifact preparation progress",
                extra={
                    "run_id": run_id,
                    "source_zarr_path": source_zarr_path,
                    "block_index": block_idx,
                    "total_blocks": len(reach_windows),
                    "rivid_start": reach_start,
                    "rivid_end": reach_end,
                    "block_reach_count": int(reach_end - reach_start),
                    "matched_supported_reaches_block": matched_reach_count,
                    "matched_supported_reaches_cumulative": cumulative_matched_reaches,
                    "rows_emitted_block": rows_emitted_block,
                    "rows_written_so_far": rows_written,
                    "elapsed_seconds": round(elapsed, 2),
                    "rows_per_second": round(rows_written / elapsed, 2) if elapsed > 0 else None,
                },
            )

        logger.info(
            "GEOGLOWS public Zarr artifact preparation completed",
            extra={
                "run_id": run_id,
                "source_zarr_path": source_zarr_path,
                "matched_supported_reaches_total": cumulative_matched_reaches,
                "rows_written": rows_written,
                "elapsed_seconds": round((datetime.now(UTC) - start).total_seconds(), 2),
            },
        )

    def _extract_high_res_candidate(self, member_slice: Any, ensemble_dims: list[str]) -> float | None:
        if not ensemble_dims:
            values = np.asarray(member_slice.values, dtype=float).reshape(-1)
            finite = values[np.isfinite(values)]
            return None if finite.size == 0 else float(finite[0])

        for dim in ensemble_dims:
            coords = member_slice.coords.get(dim)
            if coords is None:
                continue
            labels = [str(x).lower() for x in np.asarray(coords.values).reshape(-1)]
            for idx, label in enumerate(labels):
                if any(token in label for token in ("high", "determin", "control", "member_0", "0")):
                    flat = np.asarray(member_slice.values, dtype=float).reshape(-1)
                    if idx < flat.size and np.isfinite(flat[idx]):
                        return float(flat[idx])
        return None

    def _detect_high_res_member_index(self, qout_view: Any, ensemble_dims: list[str]) -> int | None:
        if not ensemble_dims:
            return None
        primary_dim = ensemble_dims[0]
        coords = qout_view.coords.get(primary_dim)
        if coords is None:
            return None
        labels = [str(x).lower() for x in np.asarray(coords.values).reshape(-1)]
        for idx, label in enumerate(labels):
            if any(token in label for token in ("high", "determin", "control", "member_0", "0")):
                return idx
        return 0 if labels else None

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
        if self.bulk_acquisition_mode() != "aws_public_zarr":
            raise ProviderOperationalError("GEOGLOWS bulk summary preparation currently supports aws_public_zarr mode only.")

        xr = self._import_xarray()
        source_zarr_path = self.build_source_zarr_path(run_id)
        ds = open_geoglows_public_forecast_run_zarr(
            xr=xr,
            run_id=run_id,
            bucket=self.settings.geoglows_forecast_bucket,
            region=self.settings.geoglows_forecast_region,
            use_anon=self.settings.geoglows_forecast_use_anon,
            run_suffix=self.settings.geoglows_forecast_run_suffix,
        )
        structure = detect_forecast_structure(ds, self.settings.geoglows_forecast_variable)
        summary = describe_forecast_dataset(ds, self.settings.geoglows_forecast_variable)

        qout = ds[self.settings.geoglows_forecast_variable]
        time_values = ds[structure["time_dim"]].values
        reach_values = ds[structure["reach_dim"]].values
        ensemble_dims = structure["ensemble_dims"]
        reach_windows = chunk_aligned_windows(len(reach_values), dataarray_chunking(qout).get(structure["reach_dim"]))
        qout_view = qout.transpose(*[*ensemble_dims, structure["time_dim"], structure["reach_dim"]])

        start = datetime.now(UTC)
        rows_written = 0
        emitted_reaches = 0
        supported_reaches = self._supported_reach_filter
        partial_reason = None
        prefetch_workers = min(4, len(reach_windows))

        logger.info(
            "GEOGLOWS public Zarr summary preparation started",
            extra={
                "provider": self.get_provider_name(),
                "run_id": run_id,
                "source_zarr_path": source_zarr_path,
                "variable": self.settings.geoglows_forecast_variable,
                "detected_dims": structure["dims"],
                "chunk_layout": summary["chunking"],
                "supported_filter_enabled": supported_reaches is not None,
                "supported_reach_count": None if supported_reaches is None else len(supported_reaches),
                "total_blocks": len(reach_windows),
                "bounded": not full_run,
                "max_reaches": max_reaches,
                "max_blocks": max_blocks,
                "max_seconds": max_seconds,
                "prefetch_workers": prefetch_workers,
            },
        )

        def _fetch_block(reach_start: int, reach_end: int) -> np.ndarray:
            block = qout_view.isel({structure["reach_dim"]: slice(reach_start, reach_end)})
            return np.asarray(block.values, dtype=np.float32)

        with ThreadPoolExecutor(max_workers=prefetch_workers) as executor:
            # Submit all block fetches upfront so S3 reads overlap
            pending_futures: list[tuple[int, int, int, Future]] = []
            for idx, (rs, re_) in enumerate(reach_windows):
                pending_futures.append((idx + 1, rs, re_, executor.submit(_fetch_block, rs, re_)))

            for block_idx, reach_start, reach_end, future in pending_futures:
                elapsed = (datetime.now(UTC) - start).total_seconds()
                if max_blocks is not None and block_idx > max_blocks:
                    partial_reason = "max_blocks"
                    break
                if max_seconds is not None and elapsed >= max_seconds:
                    partial_reason = "max_seconds"
                    break

                values = future.result()
                if ensemble_dims:
                    ensemble_axes = tuple(range(len(ensemble_dims)))
                    mean_values = np.nanmean(values, axis=ensemble_axes)
                    median_values = np.nanmedian(values, axis=ensemble_axes)
                    max_values = np.nanmax(values, axis=ensemble_axes)
                else:
                    mean_values = median_values = max_values = values

                block_reach_ids = np.asarray(reach_values[reach_start:reach_end]).astype(str)
                reach_mask = np.ones(block_reach_ids.shape[0], dtype=bool)
                if supported_reaches is not None:
                    reach_mask = np.fromiter((rid in supported_reaches for rid in block_reach_ids), dtype=bool)
                selected_idx = np.flatnonzero(reach_mask)
                if selected_idx.size == 0:
                    continue

                if max_reaches is not None:
                    remaining = max_reaches - emitted_reaches
                    if remaining <= 0:
                        partial_reason = "max_reaches"
                        break
                    if selected_idx.size > remaining:
                        selected_idx = selected_idx[:remaining]
                        partial_reason = "max_reaches"

                selected_reaches = block_reach_ids[selected_idx]
                block_mean = mean_values[:, selected_idx]
                block_median = median_values[:, selected_idx]
                block_max = max_values[:, selected_idx]

                for rid_pos, reach_id in enumerate(selected_reaches):
                    mean_series = block_mean[:, rid_pos]
                    median_series = block_median[:, rid_pos]
                    max_series = block_max[:, rid_pos]
                    has_finite_max = np.isfinite(max_series).any()
                    peak_idx = int(np.nanargmax(max_series)) if has_finite_max else None
                    rows_written += 1
                    emitted_reaches += 1
                    yield {
                        "provider_reach_id": str(reach_id),
                        "peak_time_utc": None if peak_idx is None else to_utc_datetime(time_values[peak_idx]).isoformat(),
                        "peak_mean_cms": None if not np.isfinite(mean_series).any() else _safe_float(np.nanmax(mean_series)),
                        "peak_median_cms": None if not np.isfinite(median_series).any() else _safe_float(np.nanmax(median_series)),
                        "peak_max_cms": None if not has_finite_max else _safe_float(np.nanmax(max_series)),
                        "now_mean_cms": _safe_float(mean_series[0]),
                        "now_max_cms": _safe_float(max_series[0]),
                        "raw_payload_json": {
                            "source": "geoglows_public_forecast_zarr",
                            "zarr_path": source_zarr_path,
                            "block_index": block_idx,
                            "block_start": reach_start,
                            "block_end": reach_end,
                        },
                    }

                elapsed = (datetime.now(UTC) - start).total_seconds()
                logger.info(
                    "GEOGLOWS public Zarr summary preparation progress",
                    extra={
                        "provider": self.get_provider_name(),
                        "run_id": run_id,
                        "block_index": block_idx,
                        "total_blocks": len(reach_windows),
                        "reach_start": reach_start,
                        "reach_end": reach_end,
                        "reaches_in_block": int(reach_end - reach_start),
                        "total_summary_rows_written": rows_written,
                        "matched_supported_reaches_total": emitted_reaches,
                        "elapsed_seconds": round(elapsed, 2),
                        "rows_per_second": round(rows_written / elapsed, 2) if elapsed > 0 else None,
                    },
                )

        logger.info(
            "GEOGLOWS public Zarr summary preparation completed",
            extra={
                "provider": self.get_provider_name(),
                "run_id": run_id,
                "source_zarr_path": source_zarr_path,
                "total_rows_written": rows_written,
                "total_duration_seconds": round((datetime.now(UTC) - start).total_seconds(), 2),
                "bounded": not full_run,
                "partial_reason": partial_reason,
            },
        )

    def normalize_bulk_summary_record(self, run_id: str, record: dict) -> BulkForecastSummaryArtifactRowSchema | None:
        reach_id = str(record.get("provider_reach_id", record.get("river_id", ""))).strip()
        if not reach_id:
            return None

        peak_time = record.get("peak_time_utc")
        peak_time_utc = None
        if peak_time:
            peak_time_utc = peak_time if isinstance(peak_time, datetime) else datetime.fromisoformat(str(peak_time)).replace(tzinfo=UTC)

        payload = record.get("raw_payload_json") if isinstance(record.get("raw_payload_json"), dict) else {"source": "geoglows_public_forecast_zarr"}
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

    def fetch_reach_detail_from_public_zarr(
        self, run_id: str, provider_reach_id: str, timeseries_limit: int | None = None
    ) -> list[TimeseriesPointSchema]:
        xr = self._import_xarray()
        ds = open_geoglows_public_forecast_run_zarr(
            xr=xr,
            run_id=run_id,
            bucket=self.settings.geoglows_forecast_bucket,
            region=self.settings.geoglows_forecast_region,
            use_anon=self.settings.geoglows_forecast_use_anon,
            run_suffix=self.settings.geoglows_forecast_run_suffix,
        )
        structure = detect_forecast_structure(ds, self.settings.geoglows_forecast_variable)
        qout = ds[self.settings.geoglows_forecast_variable]
        ordered_dims = [*structure["ensemble_dims"], structure["time_dim"], structure["reach_dim"]]
        qout_view = qout.transpose(*ordered_dims)

        reach_values = np.asarray(ds[structure["reach_dim"]].values).astype(str)
        matches = np.where(reach_values == str(provider_reach_id))[0]
        if matches.size == 0:
            raise ProviderOperationalError(f"GEOGLOWS reach_id {provider_reach_id} not found in run {run_id}")
        reach_index = int(matches[0])

        series = np.asarray(qout_view.isel({structure["reach_dim"]: reach_index}).values, dtype=np.float32)
        ensemble_dims = structure["ensemble_dims"]
        if ensemble_dims:
            ensemble_axes = tuple(range(len(ensemble_dims)))
            mean_values = np.nanmean(series, axis=ensemble_axes)
            median_values = np.nanmedian(series, axis=ensemble_axes)
            p25_values = np.nanpercentile(series, 25, axis=ensemble_axes)
            p75_values = np.nanpercentile(series, 75, axis=ensemble_axes)
            max_values = np.nanmax(series, axis=ensemble_axes)
            high_res_index = self._detect_high_res_member_index(qout_view=qout_view, ensemble_dims=ensemble_dims)
            high_res_values = None if high_res_index is None else np.asarray(np.take(series, indices=high_res_index, axis=0), dtype=np.float32)
        else:
            mean_values = median_values = p25_values = p75_values = max_values = series
            high_res_values = series

        time_values = ds[structure["time_dim"]].values
        rows: list[TimeseriesPointSchema] = []
        for time_idx, forecast_time in enumerate(time_values):
            rows.append(
                TimeseriesPointSchema(
                    provider=self.get_provider_name(),
                    run_id=run_id,
                    provider_reach_id=str(provider_reach_id),
                    forecast_time_utc=to_utc_datetime(forecast_time),
                    flow_mean_cms=_safe_float(mean_values[time_idx]),
                    flow_median_cms=_safe_float(median_values[time_idx]),
                    flow_p25_cms=_safe_float(p25_values[time_idx]),
                    flow_p75_cms=_safe_float(p75_values[time_idx]),
                    flow_max_cms=_safe_float(max_values[time_idx]),
                    raw_payload_json={
                        "source": "geoglows_public_forecast_zarr",
                        "reach_index": reach_index,
                        "high_res": None if high_res_values is None else _safe_float(high_res_values[time_idx]),
                    },
                )
            )
        if timeseries_limit is not None:
            rows = rows[:timeseries_limit]
        return rows

    def normalize_bulk_record(self, run_id: str, record: dict) -> BulkForecastArtifactRowSchema | None:
        reach_id = str(record.get("provider_reach_id", record.get("river_id", ""))).strip()
        if not reach_id:
            return None

        dt = record.get("forecast_time_utc") or record.get("time")
        if dt is None:
            return None
        forecast_time = dt if isinstance(dt, datetime) else datetime.fromisoformat(str(dt)).replace(tzinfo=UTC)

        if isinstance(record.get("raw_payload_json"), dict):
            payload = dict(record["raw_payload_json"])
        else:
            payload = {
                "source": "geoglows_raw_bulk",
                "line_number": record.get("_line_number"),
                "raw_record": record,
            }

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
            raw_payload_json=payload,
        )

    def cleanup_old_raw_staging(self) -> int:
        if self.bulk_acquisition_mode() == "aws_public_zarr":
            return 0
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

    def _import_xarray(self):
        try:
            import xarray as xr
        except ModuleNotFoundError as exc:
            raise ProviderOperationalError("xarray is required for GEOGLOWS forecast Zarr ingestion.") from exc
        return xr

    def _import_s3fs(self):
        try:
            import s3fs
        except ModuleNotFoundError as exc:
            raise ProviderOperationalError("s3fs is required for GEOGLOWS public S3 run discovery.") from exc
        return s3fs

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
