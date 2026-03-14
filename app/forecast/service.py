import logging
from datetime import UTC, datetime
from time import perf_counter
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db import models
from app.db.repositories import ForecastRepository
from app.forecast.artifacts import ForecastArtifactStore
from app.forecast.base import ForecastProviderAdapter
from app.forecast.providers.geoglows_return_periods import (
    iter_geoglows_return_periods_from_zarr,
    load_geoglows_return_periods_from_path,
)
from app.forecast.schemas import (
    BulkForecastArtifactRowSchema,
    ForecastMapFilters,
    ForecastMapMeta,
    ForecastMapReachesResponse,
    ForecastRunSchema,
    MapReachSummarySchema,
    ProviderHealthResponse,
    RawAcquisitionStatus,
    ArtifactStatus,
    IngestStatus,
    SummarizeStatus,
    RunReadinessStatusResponse,
    ReachDetailResponse,
    ReachSummarySchema,
    ReturnPeriodSchema,
    TimeseriesPointSchema,
)


logger = logging.getLogger(__name__)


class ForecastService:
    def __init__(self, db: Session, settings: Settings, providers: dict[str, ForecastProviderAdapter]) -> None:
        self.db = db
        self.settings = settings
        self.repo = ForecastRepository(db)
        self.providers = providers
        self.artifacts = ForecastArtifactStore(settings.forecast_bulk_artifact_dir)

    def list_providers(self) -> list[str]:
        return sorted(self.providers.keys())

    STAGE_DISCOVERED = "discovered"
    STAGE_RAW_ACQUIRED = "raw_acquired"
    STAGE_ARTIFACT_PREPARED = "artifact_prepared"
    STAGE_INGESTED = "ingested"
    STAGE_SUMMARIZED = "summarized"
    STAGE_MAP_READY = "map_ready"
    STAGE_ORDER = [
        STAGE_DISCOVERED,
        STAGE_RAW_ACQUIRED,
        STAGE_ARTIFACT_PREPARED,
        STAGE_INGESTED,
        STAGE_SUMMARIZED,
        STAGE_MAP_READY,
    ]
    MAP_READY_DEFINITION = (
        "run exists; artifact prepared (or equivalent ingest source completed); timeseries rows exist; "
        "summary rows exist; map rows exist via /forecast/map/reaches"
    )

    def _get_provider(self, provider: str) -> ForecastProviderAdapter:
        if provider not in self.providers:
            raise ValueError(f"Provider '{provider}' is not enabled")
        return self.providers[provider]

    def _latest_upstream_run_id(self, adapter: ForecastProviderAdapter) -> str | None:
        fn = getattr(adapter, "get_latest_upstream_run_id", None)
        if callable(fn):
            try:
                return fn()
            except Exception:
                return None
        return None

    def _upstream_run_exists(self, adapter: ForecastProviderAdapter, run_id: str) -> bool | None:
        fn = getattr(adapter, "upstream_run_exists", None)
        if callable(fn):
            try:
                return fn(run_id)
            except Exception:
                return None
        return None

    def _source_zarr_path(self, adapter: ForecastProviderAdapter, run_id: str) -> str | None:
        fn = getattr(adapter, "build_source_zarr_path", None)
        if callable(fn):
            try:
                return fn(run_id)
            except Exception:
                return None
        return None

    def _run_ops_metadata(self, run_row: models.ForecastRun | None) -> dict[str, Any]:
        if run_row is None or not run_row.metadata_json:
            return {}
        metadata = run_row.metadata_json
        if not isinstance(metadata, dict):
            return {}
        return dict(metadata.get("ops", {})) if isinstance(metadata.get("ops", {}), dict) else {}

    def _set_run_ops_metadata(self, run_row: models.ForecastRun, ops: dict[str, Any]) -> None:
        base = run_row.metadata_json if isinstance(run_row.metadata_json, dict) else {}
        merged = dict(base)
        merged["ops"] = ops
        run_row.metadata_json = merged

    def _touch_ops(self, ops: dict[str, Any]) -> dict[str, Any]:
        ops["last_updated_utc"] = datetime.now(UTC).isoformat()
        return ops

    def _record_run_failure(self, provider: str, run_id: str, stage: str, message: str) -> None:
        run_row = self.repo.get_run(provider, run_id)
        if run_row is None:
            return
        ops = self._run_ops_metadata(run_row)
        ops["failure_stage"] = stage
        ops["failure_message"] = str(message)
        self._touch_ops(ops)
        self._set_run_ops_metadata(run_row, ops)
        self.db.commit()

    def _mark_stage_complete(self, provider: str, run_id: str, stage: str) -> None:
        run_row = self.repo.get_run(provider, run_id)
        if run_row is None:
            return
        ops = self._run_ops_metadata(run_row)
        completed = set(ops.get("completed_stages", []))
        completed.add(stage)
        ops["completed_stages"] = [item for item in self.STAGE_ORDER if item in completed]
        if stage == self.STAGE_MAP_READY:
            ops["map_ready"] = True
        if ops.get("failure_stage") == stage:
            ops["failure_stage"] = None
            ops["failure_message"] = None
        self._touch_ops(ops)
        self._set_run_ops_metadata(run_row, ops)
        self.db.commit()

    def discover_latest_run(self, provider: str) -> ForecastRunSchema:
        adapter = self._get_provider(provider)
        run = adapter.discover_latest_run()
        existing = self.repo.get_run(provider, run.run_id)
        previous_ops = self._run_ops_metadata(existing)
        previous_ingest_status = None if existing is None else existing.ingest_status
        run_row = self.repo.upsert_run(run)
        if previous_ingest_status and previous_ingest_status != "pending":
            run_row.ingest_status = previous_ingest_status

        ops = dict(previous_ops)
        ops.setdefault("raw_acquisition", {})
        ops.setdefault("artifact", {})
        ops.setdefault("ingest", {})
        ops.setdefault("summarize", {})
        ops.setdefault("map", {})
        if not previous_ops:
            ops["completed_stages"] = [self.STAGE_DISCOVERED]
            ops["current_status"] = self.STAGE_DISCOVERED
            ops["map_ready"] = False
        ops["raw_acquisition"]["mode"] = adapter.bulk_acquisition_mode()
        self._touch_ops(ops)
        self._set_run_ops_metadata(run_row, ops)
        self.db.commit()
        return run

    def ingest_return_periods(self, provider: str, reach_ids: list[str]) -> int:
        self._get_provider(provider)
        rows = self.providers[provider].fetch_return_periods(reach_ids)
        count = self.repo.upsert_return_periods(rows)
        self.db.commit()
        logger.info("upserted return periods", extra={"provider": provider, "count": count})
        return count

    def import_geoglows_return_periods(self, dataset_path: str) -> int:
        rows = load_geoglows_return_periods_from_path(dataset_path)
        count = self.repo.upsert_return_periods(rows)
        self.db.commit()
        logger.info("imported local GEOGLOWS return periods", extra={"count": count, "path": dataset_path})
        return count

    def import_geoglows_return_periods_zarr(
        self,
        zarr_path: str | None = None,
        method: str | None = None,
        batch_size: int | None = None,
    ) -> int:
        selected_path = zarr_path or self.settings.geoglows_return_period_zarr_path
        selected_method = method or self.settings.geoglows_return_period_method
        selected_batch_size = batch_size or self.settings.geoglows_return_period_import_batch_size

        logger.info(
            "starting GEOGLOWS Zarr return-period import",
            extra={
                "zarr_path": selected_path,
                "method": selected_method,
                "batch_size": selected_batch_size,
            },
        )

        total_upserted = 0
        total_processed = 0
        for rows in iter_geoglows_return_periods_from_zarr(
            zarr_path=selected_path,
            method=selected_method,
            batch_size=selected_batch_size,
        ):
            total_processed += len(rows)
            upserted = self.repo.upsert_return_periods(rows)
            total_upserted += upserted
            self.db.commit()
            logger.info(
                "upserted GEOGLOWS return-period batch",
                extra={
                    "method": selected_method,
                    "batch_size": selected_batch_size,
                    "batch_rows": len(rows),
                    "batch_upserted": upserted,
                    "total_processed": total_processed,
                    "total_upserted": total_upserted,
                },
            )

        logger.info(
            "completed GEOGLOWS Zarr return-period import",
            extra={
                "method": selected_method,
                "batch_size": selected_batch_size,
                "total_reaches_processed": total_processed,
                "total_upserted": total_upserted,
                "classification_available": self.repo.has_return_periods("geoglows"),
            },
        )
        return total_upserted

    def prepare_bulk_artifact(
        self,
        provider: str,
        run_id: str,
        filter_to_supported_reaches: bool = True,
        if_present: str = "skip",
        overwrite_raw: bool = False,
    ) -> tuple[str, int]:
        adapter = self._get_provider(provider)
        resolved_run = self.resolve_requested_run_id(provider, run_id)

        if not adapter.supports_bulk_acquisition():
            raise ValueError(
                "Bulk artifact preparation requires provider bulk acquisition configuration. "
                "Configure the provider bulk source before running prepare-bulk-artifact."
            )

        if if_present not in {"skip", "overwrite", "error"}:
            raise ValueError("if_present must be one of: skip, overwrite, error")

        run_row = self.repo.get_run(provider, resolved_run.run_id)
        if run_row is None:
            raise ValueError(f"Run '{resolved_run.run_id}' not found for provider '{provider}'")

        ops = self._run_ops_metadata(run_row)
        raw = dict(ops.get("raw_acquisition", {}))
        artifact = dict(ops.get("artifact", {}))
        raw["attempted"] = True
        raw["succeeded"] = False
        raw["mode"] = adapter.bulk_acquisition_mode()
        ops["raw_acquisition"] = raw
        ops["artifact"] = artifact
        ops["current_status"] = self.STAGE_DISCOVERED
        self._touch_ops(ops)
        self._set_run_ops_metadata(run_row, ops)
        self.db.commit()

        supported_reaches: set[str] | None = None
        if filter_to_supported_reaches:
            supported_reaches = set(self.repo.iter_supported_reach_ids(provider, as_chunks=False))
            if not supported_reaches:
                message = (
                    f"No supported reaches found for provider '{provider}'. "
                    "Import return periods first to establish supported map reaches."
                )
                self._record_run_failure(provider, resolved_run.run_id, self.STAGE_ARTIFACT_PREPARED, message)
                raise ValueError(message)

        artifact_path = self.artifacts.artifact_path(provider, resolved_run.run_id)
        if artifact_path.exists():
            existing_count = self.artifacts.count_rows(provider, resolved_run.run_id)
            if if_present == "skip":
                if (
                    adapter.bulk_acquisition_mode() == "aws_public_zarr"
                    and supported_reaches is not None
                    and existing_count < len(supported_reaches)
                ):
                    logger.warning(
                        "existing artifact appears incomplete for supported network; rebuilding despite if_present=skip",
                        extra={
                            "provider": provider,
                            "run_id": resolved_run.run_id,
                            "artifact_path": str(artifact_path),
                            "existing_row_count": existing_count,
                            "supported_reach_count": len(supported_reaches),
                        },
                    )
                else:
                    raw["succeeded"] = True
                    raw["staged_raw_path"] = raw.get("staged_raw_path")
                    artifact.update({"exists": True, "path": str(artifact_path), "row_count": existing_count})
                    ops["raw_acquisition"] = raw
                    ops["artifact"] = artifact
                    ops["current_status"] = self.STAGE_ARTIFACT_PREPARED
                    self._touch_ops(ops)
                    self._set_run_ops_metadata(run_row, ops)
                    self.db.commit()
                    self._mark_stage_complete(provider, resolved_run.run_id, self.STAGE_RAW_ACQUIRED)
                    self._mark_stage_complete(provider, resolved_run.run_id, self.STAGE_ARTIFACT_PREPARED)
                    logger.info(
                        "skipping bulk artifact preparation because artifact exists",
                        extra={
                            "provider": provider,
                            "run_id": resolved_run.run_id,
                            "artifact_path": str(artifact_path),
                            "if_present": if_present,
                        },
                    )
                    return str(artifact_path), 0
            if if_present == "error":
                message = (
                    f"Bulk artifact already exists for provider={provider}, run_id={resolved_run.run_id}: {artifact_path}"
                )
                self._record_run_failure(provider, resolved_run.run_id, self.STAGE_ARTIFACT_PREPARED, message)
                raise ValueError(message)

        try:
            started_at = perf_counter()
            if hasattr(adapter, "set_supported_reach_filter"):
                adapter.set_supported_reach_filter(supported_reaches)
            staged_raw_path = adapter.acquire_bulk_raw_source(resolved_run.run_id, overwrite=overwrite_raw)
            raw["source_uri"] = staged_raw_path
            raw["staged_raw_path"] = staged_raw_path
            raw["succeeded"] = True
            ops["raw_acquisition"] = raw
            ops["current_status"] = self.STAGE_RAW_ACQUIRED
            self._touch_ops(ops)
            self._set_run_ops_metadata(run_row, ops)
            self.db.commit()
            self._mark_stage_complete(provider, resolved_run.run_id, self.STAGE_RAW_ACQUIRED)

            logger.info(
                "starting bulk artifact preparation",
                extra={
                    "provider": provider,
                    "run_id": resolved_run.run_id,
                    "acquisition_mode": adapter.bulk_acquisition_mode(),
                    "raw_source_location": staged_raw_path,
                    "filter_to_supported_reaches": filter_to_supported_reaches,
                    "supported_reach_count": 0 if supported_reaches is None else len(supported_reaches),
                    "write_batch_size": self.settings.forecast_bulk_artifact_write_batch_size,
                    "if_present": if_present,
                },
            )

            stats = {
                "raw_records_seen": 0,
                "normalized_rows_written": 0,
                "rows_dropped_filtered": 0,
                "rows_dropped_invalid": 0,
            }

            def _normalized_rows():
                for record in adapter.iter_raw_bulk_records(resolved_run.run_id, staged_raw_path):
                    stats["raw_records_seen"] += 1
                    try:
                        row = adapter.normalize_bulk_record(resolved_run.run_id, record)
                    except Exception:
                        stats["rows_dropped_invalid"] += 1
                        continue
                    if row is None:
                        stats["rows_dropped_invalid"] += 1
                        continue
                    if supported_reaches is not None and row.provider_reach_id not in supported_reaches:
                        stats["rows_dropped_filtered"] += 1
                        continue
                    stats["normalized_rows_written"] += 1
                    yield row

            artifact_path, _ = self.artifacts.write_rows(provider, resolved_run.run_id, _normalized_rows())
            removed_artifacts = self.artifacts.cleanup_old_runs(
                provider, keep_latest=self.settings.forecast_bulk_artifact_retention_runs
            )
            removed_raw = adapter.cleanup_old_raw_staging()

            artifact.update(
                {
                    "exists": True,
                    "path": str(artifact_path),
                    "row_count": stats["normalized_rows_written"],
                }
            )
            ops["artifact"] = artifact
            ops["current_status"] = self.STAGE_ARTIFACT_PREPARED
            self._touch_ops(ops)
            self._set_run_ops_metadata(run_row, ops)
            self.db.commit()
            self._mark_stage_complete(provider, resolved_run.run_id, self.STAGE_ARTIFACT_PREPARED)

            logger.info(
                "completed bulk artifact preparation",
                extra={
                    "provider": provider,
                    "run_id": resolved_run.run_id,
                    "acquisition_mode": adapter.bulk_acquisition_mode(),
                    "raw_source_location": staged_raw_path,
                    "artifact_path": str(artifact_path),
                    "raw_records_seen": stats["raw_records_seen"],
                    "normalized_rows_written": stats["normalized_rows_written"],
                    "rows_dropped_filtered": stats["rows_dropped_filtered"],
                    "rows_dropped_invalid": stats["rows_dropped_invalid"],
                    "removed_old_artifacts": removed_artifacts,
                    "removed_old_raw_files": removed_raw,
                    "elapsed_seconds": round(perf_counter() - started_at, 3),
                },
            )
            return str(artifact_path), stats["normalized_rows_written"]
        except Exception as exc:
            self._record_run_failure(provider, resolved_run.run_id, self.STAGE_ARTIFACT_PREPARED, str(exc))
            raise
        finally:
            if hasattr(adapter, "set_supported_reach_filter"):
                adapter.set_supported_reach_filter(None)

    def ingest_forecast_run(
        self,
        provider: str,
        run_id: str,
        reach_ids: list[str] | None = None,
        ingest_mode: Literal["rest_single", "bulk"] | None = None,
    ) -> int:
        adapter = self._get_provider(provider)
        resolved_run = self.resolve_requested_run_id(provider, run_id)
        selected_mode = ingest_mode or ("rest_single" if reach_ids else "bulk")

        if selected_mode == "rest_single":
            if not reach_ids:
                raise ValueError("ingest_mode=rest_single requires at least one --reach-id")
            return self._ingest_via_rest(adapter, provider, resolved_run.run_id, list(reach_ids))

        if selected_mode != "bulk":
            raise ValueError(f"Unsupported ingest mode '{selected_mode}'")
        if reach_ids:
            raise ValueError("ingest_mode=bulk does not accept explicit reach_ids; use rest_single for targeted ingest")
        return self._ingest_via_bulk(adapter, provider, resolved_run.run_id)

    def _ingest_via_rest(
        self,
        adapter: ForecastProviderAdapter,
        provider: str,
        run_id: str,
        reach_ids: list[str],
    ) -> int:
        logger.info(
            "starting forecast ingest",
            extra={
                "provider": provider,
                "run_id": run_id,
                "ingest_mode": "rest_single",
                "source": "forecast_stats_rest",
                "total_reach_count": len(reach_ids),
            },
        )
        try:
            started_at = perf_counter()
            rows = adapter.fetch_forecast_timeseries(run_id, reach_ids)
            rows_written = self.repo.bulk_upsert_timeseries(rows)
            run_row = self.repo.get_run(provider, run_id)
            if run_row:
                run_row.ingest_status = "partial" if rows_written == 0 else "complete"
                ops = self._run_ops_metadata(run_row)
                ingest = dict(ops.get("ingest", {}))
                ingest["completed"] = rows_written > 0
                ingest["timeseries_row_count"] = self.repo.count_timeseries_rows_for_run(provider, run_id)
                ingest["mode"] = "rest_single"
                ops["ingest"] = ingest
                ops["current_status"] = self.STAGE_INGESTED if ingest["completed"] else self.STAGE_DISCOVERED
                self._touch_ops(ops)
                self._set_run_ops_metadata(run_row, ops)
            self.db.commit()
            if rows_written > 0:
                self._mark_stage_complete(provider, run_id, self.STAGE_INGESTED)
            logger.info(
                "completed forecast ingest",
                extra={
                    "provider": provider,
                    "run_id": run_id,
                    "ingest_mode": "rest_single",
                    "source": "forecast_stats_rest",
                    "rows_written": rows_written,
                    "elapsed_seconds": round(perf_counter() - started_at, 3),
                },
            )
            return rows_written
        except Exception as exc:
            self._record_run_failure(provider, run_id, self.STAGE_INGESTED, str(exc))
            raise

    def _ingest_via_bulk(self, adapter: ForecastProviderAdapter, provider: str, run_id: str) -> int:
        _ = adapter
        supported_reach_count = self.repo.count_supported_reaches(provider)
        if supported_reach_count == 0:
            message = (
                f"No supported reaches found for provider '{provider}'. "
                "Import return periods first to establish supported map reaches."
            )
            self._record_run_failure(provider, run_id, self.STAGE_INGESTED, message)
            raise ValueError(message)

        artifact_exists = self.artifacts.exists(provider, run_id)
        if not artifact_exists:
            message = (
                "Bulk ingest was requested, but no normalized bulk artifact exists for this run. "
                "Run prepare-bulk-artifact first."
            )
            self._record_run_failure(provider, run_id, self.STAGE_INGESTED, message)
            raise ValueError(message)

        logger.info(
            "starting forecast ingest",
            extra={
                "provider": provider,
                "run_id": run_id,
                "ingest_mode": "bulk",
                "source": "normalized_artifact",
                "supported_reach_count": supported_reach_count,
                "batch_size": self.settings.forecast_bulk_ingest_batch_size,
            },
        )

        try:
            started_at = perf_counter()
            total_rows = 0
            batch: list[BulkForecastArtifactRowSchema] = []
            chunk_index = 0
            for row in self.artifacts.iter_rows(provider, run_id):
                batch.append(row)
                if len(batch) < self.settings.forecast_bulk_ingest_batch_size:
                    continue
                chunk_index += 1
                chunk_rows = self._upsert_artifact_batch(batch)
                total_rows += chunk_rows
                batch = []
                logger.info(
                    "ingest chunk complete",
                    extra={
                        "provider": provider,
                        "run_id": run_id,
                        "ingest_mode": "bulk",
                        "source": "normalized_artifact",
                        "chunk_number": chunk_index,
                        "chunk_rows_written": chunk_rows,
                        "total_rows_written": total_rows,
                    },
                )

            if batch:
                chunk_index += 1
                chunk_rows = self._upsert_artifact_batch(batch)
                total_rows += chunk_rows
                logger.info(
                    "ingest chunk complete",
                    extra={
                        "provider": provider,
                        "run_id": run_id,
                        "ingest_mode": "bulk",
                        "source": "normalized_artifact",
                        "chunk_number": chunk_index,
                        "chunk_rows_written": chunk_rows,
                        "total_rows_written": total_rows,
                    },
                )

            run_row = self.repo.get_run(provider, run_id)
            if run_row:
                run_row.ingest_status = "partial" if total_rows == 0 else "complete"
                ops = self._run_ops_metadata(run_row)
                ingest = dict(ops.get("ingest", {}))
                ingest["completed"] = total_rows > 0
                ingest["timeseries_row_count"] = self.repo.count_timeseries_rows_for_run(provider, run_id)
                ingest["mode"] = "bulk"
                ops["ingest"] = ingest
                ops["current_status"] = self.STAGE_INGESTED if ingest["completed"] else self.STAGE_DISCOVERED
                self._touch_ops(ops)
                self._set_run_ops_metadata(run_row, ops)
            self.db.commit()
            if total_rows > 0:
                self._mark_stage_complete(provider, run_id, self.STAGE_INGESTED)

            logger.info(
                "completed forecast ingest",
                extra={
                    "provider": provider,
                    "run_id": run_id,
                    "ingest_mode": "bulk",
                    "source": "normalized_artifact",
                    "supported_reach_count": supported_reach_count,
                    "artifact_rows_read": self.artifacts.count_rows(provider, run_id),
                    "rows_written": total_rows,
                    "distinct_reaches_ingested": self.repo.count_timeseries_reaches_for_run(provider, run_id),
                    "elapsed_seconds": round(perf_counter() - started_at, 3),
                },
            )
            return total_rows
        except Exception as exc:
            self._record_run_failure(provider, run_id, self.STAGE_INGESTED, str(exc))
            raise

    def _upsert_artifact_batch(self, batch: list[BulkForecastArtifactRowSchema]) -> int:
        rows = [
            TimeseriesPointSchema(
                provider=item.provider,
                run_id=item.run_id,
                provider_reach_id=item.provider_reach_id,
                forecast_time_utc=item.forecast_time_utc,
                flow_mean_cms=item.flow_mean_cms,
                flow_median_cms=item.flow_median_cms,
                flow_p25_cms=item.flow_p25_cms,
                flow_p75_cms=item.flow_p75_cms,
                flow_max_cms=item.flow_max_cms,
                raw_payload_json=item.raw_payload_json,
            )
            for item in batch
        ]
        count = self.repo.bulk_upsert_timeseries(rows)
        self.db.commit()
        return count

    def summarize_run(self, provider: str, run_id: str, reach_ids: list[str] | None = None) -> int:
        adapter = self._get_provider(provider)
        resolved_run = self.resolve_requested_run_id(provider, run_id)

        try:
            if reach_ids is None:
                reach_ids = list(
                    self.db.execute(
                        select(models.ForecastProviderReachTimeseries.provider_reach_id)
                        .where(
                            models.ForecastProviderReachTimeseries.provider == provider,
                            models.ForecastProviderReachTimeseries.run_id == resolved_run.run_id,
                        )
                        .distinct()
                    ).scalars()
                )

            summaries: list[ReachSummarySchema] = []
            for reach_id in reach_ids:
                ts_rows = [to_timeseries_schema(x) for x in self.repo.get_timeseries(provider, resolved_run.run_id, reach_id)]
                rp_model = self.repo.get_return_period(provider, reach_id)
                rp_schema = None if not rp_model else to_return_period_schema(rp_model)
                if rp_schema is None:
                    logger.info("generating summary without return periods", extra={"provider": provider, "run_id": resolved_run.run_id, "reach_id": reach_id})
                summary = adapter.summarize_reach(resolved_run.run_id, reach_id, ts_rows, rp_schema)
                logger.info("summary peak values", extra={"provider": provider, "run_id": resolved_run.run_id, "reach_id": reach_id, "peak_mean_cms": summary.peak_mean_cms, "peak_median_cms": summary.peak_median_cms, "peak_max_cms": summary.peak_max_cms})
                summaries.append(summary)

            count = self.repo.upsert_summaries(summaries)
            run_row = self.repo.get_run(provider, resolved_run.run_id)
            if run_row:
                ops = self._run_ops_metadata(run_row)
                summarize = dict(ops.get("summarize", {}))
                summary_count = self.repo.count_summaries_for_run(provider, resolved_run.run_id)
                summarize["completed"] = summary_count > 0
                summarize["summary_row_count"] = summary_count
                ops["summarize"] = summarize
                ops["map"] = {"map_row_count": summary_count}
                ops["current_status"] = self.STAGE_SUMMARIZED if summary_count > 0 else ops.get("current_status", self.STAGE_DISCOVERED)
                self._touch_ops(ops)
                self._set_run_ops_metadata(run_row, ops)
            self.db.commit()
            if count > 0:
                self._mark_stage_complete(provider, resolved_run.run_id, self.STAGE_SUMMARIZED)
            status = self.get_run_status(provider, resolved_run.run_id)
            if status.map_ready:
                self._mark_stage_complete(provider, resolved_run.run_id, self.STAGE_MAP_READY)
            logger.info(
                "completed summarize run",
                extra={
                    "provider": provider,
                    "run_id": resolved_run.run_id,
                    "summary_rows_upserted": count,
                    "map_rows_available": self.repo.count_summaries_for_run(provider, resolved_run.run_id),
                },
            )
            return count
        except Exception as exc:
            self._record_run_failure(provider, resolved_run.run_id, self.STAGE_SUMMARIZED, str(exc))
            raise

    def get_latest_run(self, provider: str) -> ForecastRunSchema | None:
        return self.resolve_requested_run_id(provider, "latest", require_existing=False)

    def get_reach_detail(
        self, provider: str, provider_reach_id: str, run_id: str | None = None, timeseries_limit: int | None = None
    ) -> ReachDetailResponse:
        self._get_provider(provider)
        run = self.resolve_requested_run_id(provider, run_id or "latest")
        rp_row = self.repo.get_return_period(provider, provider_reach_id)
        ts_rows = self.repo.get_timeseries(provider, run.run_id, provider_reach_id, limit=timeseries_limit)
        summary = self.repo.get_summary(provider, run.run_id, provider_reach_id)
        return ReachDetailResponse(
            provider=provider,
            run=run,
            return_periods=None if rp_row is None else to_return_period_schema(rp_row),
            timeseries=[to_timeseries_schema(x) for x in ts_rows],
            summary=None if summary is None else to_summary_schema(summary),
        )

    def get_reach_summaries(
        self, provider: str, run_id: str | None = None, severity_min: int | None = None, limit: int | None = None
    ) -> list[ReachSummarySchema]:
        self._get_provider(provider)
        run = self.resolve_requested_run_id(provider, run_id or "latest", require_existing=False)
        if not run:
            return []
        rows = self.repo.get_summaries(
            provider,
            run.run_id,
            severity_min=severity_min,
            limit=limit or self.settings.forecast_summary_default_limit,
        )
        return [to_summary_schema(x) for x in rows]


    def list_forecast_map_reaches(
        self,
        provider: str,
        run_id: str | None = None,
        bbox: str | None = None,
        limit: int | None = None,
        flagged_only: bool = False,
        min_severity_score: float | None = None,
    ) -> ForecastMapReachesResponse:
        """
        Map endpoint contract: serve lightweight forecast attributes only.

        River geometry is intentionally excluded from this backend because geometry is
        provided by frontend tiles (PMTiles/vector tiles). The detailed reach endpoint
        remains the heavy view for timeseries and return-period payloads.

        NOTE: bbox is accepted for forward compatibility; this repository currently has
        no reach geometry/bounds table, so bbox filtering is not applied yet.
        """
        self._get_provider(provider)
        run = self.resolve_requested_run_id(provider, run_id or "latest", require_existing=False)
        if not run:
            return ForecastMapReachesResponse(
                data=[],
                meta=ForecastMapMeta(
                    provider=provider,
                    run_id="",
                    count=0,
                    filters=ForecastMapFilters(
                        bbox=bbox,
                        flagged_only=flagged_only,
                        min_severity_score=min_severity_score,
                    ),
                ),
            )

        rows = self.repo.get_map_summaries(
            provider=provider,
            run_id=run.run_id,
            flagged_only=flagged_only,
            min_severity_score=min_severity_score,
            limit=limit or self.settings.forecast_summary_default_limit,
        )
        data = [to_map_summary_schema(x) for x in rows]
        return ForecastMapReachesResponse(
            data=data,
            meta=ForecastMapMeta(
                provider=provider,
                run_id=run.run_id,
                count=len(data),
                filters=ForecastMapFilters(
                    bbox=bbox,
                    flagged_only=flagged_only,
                    min_severity_score=min_severity_score,
                ),
            ),
        )

    def get_provider_health(self, provider: str) -> ProviderHealthResponse:
        adapter = self._get_provider(provider)
        latest = self.get_latest_run(provider)
        summary_count = 0
        status = None
        latest_run_timeseries_row_count = 0
        latest_run_reach_count = 0
        latest_status: RunReadinessStatusResponse | None = None
        if latest:
            status = latest.ingest_status
            summary_count = self.repo.count_summaries_for_run(provider, latest.run_id)
            latest_run_timeseries_row_count = self.repo.count_timeseries_rows_for_run(provider, latest.run_id)
            latest_run_reach_count = self.repo.count_timeseries_reaches_for_run(provider, latest.run_id)
            latest_status = self.get_run_status(provider, latest.run_id)

        capabilities = getattr(adapter, "capabilities", None)
        supports_forecast_stats_rest = bool(
            getattr(capabilities, "supports_forecast_stats_rest", False)
        )
        supports_bulk_forecast_ingest = bool(adapter.supports_bulk_acquisition())
        bulk_acquisition_mode = adapter.bulk_acquisition_mode()
        bulk_raw_source_reachable = adapter.is_bulk_source_reachable()
        source = self.settings.geoglows_data_source.lower() if provider == "geoglows" else "unknown"
        supports_return_periods_current_backend = bool(
            getattr(capabilities, f"supports_return_periods_{source}", False)
        )
        local_return_periods_available = self.repo.has_return_periods(provider)
        latest_run_artifact_exists = bool(latest_status and latest_status.artifact.exists)
        latest_run_map_ready = bool(latest_status and latest_status.map_ready)
        upstream_latest_run_id = self._latest_upstream_run_id(adapter)
        latest_upstream_run_exists = self._upstream_run_exists(adapter, upstream_latest_run_id) if upstream_latest_run_id else None
        latest_source_zarr_path = self._source_zarr_path(adapter, latest.run_id) if latest else None

        return ProviderHealthResponse(
            provider=provider,
            enabled=provider in self.providers,
            latest_run=latest,
            ingest_status=status,
            summary_count=summary_count,
            supports_forecast_stats_rest=supports_forecast_stats_rest,
            supports_return_periods_current_backend=supports_return_periods_current_backend,
            supports_bulk_forecast_ingest=supports_bulk_forecast_ingest,
            bulk_acquisition_configured=supports_bulk_forecast_ingest,
            bulk_acquisition_mode=bulk_acquisition_mode,
            bulk_raw_source_reachable=bulk_raw_source_reachable,
            local_return_periods_available=local_return_periods_available,
            latest_run_has_timeseries=latest_run_timeseries_row_count > 0,
            latest_run_timeseries_row_count=latest_run_timeseries_row_count,
            latest_run_reach_count=latest_run_reach_count,
            latest_run_has_summaries=summary_count > 0,
            latest_run_artifact_exists=latest_run_artifact_exists,
            latest_run_artifact_row_count=0 if latest_status is None else latest_status.artifact.row_count,
            latest_run_summary_count=0 if latest_status is None else latest_status.summarize.summary_row_count,
            latest_run_map_count=0 if latest_status is None else latest_status.map_row_count,
            latest_run_status=None if latest_status is None else latest_status.current_status,
            latest_run_missing_stages=[] if latest_status is None else latest_status.missing_stages,
            latest_run_map_ready=latest_run_map_ready,
            latest_run_failure_stage=None if latest_status is None else latest_status.failure_stage,
            latest_run_failure_message=None if latest_status is None else latest_status.failure_message,
            authoritative_latest_upstream_run_id=upstream_latest_run_id,
            latest_upstream_run_exists=latest_upstream_run_exists,
            source_bucket=getattr(self.settings, "geoglows_forecast_bucket", None) if provider == "geoglows" else None,
            source_zarr_path=latest_source_zarr_path,
        )

    def _parse_last_updated(self, value: Any) -> datetime | None:
        if not isinstance(value, str) or not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _run_status_from_row(self, provider: str, run_row: models.ForecastRun) -> RunReadinessStatusResponse:
        ops = self._run_ops_metadata(run_row)
        completed_set = set(ops.get("completed_stages", []))

        artifact_exists = self.artifacts.exists(provider, run_row.run_id)
        artifact_path = str(self.artifacts.artifact_path(provider, run_row.run_id))
        artifact_row_count = self.artifacts.count_rows(provider, run_row.run_id) if artifact_exists else 0
        timeseries_row_count = self.repo.count_timeseries_rows_for_run(provider, run_row.run_id)
        summary_row_count = self.repo.count_summaries_for_run(provider, run_row.run_id)
        map_row_count = summary_row_count

        raw_meta = dict(ops.get("raw_acquisition", {}))
        artifact_meta = dict(ops.get("artifact", {}))
        ingest_meta = dict(ops.get("ingest", {}))
        summarize_meta = dict(ops.get("summarize", {}))

        artifact_source_completed = artifact_exists
        map_ready = bool(
            artifact_source_completed and timeseries_row_count > 0 and summary_row_count > 0 and map_row_count > 0
        )

        if raw_meta.get("succeeded"):
            completed_set.add(self.STAGE_RAW_ACQUIRED)
        if artifact_exists:
            completed_set.add(self.STAGE_ARTIFACT_PREPARED)
        if timeseries_row_count > 0:
            completed_set.add(self.STAGE_INGESTED)
        if summary_row_count > 0:
            completed_set.add(self.STAGE_SUMMARIZED)
        if map_ready:
            completed_set.add(self.STAGE_MAP_READY)

        completed_set.add(self.STAGE_DISCOVERED)
        completed_stages = [stage for stage in self.STAGE_ORDER if stage in completed_set]

        missing_stages = [stage for stage in self.STAGE_ORDER if stage not in completed_stages]

        raw = RawAcquisitionStatus(
            attempted=bool(raw_meta.get("attempted", False)),
            succeeded=bool(raw_meta.get("succeeded", False)),
            mode=raw_meta.get("mode"),
            source_uri=raw_meta.get("source_uri"),
            staged_raw_path=raw_meta.get("staged_raw_path"),
        )
        artifact = ArtifactStatus(
            exists=artifact_exists,
            path=artifact_meta.get("path") or artifact_path,
            row_count=artifact_row_count,
        )
        ingest = IngestStatus(
            completed=timeseries_row_count > 0,
            timeseries_row_count=timeseries_row_count,
        )
        summarize = SummarizeStatus(
            completed=summary_row_count > 0,
            summary_row_count=summary_row_count,
        )

        current_status = ops.get("current_status") or run_row.ingest_status or self.STAGE_DISCOVERED
        if map_ready:
            current_status = self.STAGE_MAP_READY
        elif summarize.completed:
            current_status = self.STAGE_SUMMARIZED
        elif ingest.completed:
            current_status = self.STAGE_INGESTED
        elif artifact.exists:
            current_status = self.STAGE_ARTIFACT_PREPARED
        elif raw.succeeded:
            current_status = self.STAGE_RAW_ACQUIRED
        else:
            current_status = self.STAGE_DISCOVERED

        authoritative_latest_upstream_run_id = self._latest_upstream_run_id(self._get_provider(provider))
        source_zarr_path = self._source_zarr_path(self._get_provider(provider), run_row.run_id)

        return RunReadinessStatusResponse(
            provider=provider,
            run_id=run_row.run_id,
            current_status=current_status,
            completed_stages=completed_stages,
            missing_stages=missing_stages,
            raw_acquisition=raw,
            artifact=artifact,
            ingest=ingest,
            summarize=summarize,
            map_row_count=map_row_count,
            map_ready=map_ready,
            map_ready_definition=self.MAP_READY_DEFINITION,
            failure_stage=ops.get("failure_stage"),
            failure_message=ops.get("failure_message"),
            last_updated_utc=self._parse_last_updated(ops.get("last_updated_utc")) or run_row.updated_at,
            authoritative_latest_upstream_run_id=authoritative_latest_upstream_run_id,
            upstream_run_exists=self._upstream_run_exists(self._get_provider(provider), run_row.run_id),
            acquisition_mode=self._get_provider(provider).bulk_acquisition_mode(),
            source_bucket=getattr(self.settings, "geoglows_forecast_bucket", None) if provider == "geoglows" else None,
            source_zarr_path=source_zarr_path,
        )

    def get_run_status(self, provider: str, run_id: str) -> RunReadinessStatusResponse:
        self._get_provider(provider)
        resolved = self.resolve_requested_run_id(provider, run_id, require_existing=False)
        if not resolved:
            raise ValueError(f"Run '{run_id}' not found for provider '{provider}'")
        run_row = self.repo.get_run(provider, resolved.run_id)
        if run_row is None:
            raise ValueError(f"Run '{resolved.run_id}' not found for provider '{provider}'")
        return self._run_status_from_row(provider, run_row)

    def resolve_requested_run_id(
        self, provider: str, requested_run_id: str, require_existing: bool = True
    ) -> ForecastRunSchema | None:
        return self._resolve_run(provider, requested_run_id, require_existing=require_existing)

    def _resolve_run(
        self, provider: str, run_id: str, require_existing: bool = True
    ) -> ForecastRunSchema | None:
        if run_id == "latest":
            # Always resolve authoritative upstream latest for providers that support it,
            # and reconcile a local run row for the resolved run_id.
            try:
                return self.discover_latest_run(provider)
            except Exception:
                if require_existing:
                    raise
                latest = self.repo.get_latest_run(provider)
                return None if latest is None else to_run_schema(latest)

        run = self.repo.get_run(provider, run_id)
        if run:
            return to_run_schema(run)
        if require_existing:
            raise ValueError(f"Run '{run_id}' not found for provider '{provider}'")
        return None


def to_run_schema(row: models.ForecastRun) -> ForecastRunSchema:
    return ForecastRunSchema.model_validate(row, from_attributes=True)


def to_return_period_schema(row: models.ForecastProviderReturnPeriod) -> ReturnPeriodSchema:
    return ReturnPeriodSchema.model_validate(row, from_attributes=True)


def to_timeseries_schema(row: models.ForecastProviderReachTimeseries) -> TimeseriesPointSchema:
    return TimeseriesPointSchema.model_validate(row, from_attributes=True)


def to_summary_schema(row: models.ForecastProviderReachSummary) -> ReachSummarySchema:
    return ReachSummarySchema.model_validate(row, from_attributes=True)


def to_map_summary_schema(row: models.ForecastProviderReachSummary) -> MapReachSummarySchema:
    return MapReachSummarySchema(
        provider=row.provider,
        run_id=row.run_id,
        provider_reach_id=row.provider_reach_id,
        peak_time_utc=row.peak_time_utc,
        peak_mean_cms=row.peak_mean_cms,
        peak_median_cms=row.peak_median_cms,
        peak_max_cms=row.peak_max_cms,
        return_period_band=row.return_period_band,
        severity_score=row.severity_score,
        is_flagged=row.is_flagged,
    )
