from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path
import json


from app.forecast.exceptions import ForecastValidationError
from app.forecast.schemas import BulkForecastArtifactRowSchema, BulkForecastSummaryArtifactRowSchema


def _load_pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        return pa, pq
    except Exception as exc:
        raise RuntimeError("pyarrow is required for Parquet summary artifacts") from exc


class ForecastArtifactStore:
    def __init__(self, artifact_dir: str, summary_format: str = "parquet") -> None:
        self.base_dir = Path(artifact_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.summary_format = summary_format.lower()

    def artifact_path(self, provider: str, run_id: str) -> Path:
        safe_provider = provider.replace("/", "_")
        safe_run = run_id.replace("/", "_")
        return self.base_dir / provider / f"{safe_provider}_{safe_run}.jsonl"

    def summary_artifact_dir(self, provider: str, run_id: str) -> Path:
        safe_provider = provider.replace("/", "_")
        safe_run = run_id.replace("/", "_")
        return self.base_dir / safe_provider / f"run_id={safe_run}"

    def summary_artifact_path(self, provider: str, run_id: str) -> Path:
        base = self.summary_artifact_dir(provider, run_id)
        if self.summary_format == "jsonl":
            return base / "part-000.jsonl"
        return base / "part-000.parquet"

    def write_rows(self, provider: str, run_id: str, rows: Iterable[BulkForecastArtifactRowSchema]) -> tuple[Path, int]:
        path = self.artifact_path(provider, run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row.model_dump(mode="json"), separators=(",", ":")))
                handle.write("\n")
                count += 1
        return path, count

    def iter_rows(self, provider: str, run_id: str) -> Iterator[BulkForecastArtifactRowSchema]:
        path = self.artifact_path(provider, run_id)
        if not path.exists():
            raise FileNotFoundError(f"bulk artifact does not exist for provider={provider}, run_id={run_id}: {path}")

        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                text = line.strip()
                if not text:
                    continue
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError as exc:
                    raise ForecastValidationError(
                        f"Invalid bulk artifact JSON at line {line_number} in {path}: {exc}"
                    ) from exc
                try:
                    yield BulkForecastArtifactRowSchema.model_validate(payload)
                except Exception as exc:
                    raise ForecastValidationError(
                        f"Invalid bulk artifact row at line {line_number} in {path}: {exc}"
                    ) from exc

    def _summary_schema(self):
        pa, _ = _load_pyarrow()
        return pa.schema(
            [
                ("provider", pa.string()),
                ("run_id", pa.string()),
                ("provider_reach_id", pa.string()),
                ("peak_time_utc", pa.timestamp("us", tz="UTC")),
                ("peak_mean_cms", pa.float64()),
                ("peak_median_cms", pa.float64()),
                ("peak_max_cms", pa.float64()),
                ("now_mean_cms", pa.float64()),
                ("now_max_cms", pa.float64()),
                ("return_period_band", pa.string()),
                ("severity_score", pa.float64()),
                ("is_flagged", pa.bool_()),
            ]
        )

    def _normalize_summary_row(self, row: BulkForecastSummaryArtifactRowSchema) -> dict:
        peak_time_utc = row.peak_time_utc
        if peak_time_utc is not None:
            if peak_time_utc.tzinfo is None:
                peak_time_utc = peak_time_utc.replace(tzinfo=UTC)
            else:
                peak_time_utc = peak_time_utc.astimezone(UTC)

        return {
            "provider": str(row.provider),
            "run_id": str(row.run_id),
            "provider_reach_id": str(row.provider_reach_id),
            "peak_time_utc": peak_time_utc,
            "peak_mean_cms": None if row.peak_mean_cms is None else float(row.peak_mean_cms),
            "peak_median_cms": None if row.peak_median_cms is None else float(row.peak_median_cms),
            "peak_max_cms": None if row.peak_max_cms is None else float(row.peak_max_cms),
            "now_mean_cms": None if row.now_mean_cms is None else float(row.now_mean_cms),
            "now_max_cms": None if row.now_max_cms is None else float(row.now_max_cms),
            "return_period_band": None if row.return_period_band is None else str(row.return_period_band),
            "severity_score": float(row.severity_score),
            "is_flagged": bool(row.is_flagged),
        }

    def _table_from_rows(self, rows: list[dict]):
        pa, _ = _load_pyarrow()
        schema = self._summary_schema()
        arrays = [pa.array([r[field.name] for r in rows], type=field.type) for field in schema]
        return pa.Table.from_arrays(arrays, schema=schema)

    def write_summary_rows(
        self,
        provider: str,
        run_id: str,
        rows: Iterable[BulkForecastSummaryArtifactRowSchema],
        batch_size: int = 5000,
    ) -> tuple[Path, int]:
        path = self.summary_artifact_path(provider, run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()
        if self.summary_format == "jsonl":
            count = 0
            with path.open("w", encoding="utf-8") as handle:
                for row in rows:
                    handle.write(json.dumps(row.model_dump(mode="json"), separators=(",", ":")))
                    handle.write("\n")
                    count += 1
            return path, count

        _, pq = _load_pyarrow()
        schema = self._summary_schema()
        tmp_path = path.with_suffix(".parquet.tmp")
        writer = pq.ParquetWriter(tmp_path, schema=schema, compression="zstd", use_dictionary=False)
        buffer: list[dict] = []
        count = 0
        success = False
        try:
            for row in rows:
                buffer.append(self._normalize_summary_row(row))
                if len(buffer) >= batch_size:
                    writer.write_table(self._table_from_rows(buffer))
                    count += len(buffer)
                    buffer = []
            if buffer:
                writer.write_table(self._table_from_rows(buffer))
                count += len(buffer)
            success = True
        finally:
            writer.close()
            if success:
                tmp_path.rename(path)
            elif tmp_path.exists():
                tmp_path.unlink()
        return path, count

    def _read_summary_parquet_table(self, path: Path):
        _, pq = _load_pyarrow()
        schema = self._summary_schema()
        pf = pq.ParquetFile(path)
        table = pf.read(columns=schema.names)
        return table.cast(schema)

    def iter_summary_rows(self, provider: str, run_id: str) -> Iterator[BulkForecastSummaryArtifactRowSchema]:
        path = self.summary_artifact_path(provider, run_id)
        if not path.exists():
            raise FileNotFoundError(f"summary bulk artifact does not exist for provider={provider}, run_id={run_id}: {path}")

        if path.suffix == ".jsonl":
            with path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    text = line.strip()
                    if not text:
                        continue
                    try:
                        payload = json.loads(text)
                    except json.JSONDecodeError as exc:
                        raise ForecastValidationError(
                            f"Invalid summary bulk artifact JSON at line {line_number} in {path}: {exc}"
                        ) from exc
                    yield BulkForecastSummaryArtifactRowSchema.model_validate(payload)
            return

        table = self._read_summary_parquet_table(path)
        for payload in table.to_pylist():
            payload["provider"] = str(payload["provider"])
            payload["run_id"] = str(payload["run_id"])
            payload["provider_reach_id"] = str(payload["provider_reach_id"])
            payload["return_period_band"] = None if payload["return_period_band"] is None else str(payload["return_period_band"])
            payload["severity_score"] = 0 if payload["severity_score"] is None else int(payload["severity_score"])
            payload["is_flagged"] = bool(payload["is_flagged"])
            payload.setdefault("now_mean_cms", None)
            payload.setdefault("now_max_cms", None)
            payload["raw_payload_json"] = None
            yield BulkForecastSummaryArtifactRowSchema.model_validate(payload)

    def count_summary_rows(self, provider: str, run_id: str) -> int:
        path = self.summary_artifact_path(provider, run_id)
        if not path.exists():
            return 0
        if path.suffix == ".jsonl":
            count = 0
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        count += 1
            return count
        return int(self._read_summary_parquet_table(path).num_rows)

    def summary_schema_string(self, provider: str, run_id: str) -> str:
        path = self.summary_artifact_path(provider, run_id)
        if not path.exists() or path.suffix == ".jsonl":
            return "jsonl/no_schema"
        return str(self._read_summary_parquet_table(path).schema)

    def summary_exists(self, provider: str, run_id: str) -> bool:
        return self.summary_artifact_path(provider, run_id).exists()

    def summary_artifact_size_bytes(self, provider: str, run_id: str) -> int:
        path = self.summary_artifact_path(provider, run_id)
        return path.stat().st_size if path.exists() else 0

    def count_rows(self, provider: str, run_id: str) -> int:
        path = self.artifact_path(provider, run_id)
        if not path.exists():
            return 0

        count = 0
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    count += 1
        return count

    def preview_rows(self, provider: str, run_id: str, limit: int = 5) -> list[dict]:
        rows: list[dict] = []
        for row in self.iter_rows(provider, run_id):
            rows.append(row.model_dump(mode="json"))
            if len(rows) >= limit:
                break
        return rows

    def preview_summary_rows(self, provider: str, run_id: str, limit: int = 5) -> list[dict]:
        rows: list[dict] = []
        for row in self.iter_summary_rows(provider, run_id):
            rows.append(row.model_dump(mode="json"))
            if len(rows) >= limit:
                break
        return rows

    def exists(self, provider: str, run_id: str) -> bool:
        return self.artifact_path(provider, run_id).exists()

    def cleanup_old_runs(self, provider: str, keep_latest: int) -> int:
        provider_dir = self.base_dir / provider
        if keep_latest < 1 or not provider_dir.exists():
            return 0

        files = sorted(provider_dir.glob(f"{provider}_*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
        removed = 0
        for f in files[keep_latest:]:
            f.unlink(missing_ok=True)
            removed += 1
        return removed
