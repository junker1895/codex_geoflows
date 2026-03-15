from __future__ import annotations

from collections.abc import Iterable, Iterator
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
                ("return_period_band", pa.string()),
                ("severity_score", pa.int64()),
                ("is_flagged", pa.bool_()),
            ]
        )

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

        pa, pq = _load_pyarrow()
        schema = self._summary_schema()
        writer = pq.ParquetWriter(path, schema=schema, compression="zstd")
        buffer: list[dict] = []
        count = 0
        try:
            for row in rows:
                payload = row.model_dump(mode="python")
                payload.pop("raw_payload_json", None)
                buffer.append(payload)
                if len(buffer) >= batch_size:
                    writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
                    count += len(buffer)
                    buffer = []
            if buffer:
                writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
                count += len(buffer)
        finally:
            writer.close()
        return path, count

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

        _, pq = _load_pyarrow()
        table = pq.read_table(path)
        for payload in table.to_pylist():
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
        _, pq = _load_pyarrow()
        return int(pq.read_metadata(path).num_rows)

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
