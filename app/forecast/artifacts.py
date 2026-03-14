from __future__ import annotations

from collections.abc import Iterable, Iterator
from pathlib import Path
import json

from app.forecast.exceptions import ForecastValidationError
from app.forecast.schemas import BulkForecastArtifactRowSchema, BulkForecastSummaryArtifactRowSchema


class ForecastArtifactStore:
    def __init__(self, artifact_dir: str) -> None:
        self.base_dir = Path(artifact_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def artifact_path(self, provider: str, run_id: str) -> Path:
        safe_provider = provider.replace("/", "_")
        safe_run = run_id.replace("/", "_")
        return self.base_dir / provider / f"{safe_provider}_{safe_run}.jsonl"


    def summary_artifact_path(self, provider: str, run_id: str) -> Path:
        safe_provider = provider.replace("/", "_")
        safe_run = run_id.replace("/", "_")
        return self.base_dir / provider / f"{safe_provider}_{safe_run}_summaries.jsonl"

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



    def write_summary_rows(
        self, provider: str, run_id: str, rows: Iterable[BulkForecastSummaryArtifactRowSchema]
    ) -> tuple[Path, int]:
        path = self.summary_artifact_path(provider, run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row.model_dump(mode="json"), separators=(",", ":")))
                handle.write("\n")
                count += 1
        return path, count

    def iter_summary_rows(self, provider: str, run_id: str) -> Iterator[BulkForecastSummaryArtifactRowSchema]:
        path = self.summary_artifact_path(provider, run_id)
        if not path.exists():
            raise FileNotFoundError(f"summary bulk artifact does not exist for provider={provider}, run_id={run_id}: {path}")

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
                try:
                    yield BulkForecastSummaryArtifactRowSchema.model_validate(payload)
                except Exception as exc:
                    raise ForecastValidationError(
                        f"Invalid summary bulk artifact row at line {line_number} in {path}: {exc}"
                    ) from exc

    def count_summary_rows(self, provider: str, run_id: str) -> int:
        path = self.summary_artifact_path(provider, run_id)
        if not path.exists():
            return 0

        count = 0
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    count += 1
        return count

    def summary_exists(self, provider: str, run_id: str) -> bool:
        return self.summary_artifact_path(provider, run_id).exists()

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
