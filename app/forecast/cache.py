from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import os
import shutil


class ForecastCacheManager:
    def __init__(self, cache_dir: str) -> None:
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def apply_process_env(self) -> None:
        os.environ.setdefault("TMPDIR", str(self.cache_dir / "tmp"))
        os.environ.setdefault("TEMP", str(self.cache_dir / "tmp"))
        os.environ.setdefault("TMP", str(self.cache_dir / "tmp"))
        os.environ.setdefault("XDG_CACHE_HOME", str(self.cache_dir / "xdg"))

    def cleanup(self) -> int:
        if not self.cache_dir.exists():
            return 0
        removed = 0
        for child in self.cache_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)
            removed += 1
        return removed

    def size_bytes(self) -> int:
        if not self.cache_dir.exists():
            return 0
        total = 0
        for path in self.cache_dir.rglob("*"):
            if path.is_file():
                total += path.stat().st_size
        return total


class DetailCache:
    def __init__(self, ttl_seconds: int, max_items: int) -> None:
        self.ttl_seconds = ttl_seconds
        self.max_items = max_items
        self._store: dict[str, tuple[datetime, object]] = {}

    def get(self, key: str):
        item = self._store.get(key)
        if item is None:
            return None
        expires_at, value = item
        if expires_at <= datetime.now(UTC):
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: object) -> None:
        if len(self._store) >= self.max_items:
            oldest = sorted(self._store.items(), key=lambda i: i[1][0])[0][0]
            self._store.pop(oldest, None)
        self._store[key] = (datetime.now(UTC) + timedelta(seconds=self.ttl_seconds), value)
