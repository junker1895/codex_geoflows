from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator

from app.forecast.exceptions import ProviderBackendUnavailableError
from app.forecast.schemas import (
    BulkForecastArtifactRowSchema,
    ForecastRunSchema,
    ReachSummarySchema,
    ReturnPeriodSchema,
    TimeseriesPointSchema,
)


class ForecastProviderAdapter(ABC):
    @abstractmethod
    def get_provider_name(self) -> str: ...

    @abstractmethod
    def discover_latest_run(self) -> ForecastRunSchema: ...

    @abstractmethod
    def fetch_return_periods(self, reach_ids: list[str | int]) -> list[ReturnPeriodSchema]: ...

    @abstractmethod
    def fetch_forecast_timeseries(
        self, run_id: str, reach_ids: list[str | int]
    ) -> list[TimeseriesPointSchema]: ...

    def supports_bulk_acquisition(self) -> bool:
        return False

    def iter_acquired_bulk_records(self, run_id: str) -> Iterator[dict]:
        raise ProviderBackendUnavailableError(
            f"Provider '{self.get_provider_name()}' does not have a configured bulk acquisition source."
        )

    def normalize_bulk_record(self, run_id: str, record: dict) -> BulkForecastArtifactRowSchema | None:
        raise ProviderBackendUnavailableError(
            f"Provider '{self.get_provider_name()}' does not implement bulk normalization."
        )

    @abstractmethod
    def summarize_reach(
        self,
        run_id: str,
        reach_id: str | int,
        timeseries_rows: list[TimeseriesPointSchema],
        return_period_row: ReturnPeriodSchema | None,
    ) -> ReachSummarySchema: ...
