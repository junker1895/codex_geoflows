from datetime import UTC, datetime

from app.core.config import Settings
from app.forecast.cache import DetailCache, ForecastCacheManager
from app.forecast.service import ForecastService


class _BoundedSummaryProvider:
    def get_provider_name(self):
        return "geoglows"

    def discover_latest_run(self):
        from app.forecast.schemas import ForecastRunSchema

        return ForecastRunSchema(
            provider="geoglows",
            run_id="2026031400",
            run_date_utc=datetime(2026, 3, 14, tzinfo=UTC),
            issued_at_utc=datetime(2026, 3, 14, tzinfo=UTC),
            source_type="geoglows_api",
            ingest_status="pending",
        )

    def fetch_return_periods(self, reach_ids):
        from app.forecast.schemas import ReturnPeriodSchema

        return [ReturnPeriodSchema(provider="geoglows", provider_reach_id=str(x), rp_2=1.0) for x in reach_ids]

    def fetch_forecast_timeseries(self, run_id, reach_ids):
        return []

    def supports_bulk_acquisition(self):
        return True

    def bulk_acquisition_mode(self):
        return "aws_public_zarr"

    def set_supported_reach_filter(self, reach_ids):
        self._supported = reach_ids

    def iter_bulk_summary_records(self, run_id, **kwargs):
        self.kwargs = kwargs
        for rid in ["101", "102", "103"]:
            yield {
                "provider_reach_id": rid,
                "peak_time_utc": datetime(2026, 3, 14, tzinfo=UTC).isoformat(),
                "peak_mean_cms": 1.0,
                "peak_median_cms": 1.0,
                "peak_max_cms": 2.0,
            }

    def normalize_bulk_summary_record(self, run_id, record):
        from app.forecast.schemas import BulkForecastSummaryArtifactRowSchema

        return BulkForecastSummaryArtifactRowSchema(
            provider="geoglows",
            run_id=run_id,
            provider_reach_id=record["provider_reach_id"],
            peak_time_utc=datetime.fromisoformat(record["peak_time_utc"]),
            peak_mean_cms=record["peak_mean_cms"],
            peak_median_cms=record["peak_median_cms"],
            peak_max_cms=record["peak_max_cms"],
        )

    def summarize_reach(self, run_id, reach_id, timeseries_rows, return_period_row):
        raise NotImplementedError


def test_prepare_bulk_summaries_uses_bounded_defaults(db_session):
    provider = _BoundedSummaryProvider()
    settings = Settings(FORECAST_DEFAULT_MAX_BLOCKS=2, FORECAST_DEFAULT_MAX_REACHES=3, FORECAST_DEFAULT_MAX_SECONDS=10)
    service = ForecastService(db_session, settings, {"geoglows": provider})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["101", "102", "103"])

    service.prepare_bulk_summaries("geoglows", run.run_id)

    assert provider.kwargs["max_blocks"] == 2
    assert provider.kwargs["max_reaches"] == 3
    assert provider.kwargs["max_seconds"] == 10
    assert provider.kwargs["full_run"] is False


def test_forecast_cache_cleanup_command(tmp_path):
    mgr = ForecastCacheManager(str(tmp_path))
    (tmp_path / "x").write_text("1")
    assert mgr.cleanup() == 1


def test_detail_cache_ttl_behavior():
    cache = DetailCache(ttl_seconds=60, max_items=1)
    cache.set("k1", [1])
    assert cache.get("k1") == [1]
    cache.set("k2", [2])
    assert cache.get("k1") is None
    assert cache.get("k2") == [2]
