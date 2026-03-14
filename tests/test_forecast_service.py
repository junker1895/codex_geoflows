from datetime import UTC, datetime

import pandas as pd

from app.core.config import Settings
from app.forecast.providers.geoglows import GeoglowsForecastProvider
from app.forecast.schemas import ForecastRunSchema
from app.forecast.service import ForecastService
from tests.conftest import FakeProvider


class _MockGeoglowsForecastOnly:
    @staticmethod
    def forecast_stats(river_id, data_source=None):
        assert data_source == "rest"
        return pd.DataFrame(
            [
                {
                    "forecast_time_utc": datetime(2024, 1, 1, tzinfo=UTC),
                    "flow_avg": "3.2",
                    "flow_med": "2.9",
                    "flow_25p": "2.0",
                    "flow_75p": "4.3",
                    "flow_max": "5.1",
                    "flow_min": "1.0",
                    "high_res": "4.9",
                },
                {
                    "forecast_time_utc": datetime(2024, 1, 1, 1, tzinfo=UTC),
                    "flow_avg": "7.2",
                    "flow_med": "6.0",
                    "flow_25p": "5.5",
                    "flow_75p": "8.1",
                    "flow_max": "9.4",
                    "flow_min": "2.0",
                    "high_res": "8.8",
                },
            ]
        )


def test_service_ingest_and_summary(db_session):
    service = ForecastService(db_session, Settings(), {"geoglows": FakeProvider()})

    run = service.discover_latest_run("geoglows")
    assert run.run_id
    assert service.ingest_return_periods("geoglows", ["101"]) == 1
    assert service.ingest_forecast_run("geoglows", "latest", ["101"]) == 3
    assert service.summarize_run("geoglows", "latest") == 1

    detail = service.get_reach_detail("geoglows", "101")
    assert detail.summary is not None
    summaries = service.get_reach_summaries("geoglows")
    assert len(summaries) == 1


def test_verified_workflow_forecast_only_summary_has_peak_values(db_session):
    settings = Settings(GEOGLOWS_DATA_SOURCE="rest")
    provider = GeoglowsForecastProvider(settings, geoglows_module=_MockGeoglowsForecastOnly())
    service = ForecastService(db_session, settings, {"geoglows": provider})

    run = service.discover_latest_run("geoglows")
    count = service.ingest_forecast_run("geoglows", run.run_id, ["760021611"])
    assert count == 2

    detail = service.get_reach_detail("geoglows", "760021611", run_id=run.run_id)
    assert detail.timeseries[0].flow_mean_cms is not None
    assert detail.timeseries[0].flow_max_cms is not None

    summarized = service.summarize_run("geoglows", run.run_id)
    assert summarized == 1

    detail_after = service.get_reach_detail("geoglows", "760021611", run_id=run.run_id)
    assert detail_after.summary is not None
    assert detail_after.summary.peak_mean_cms is not None
    assert detail_after.summary.peak_median_cms is not None
    assert detail_after.summary.peak_max_cms is not None
    assert detail_after.summary.return_period_band == "unknown"
    assert detail_after.summary.severity_score == 0


def test_import_local_geoglows_return_periods_upsert_and_classify(db_session, tmp_path):
    settings = Settings(GEOGLOWS_DATA_SOURCE="rest")
    provider = GeoglowsForecastProvider(settings, geoglows_module=_MockGeoglowsForecastOnly())
    service = ForecastService(db_session, settings, {"geoglows": provider})

    run = service.discover_latest_run("geoglows")
    service.ingest_forecast_run("geoglows", run.run_id, ["760021611"])

    dataset = tmp_path / "rp.csv"
    dataset.write_text(
        "rivid,return_period_2,return_period_5,return_period_10,return_period_25,return_period_50,return_period_100\n"
        "760021611,3,6,8,10,11,12\n"
    )

    assert service.import_geoglows_return_periods(str(dataset)) == 1
    assert service.import_geoglows_return_periods(str(dataset)) == 1

    service.summarize_run("geoglows", run.run_id)
    detail = service.get_reach_detail("geoglows", "760021611", run_id=run.run_id)

    assert detail.summary is not None
    assert detail.summary.return_period_band == "10"
    assert detail.summary.severity_score == 3
    assert detail.summary.is_flagged is True


def test_health_reports_local_return_period_availability(db_session, tmp_path):
    settings = Settings(GEOGLOWS_DATA_SOURCE="rest")
    provider = GeoglowsForecastProvider(settings, geoglows_module=_MockGeoglowsForecastOnly())
    service = ForecastService(db_session, settings, {"geoglows": provider})

    before = service.get_provider_health("geoglows")
    assert before.local_return_periods_available is False

    dataset = tmp_path / "rp.csv"
    dataset.write_text("river_id,return_period_2\n760021611,3\n")
    service.import_geoglows_return_periods(str(dataset))

    after = service.get_provider_health("geoglows")
    assert after.local_return_periods_available is True


def test_import_geoglows_return_periods_zarr_upsert_and_classify(db_session, monkeypatch):
    settings = Settings(GEOGLOWS_DATA_SOURCE="rest")
    provider = GeoglowsForecastProvider(settings, geoglows_module=_MockGeoglowsForecastOnly())
    service = ForecastService(db_session, settings, {"geoglows": provider})

    run = service.discover_latest_run("geoglows")
    service.ingest_forecast_run("geoglows", run.run_id, ["760021611"])

    def _fake_batches(**_kwargs):
        from app.forecast.schemas import ReturnPeriodSchema

        yield [
            ReturnPeriodSchema(
                provider="geoglows",
                provider_reach_id="760021611",
                rp_2=3,
                rp_5=6,
                rp_10=8,
                rp_25=10,
                rp_50=11,
                rp_100=12,
                metadata_json={"method": "gumbel"},
            )
        ]

    monkeypatch.setattr("app.forecast.service.iter_geoglows_return_periods_from_zarr", _fake_batches)

    assert service.import_geoglows_return_periods_zarr(batch_size=1) == 1

    service.summarize_run("geoglows", run.run_id)
    detail = service.get_reach_detail("geoglows", "760021611", run_id=run.run_id)

    assert detail.summary is not None
    assert detail.summary.return_period_band == "10"
    assert detail.summary.severity_score == 3
    assert detail.summary.is_flagged is True


def test_summary_with_return_periods_below_two_not_flagged(db_session, tmp_path):
    settings = Settings(GEOGLOWS_DATA_SOURCE="rest")
    provider = GeoglowsForecastProvider(settings, geoglows_module=_MockGeoglowsForecastOnly())
    service = ForecastService(db_session, settings, {"geoglows": provider})

    run = service.discover_latest_run("geoglows")
    service.ingest_forecast_run("geoglows", run.run_id, ["760021611"])

    dataset = tmp_path / "rp.csv"
    dataset.write_text(
        "rivid,return_period_2,return_period_5,return_period_10,return_period_25,return_period_50,return_period_100\n"
        "760021611,27516.609375,30000,35000,40000,45000,50000\n"
    )
    service.import_geoglows_return_periods(str(dataset))

    service.summarize_run("geoglows", run.run_id)
    detail = service.get_reach_detail("geoglows", "760021611", run_id=run.run_id)

    assert detail.summary is not None
    assert detail.summary.return_period_band == "below_2"
    assert detail.summary.severity_score == 0
    assert detail.summary.is_flagged is False


def test_list_forecast_map_reaches_latest_and_filters(db_session):
    service = ForecastService(db_session, Settings(), {"geoglows": FakeProvider()})

    old_run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["old"])
    service.ingest_forecast_run("geoglows", old_run.run_id, ["old"])
    service.summarize_run("geoglows", old_run.run_id, reach_ids=["old"])

    new_run = ForecastRunSchema(
        provider="geoglows",
        run_id="2024010200",
        run_date_utc=datetime(2024, 1, 2, tzinfo=UTC),
        issued_at_utc=datetime(2024, 1, 2, tzinfo=UTC),
        source_type="geoglows_api",
        ingest_status="pending",
    )
    service.repo.upsert_run(new_run)
    db_session.commit()
    service.ingest_return_periods("geoglows", ["760021611", "760021612"])
    service.ingest_forecast_run("geoglows", new_run.run_id, ["760021611", "760021612"])
    service.summarize_run("geoglows", new_run.run_id)

    high = service.repo.get_summary("geoglows", new_run.run_id, "760021611")
    assert high is not None
    high.severity_score = 4
    high.is_flagged = True

    low = service.repo.get_summary("geoglows", new_run.run_id, "760021612")
    assert low is not None
    low.severity_score = 0
    low.is_flagged = False
    db_session.commit()

    response = service.list_forecast_map_reaches("geoglows", run_id="latest", min_severity_score=1)
    assert response.meta.run_id == new_run.run_id
    assert response.meta.provider == "geoglows"
    assert response.meta.filters.min_severity_score == 1
    assert response.data
    assert all(item.provider_reach_id != "old" for item in response.data)

    flagged_response = service.list_forecast_map_reaches("geoglows", run_id="latest", flagged_only=True)
    assert len(flagged_response.data) == 1
    assert flagged_response.data[0].provider_reach_id == "760021611"


def test_list_forecast_map_reaches_has_lightweight_contract(db_session):
    service = ForecastService(db_session, Settings(), {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["760021611"])
    service.ingest_forecast_run("geoglows", run.run_id, ["760021611"])
    service.summarize_run("geoglows", run.run_id)

    response = service.list_forecast_map_reaches("geoglows", run_id=run.run_id, bbox="1,2,3,4")
    payload = response.model_dump()

    assert payload["data"][0]["provider_reach_id"] == "760021611"
    assert payload["meta"]["filters"]["bbox"] == "1,2,3,4"
    assert "timeseries" not in payload["data"][0]
    assert "return_periods" not in payload["data"][0]
    assert "raw_payload_json" not in payload["data"][0]


def test_map_reaches_reads_summary_table_not_timeseries(db_session):
    service = ForecastService(db_session, Settings(), {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_forecast_run("geoglows", run.run_id, ["760021611"])

    response = service.list_forecast_map_reaches("geoglows", run_id=run.run_id)
    assert response.data == []
    assert response.meta.count == 0


def test_bulk_ingest_uses_supported_reaches_and_chunks(db_session):
    settings = Settings(FORECAST_BULK_INGEST_BATCH_SIZE=2)
    service = ForecastService(db_session, settings, {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["101", "102", "103", "104", "105"])

    rows = service.ingest_forecast_run("geoglows", run.run_id, ingest_mode="bulk")
    assert rows == 15

    summaries = service.summarize_run("geoglows", run.run_id)
    assert summaries == 5

    map_rows = service.list_forecast_map_reaches("geoglows", run_id=run.run_id, limit=10).data
    assert len(map_rows) == 5


def test_bulk_ingest_requires_supported_reaches(db_session):
    service = ForecastService(db_session, Settings(), {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")

    try:
        service.ingest_forecast_run("geoglows", run.run_id, ingest_mode="bulk")
    except ValueError as exc:
        assert "No supported reaches found" in str(exc)
    else:
        raise AssertionError("expected ValueError when supported reaches are unavailable")


def test_bulk_ingest_fetches_forecasts_in_chunks(db_session):
    class _ChunkTrackingProvider(FakeProvider):
        def __init__(self):
            self.calls = []

        def fetch_forecast_timeseries(self, run_id, reach_ids):
            self.calls.append(list(reach_ids))
            return super().fetch_forecast_timeseries(run_id, reach_ids)

    provider = _ChunkTrackingProvider()
    settings = Settings(FORECAST_BULK_INGEST_BATCH_SIZE=2)
    service = ForecastService(db_session, settings, {"geoglows": provider})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["101", "102", "103", "104", "105"])

    service.ingest_forecast_run("geoglows", run.run_id, ingest_mode="bulk")

    assert provider.calls == [["101", "102"], ["103", "104"], ["105"]]


def test_bulk_mode_does_not_fallback_to_rest_per_reach(db_session):
    class _NoBulkProvider(FakeProvider):
        def __init__(self):
            super().__init__(supports_bulk=False)
            self.rest_called = False

        def fetch_forecast_timeseries(self, run_id, reach_ids):
            self.rest_called = True
            return super().fetch_forecast_timeseries(run_id, reach_ids)

    provider = _NoBulkProvider()
    service = ForecastService(db_session, Settings(), {"geoglows": provider})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["101"])

    try:
        service.ingest_forecast_run("geoglows", run.run_id, ingest_mode="bulk")
    except ValueError as exc:
        assert "Bulk ingest was requested" in str(exc)
    else:
        raise AssertionError("expected ValueError")

    assert provider.rest_called is False


def test_rest_single_ingest_mode_explicit_path(db_session):
    service = ForecastService(db_session, Settings(), {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")

    count = service.ingest_forecast_run("geoglows", run.run_id, reach_ids=["101"], ingest_mode="rest_single")

    assert count == 3
    detail = service.get_reach_detail("geoglows", "101", run_id=run.run_id)
    assert len(detail.timeseries) == 3
