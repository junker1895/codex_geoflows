from datetime import UTC, datetime

import pandas as pd

from app.core.config import Settings
from app.forecast.providers.geoglows import GeoglowsForecastProvider
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
    assert detail.summary.return_period_band == "25"
    assert detail.summary.severity_score == 4
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
    assert detail.summary.return_period_band == "25"
    assert detail.summary.severity_score == 4
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
