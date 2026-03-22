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
    # peak_mean=7.2 (from flow_avg), rp_5=6, rp_10=8 → band "5"
    assert detail.summary.return_period_band == "5"
    assert detail.summary.severity_score == 2
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
    # peak_mean=7.2 (from flow_avg), rp_5=6, rp_10=8 → band "5"
    assert detail.summary.return_period_band == "5"
    assert detail.summary.severity_score == 2
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

    service.prepare_bulk_artifact("geoglows", run.run_id)
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

    service.prepare_bulk_artifact("geoglows", run.run_id)
    service.ingest_forecast_run("geoglows", run.run_id, ingest_mode="bulk")

    # ensure artifact ingestion wrote supported reaches
    assert len(service.list_forecast_map_reaches("geoglows", run_id=run.run_id, limit=20).data) == 0


def test_bulk_mode_does_not_fallback_to_rest_per_reach(db_session):
    class _NoBulkProvider(FakeProvider):
        def __init__(self):
            super().__init__(supports_bulk=False)
            self.rest_called = False

        def supports_bulk_acquisition(self):
            return False

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
        assert "normalized bulk artifact" in str(exc) or "Bulk ingest was requested" in str(exc)
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


def test_bulk_mode_rejects_explicit_reach_ids(db_session):
    service = ForecastService(db_session, Settings(), {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")

    try:
        service.ingest_forecast_run("geoglows", run.run_id, reach_ids=["101"], ingest_mode="bulk")
    except ValueError as exc:
        assert "does not accept explicit reach_ids" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_prepare_bulk_artifact_filters_to_supported_reaches(db_session):
    service = ForecastService(db_session, Settings(), {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["101", "102"])

    artifact_path, count = service.prepare_bulk_artifact("geoglows", run.run_id, filter_to_supported_reaches=True)

    assert artifact_path
    assert count == 6


def test_bulk_ingest_requires_prepared_artifact(db_session):
    service = ForecastService(db_session, Settings(), {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["101"])

    try:
        service.ingest_forecast_run("geoglows", run.run_id, ingest_mode="bulk")
    except ValueError as exc:
        assert "prepare-bulk-artifact" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_prepare_bulk_artifact_if_present_skip_returns_zero(db_session):
    service = ForecastService(db_session, Settings(), {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["101"])

    first_path, first_count = service.prepare_bulk_artifact("geoglows", run.run_id, if_present="overwrite")
    second_path, second_count = service.prepare_bulk_artifact("geoglows", run.run_id, if_present="skip")

    assert first_path == second_path
    assert first_count > 0
    assert second_count == 0


def test_prepare_bulk_artifact_skip_rebuilds_incomplete_public_zarr_artifact(db_session):
    class _PublicModeProvider(FakeProvider):
        def bulk_acquisition_mode(self) -> str:
            return "aws_public_zarr"

    service = ForecastService(db_session, Settings(), {"geoglows": _PublicModeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["101", "102"])

    artifact_path = service.artifacts.artifact_path("geoglows", run.run_id)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        '{"provider":"geoglows","run_id":"%s","provider_reach_id":"101","forecast_time_utc":"2024-01-01T00:00:00+00:00"}\n'
        % run.run_id
    )

    _, count = service.prepare_bulk_artifact("geoglows", run.run_id, if_present="skip")

    assert count > 1


def test_run_status_reports_map_ready_for_bulk_pipeline(db_session):
    settings = Settings(FORECAST_BULK_INGEST_BATCH_SIZE=2)
    service = ForecastService(db_session, settings, {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["101", "102"])
    service.prepare_bulk_artifact("geoglows", run.run_id)
    service.ingest_forecast_run("geoglows", run.run_id, ingest_mode="bulk")
    service.summarize_run("geoglows", run.run_id)

    status = service.get_run_status("geoglows", run.run_id)

    assert status.map_ready is True
    assert status.current_status == "map_ready"
    assert status.artifact.exists is True
    assert status.artifact.row_count > 0
    assert status.ingest.timeseries_row_count > 0
    assert status.summarize.summary_row_count > 0
    assert status.map_row_count > 0
    assert status.failure_stage is None
    assert status.completed_stages == [
        "discovered",
        "raw_acquired",
        "artifact_prepared",
        "ingested",
        "summarized",
        "map_ready",
    ]
    assert status.missing_stages == []


def test_run_status_tracks_missing_artifact_for_bulk_ingest_failure(db_session):
    service = ForecastService(db_session, Settings(), {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["101"])

    try:
        service.ingest_forecast_run("geoglows", run.run_id, ingest_mode="bulk")
    except ValueError:
        pass

    status = service.get_run_status("geoglows", run.run_id)
    assert status.map_ready is False
    assert "artifact_prepared" in status.missing_stages
    assert status.failure_stage == "ingested"
    assert status.failure_message is not None


def test_health_and_status_report_upstream_bulk_context(db_session):
    class _UpstreamAwareFakeProvider(FakeProvider):
        def bulk_acquisition_mode(self) -> str:
            return "aws_public_zarr"

        def get_latest_upstream_run_id(self) -> str:
            return "2026031400"

        def upstream_run_exists(self, run_id: str):
            return run_id == "2024010100"

        def build_source_zarr_path(self, run_id: str) -> str:
            return f"s3://geoglows-v2-forecasts/{run_id}.zarr"

    settings = Settings()
    service = ForecastService(db_session, settings, {"geoglows": _UpstreamAwareFakeProvider()})
    run = service.discover_latest_run("geoglows")

    health = service.get_provider_health("geoglows", refresh_upstream=True)
    status = service.get_run_status("geoglows", run.run_id, refresh_upstream=True)

    assert health.authoritative_latest_upstream_run_id == "2026031400"
    assert health.source_bucket == "geoglows-v2-forecasts"
    assert health.source_zarr_path == f"s3://geoglows-v2-forecasts/{run.run_id}.zarr"
    assert status.authoritative_latest_upstream_run_id == "2026031400"
    assert status.source_bucket == "geoglows-v2-forecasts"
    assert status.source_zarr_path == f"s3://geoglows-v2-forecasts/{run.run_id}.zarr"
    assert status.acquisition_mode == "aws_public_zarr"


def test_latest_resolution_uses_authoritative_upstream_run_across_bulk_lifecycle(db_session, tmp_path):
    class _AuthoritativeLatestProvider(FakeProvider):
        def __init__(self):
            super().__init__(supports_bulk=True)
            self.acquire_run_ids = []
            self.summarize_run_ids = []

        def discover_latest_run(self):
            return ForecastRunSchema(
                provider="geoglows",
                run_id="2026031400",
                run_date_utc=datetime(2026, 3, 14, tzinfo=UTC),
                issued_at_utc=datetime(2026, 3, 14, tzinfo=UTC),
                source_type="geoglows_api",
                ingest_status="pending",
            )

        def get_latest_upstream_run_id(self) -> str:
            return "2026031400"

        def upstream_run_exists(self, run_id: str):
            return run_id == "2026031400"

        def build_source_zarr_path(self, run_id: str) -> str:
            return f"s3://geoglows-v2-forecasts/{run_id}.zarr"

        def acquire_bulk_raw_source(self, run_id: str, overwrite: bool = False) -> str:
            _ = overwrite
            self.acquire_run_ids.append(run_id)
            return f"fake://{run_id}"

        def summarize_reach(self, run_id, reach_id, timeseries_rows, return_period_row):
            self.summarize_run_ids.append(run_id)
            return super().summarize_reach(run_id, reach_id, timeseries_rows, return_period_row)

    provider = _AuthoritativeLatestProvider()
    settings = Settings(FORECAST_BULK_ARTIFACT_DIR=str(tmp_path / "artifacts"))
    service = ForecastService(db_session, settings, {"geoglows": provider})

    # Seed a synthetic stale run that should never win latest resolution.
    service.repo.upsert_run(
        ForecastRunSchema(
            provider="geoglows",
            run_id="2026031410",
            run_date_utc=datetime(2026, 3, 14, 10, tzinfo=UTC),
            issued_at_utc=datetime(2026, 3, 14, 10, tzinfo=UTC),
            source_type="geoglows_api",
            ingest_status="pending",
        )
    )
    service.db.commit()

    service.ingest_return_periods("geoglows", ["101", "102"])

    artifact_path, artifact_count = service.prepare_bulk_artifact("geoglows", "latest")
    ingest_count = service.ingest_forecast_run("geoglows", "latest", ingest_mode="bulk")
    summary_count = service.summarize_run("geoglows", "latest")
    status = service.get_run_status("geoglows", "latest")

    assert "2026031400" in artifact_path
    assert artifact_count > 0
    assert ingest_count > 0
    assert summary_count > 0
    assert provider.acquire_run_ids and set(provider.acquire_run_ids) == {"2026031400"}
    assert provider.summarize_run_ids and set(provider.summarize_run_ids) == {"2026031400"}
    assert status.run_id == "2026031400"
    assert status.authoritative_latest_upstream_run_id == "2026031400"
    assert status.source_zarr_path == "s3://geoglows-v2-forecasts/2026031400.zarr"

    latest = service.get_latest_run("geoglows")
    assert latest is not None
    assert latest.run_id == "2026031400"


def test_discover_latest_run_does_not_reset_existing_stage_progress(db_session):
    service = ForecastService(db_session, Settings(), {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["101", "102"])
    service.prepare_bulk_artifact("geoglows", run.run_id)
    service.ingest_forecast_run("geoglows", run.run_id, ingest_mode="bulk")
    service.summarize_run("geoglows", run.run_id)

    before = service.get_run_status("geoglows", run.run_id)
    assert before.map_ready is True

    service.discover_latest_run("geoglows")
    after = service.get_run_status("geoglows", run.run_id)

    assert after.map_ready is True
    assert after.completed_stages == before.completed_stages
    assert after.missing_stages == []

def test_prepare_bulk_summaries_and_ingest_use_one_row_per_reach(db_session, tmp_path):
    settings = Settings(FORECAST_BULK_ARTIFACT_DIR=str(tmp_path / "artifacts"))
    service = ForecastService(db_session, settings, {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["100", "101", "102", "103", "104", "105"])

    artifact_path, count = service.prepare_bulk_summaries("geoglows", run.run_id, if_present="overwrite")
    ingested = service.ingest_forecast_summaries("geoglows", run.run_id)

    assert artifact_path.endswith("part-000.parquet")
    assert count == 6
    assert ingested == 6
    assert service.repo.count_summaries_for_run("geoglows", run.run_id) == 6
    assert service.repo.count_timeseries_rows_for_run("geoglows", run.run_id) == 0


def test_prepare_bulk_summaries_classifies_from_return_periods(db_session, tmp_path):
    settings = Settings(FORECAST_BULK_ARTIFACT_DIR=str(tmp_path / "artifacts"))
    service = ForecastService(db_session, settings, {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["100"])

    service.prepare_bulk_summaries("geoglows", run.run_id, if_present="overwrite")
    service.ingest_forecast_summaries("geoglows", run.run_id)
    summary = service.repo.get_summary("geoglows", run.run_id, "100")

    assert summary is not None
    # FakeProvider emits peak=22.0 and fake RP thresholds are 10,20,30...
    assert summary.return_period_band == "5"
    assert summary.severity_score == 2
    assert summary.is_flagged is True


def test_ingest_forecast_summaries_reclassifies_with_current_return_periods(db_session, tmp_path):
    """Severity scores must reflect current return periods at ingest time,
    not stale values baked into the artifact."""
    from app.forecast.schemas import ReturnPeriodSchema

    settings = Settings(FORECAST_BULK_ARTIFACT_DIR=str(tmp_path / "artifacts"))
    service = ForecastService(db_session, settings, {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    # FakeProvider RPs: rp_2=10, rp_5=20, rp_10=30; peak=22.0 → severity 2
    service.ingest_return_periods("geoglows", ["100"])
    service.prepare_bulk_summaries("geoglows", run.run_id, if_present="overwrite")

    # Now update return periods so peak=22.0 falls below rp_2
    updated_rp = ReturnPeriodSchema(
        provider="geoglows", provider_reach_id="100",
        rp_2=25, rp_5=50, rp_10=75, rp_25=100, rp_50=150, rp_100=200,
    )
    service.repo.upsert_return_periods([updated_rp])
    db_session.commit()

    # Ingest should re-classify with the updated return periods
    service.ingest_forecast_summaries("geoglows", run.run_id, replace_existing=True)
    summary = service.repo.get_summary("geoglows", run.run_id, "100")

    assert summary is not None
    assert summary.severity_score == 0  # peak 22.0 < rp_2 25.0
    assert summary.return_period_band == "below_2"
    assert summary.is_flagged is False


def test_reach_detail_falls_back_to_provider_on_demand_when_no_timeseries(db_session):
    class _DetailFallbackProvider(FakeProvider):
        def fetch_reach_detail_from_public_zarr(self, run_id: str, provider_reach_id: str, timeseries_limit: int | None = None):
            from app.forecast.schemas import TimeseriesPointSchema

            rows = [
                TimeseriesPointSchema(
                    provider="geoglows",
                    run_id=run_id,
                    provider_reach_id=provider_reach_id,
                    forecast_time_utc=datetime(2024, 1, 1, i, tzinfo=UTC),
                    flow_mean_cms=10 + i,
                    flow_median_cms=10 + i,
                    flow_p25_cms=9 + i,
                    flow_p75_cms=11 + i,
                    flow_max_cms=12 + i,
                    raw_payload_json={"source": "geoglows_public_forecast_zarr", "high_res": 8 + i},
                )
                for i in range(3)
            ]
            return rows if timeseries_limit is None else rows[:timeseries_limit]

    service = ForecastService(db_session, Settings(), {"geoglows": _DetailFallbackProvider()})
    run = service.discover_latest_run("geoglows")
    detail = service.get_reach_detail("geoglows", "100", run_id=run.run_id, timeseries_limit=2)

    assert len(detail.timeseries) == 2
    assert detail.timeseries[0].raw_payload_json["source"] == "geoglows_public_forecast_zarr"


def test_run_status_map_ready_depends_on_summary_rows(db_session, tmp_path):
    settings = Settings(FORECAST_BULK_ARTIFACT_DIR=str(tmp_path / "artifacts"))
    service = ForecastService(db_session, settings, {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["100"])
    service.prepare_bulk_summaries("geoglows", run.run_id, if_present="overwrite")
    service.ingest_forecast_summaries("geoglows", run.run_id)

    status = service.get_run_status("geoglows", run.run_id)
    assert status.map_ready is True
    assert status.ingest.timeseries_row_count == 0


def test_ingest_forecast_summaries_replace_existing_clears_stale_rows(db_session, tmp_path):
    settings = Settings(FORECAST_BULK_ARTIFACT_DIR=str(tmp_path / "artifacts"))
    service = ForecastService(db_session, settings, {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["100", "101", "102", "103", "104", "105"])

    service.prepare_bulk_summaries("geoglows", run.run_id, if_present="overwrite")
    service.ingest_forecast_summaries("geoglows", run.run_id)

    assert service.repo.count_summaries_for_run("geoglows", run.run_id) == 6

    # contaminate with stale value to verify replacement behavior
    row = service.repo.get_summary("geoglows", run.run_id, "100")
    assert row is not None
    row.severity_score = 999
    service.db.commit()

    service.ingest_forecast_summaries("geoglows", run.run_id, replace_existing=True)
    refreshed = service.repo.get_summary("geoglows", run.run_id, "100")
    assert refreshed is not None
    assert refreshed.severity_score != 999


def test_run_status_not_ready_when_summary_ingest_fails(db_session, tmp_path):
    class _FailingReadStore:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        def __getattr__(self, item):
            return getattr(self.wrapped, item)

        def iter_summary_rows(self, provider, run_id):
            raise RuntimeError("broken parquet schema")

    settings = Settings(FORECAST_BULK_ARTIFACT_DIR=str(tmp_path / "artifacts"))
    service = ForecastService(db_session, settings, {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["100"])
    service.prepare_bulk_summaries("geoglows", run.run_id, if_present="overwrite")

    service.artifacts = _FailingReadStore(service.artifacts)
    try:
        service.ingest_forecast_summaries("geoglows", run.run_id)
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected ingest failure")

    status = service.get_run_status("geoglows", run.run_id)
    assert status.map_ready is False
    assert status.failure_stage == "ingested"
    assert "broken parquet schema" in (status.failure_message or "")


def test_local_latest_resolution_avoids_upstream_calls_in_api_paths(db_session):
    class _NoUpstreamProvider(FakeProvider):
        def get_latest_upstream_run_id(self):
            raise AssertionError("upstream should not be called")

        def upstream_run_exists(self, run_id: str):
            raise AssertionError("upstream should not be called")

    service = ForecastService(db_session, Settings(), {"geoglows": _NoUpstreamProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["100"])
    service.prepare_bulk_summaries("geoglows", run.run_id, if_present="overwrite")
    service.ingest_forecast_summaries("geoglows", run.run_id)

    health = service.get_provider_health("geoglows")
    status = service.get_run_status("geoglows", "latest")
    detail = service.get_reach_detail("geoglows", "100", run_id=None, timeseries_limit=1)
    m = service.list_forecast_map_reaches("geoglows", run_id="latest", limit=1)

    assert health.latest_run is not None
    assert health.latest_run.run_id == run.run_id
    assert status.run_id == run.run_id
    assert detail.run.run_id == run.run_id
    assert m.meta.run_id == run.run_id


def test_cli_and_api_status_semantics_agree_after_summary_ingest(db_session, tmp_path):
    settings = Settings(FORECAST_BULK_ARTIFACT_DIR=str(tmp_path / "artifacts"))
    service = ForecastService(db_session, settings, {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["100"])
    service.prepare_bulk_summaries("geoglows", run.run_id, if_present="overwrite")
    service.ingest_forecast_summaries("geoglows", run.run_id)

    cli_like = service.get_run_status("geoglows", run.run_id)
    api_like = service.get_run_status("geoglows", "latest")

    assert cli_like.current_status == api_like.current_status
    assert cli_like.map_ready == api_like.map_ready
    assert cli_like.completed_stages == api_like.completed_stages


def test_no_repo_query_receives_latest_after_resolution(db_session):
    service = ForecastService(db_session, Settings(), {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["100"])
    service.prepare_bulk_summaries("geoglows", run.run_id, if_present="overwrite")
    service.ingest_forecast_summaries("geoglows", run.run_id)

    original_get_map = service.repo.get_map_summaries
    original_get_run = service.repo.get_run

    def _guard_map(*args, **kwargs):
        rid = kwargs.get("run_id") if "run_id" in kwargs else args[1]
        assert rid != "latest"
        return original_get_map(*args, **kwargs)

    def _guard_run(provider, run_id):
        assert run_id != "latest"
        return original_get_run(provider, run_id)

    service.repo.get_map_summaries = _guard_map
    service.repo.get_run = _guard_run

    service.list_forecast_map_reaches("geoglows", run_id="latest", limit=1)
    service.get_run_status("geoglows", "latest")


def test_summary_ingest_persists_final_ops_state(db_session, tmp_path):
    settings = Settings(FORECAST_BULK_ARTIFACT_DIR=str(tmp_path / "artifacts"))
    service = ForecastService(db_session, settings, {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["100"])
    service.prepare_bulk_summaries("geoglows", run.run_id, if_present="overwrite")
    service.ingest_forecast_summaries("geoglows", run.run_id)

    run_row = service.repo.get_run("geoglows", run.run_id)
    assert run_row is not None
    ops = (run_row.metadata_json or {}).get("ops", {})
    assert ops.get("current_status") == "map_ready"
    assert ops.get("map_ready") is True
    assert "map_ready" in ops.get("completed_stages", [])
