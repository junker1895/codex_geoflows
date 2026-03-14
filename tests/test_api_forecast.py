from app.core.config import Settings
from app.forecast.service import ForecastService
from tests.conftest import FakeProvider


def _seed(db):
    service = ForecastService(db, Settings(), {"geoglows": FakeProvider()})
    service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["100"])
    service.ingest_forecast_run("geoglows", "latest", ["100"])
    service.summarize_run("geoglows", "latest")


def test_api_endpoints(client, db_session):
    _seed(db_session)

    r = client.get("/health")
    assert r.status_code == 200

    providers = client.get("/forecast/providers")
    assert providers.status_code == 200
    assert "geoglows" in providers.json()

    latest = client.get("/forecast/runs/latest", params={"provider": "geoglows"})
    assert latest.status_code == 200

    summary = client.get("/forecast/summary", params={"provider": "geoglows"})
    assert summary.status_code == 200
    assert len(summary.json()) == 1

    detail = client.get("/forecast/reaches/geoglows/100", params={"timeseries_limit": 2})
    assert detail.status_code == 200
    payload_detail = detail.json()
    assert "timeseries" in payload_detail
    assert len(payload_detail["timeseries"]) == 2
    assert payload_detail["timeseries"][0]["provider_reach_id"] == "100"
    first = payload_detail["timeseries"][0]
    assert {
        "provider_reach_id",
        "flow_mean_cms",
        "flow_median_cms",
        "flow_p25_cms",
        "flow_p75_cms",
        "flow_max_cms",
        "raw_payload_json",
    }.issubset(set(first.keys()))

    summary_payload = payload_detail["summary"]
    assert {
        "peak_time_utc",
        "peak_mean_cms",
        "peak_median_cms",
        "peak_max_cms",
        "return_period_band",
        "severity_score",
        "is_flagged",
    }.issubset(set(summary_payload.keys()))

    health = client.get("/forecast/health", params={"provider": "geoglows"})
    assert health.status_code == 200
    payload = health.json()
    assert "supports_forecast_stats_rest" in payload
    assert "supports_return_periods_current_backend" in payload
    assert "local_return_periods_available" in payload
    assert "supports_bulk_forecast_ingest" in payload
    assert "bulk_acquisition_configured" in payload
    assert "latest_run_has_timeseries" in payload
    assert "latest_run_has_summaries" in payload
    assert "latest_run_artifact_exists" in payload
    assert "latest_run_map_ready" in payload


def test_map_reaches_endpoint_contract_and_filters(client, db_session):
    _seed(db_session)

    # Ensure filter behavior is observable in the API response.
    service = ForecastService(db_session, Settings(), {"geoglows": FakeProvider()})
    summary = service.repo.get_summary("geoglows", "2024010100", "100")
    assert summary is not None
    summary.severity_score = 3
    summary.is_flagged = True
    db_session.commit()

    response = client.get(
        "/forecast/map/reaches",
        params={
            "provider": "geoglows",
            "run_id": "latest",
            "bbox": "-10,-5,10,5",
            "flagged_only": "true",
            "min_severity_score": "1",
            "limit": "10",
        },
    )
    assert response.status_code == 200
    payload = response.json()

    assert payload["meta"]["provider"] == "geoglows"
    assert payload["meta"]["run_id"] == "2024010100"
    assert payload["meta"]["count"] == 1
    assert payload["meta"]["filters"]["bbox"] == "-10,-5,10,5"
    assert payload["meta"]["filters"]["flagged_only"] is True
    assert payload["meta"]["filters"]["min_severity_score"] == 1

    row = payload["data"][0]
    expected_keys = {
        "provider",
        "run_id",
        "provider_reach_id",
        "peak_time_utc",
        "peak_mean_cms",
        "peak_median_cms",
        "peak_max_cms",
        "return_period_band",
        "severity_score",
        "is_flagged",
    }
    assert set(row.keys()) == expected_keys
    assert row["provider_reach_id"] == "100"


def test_map_reaches_requires_valid_provider(client):
    response = client.get("/forecast/map/reaches", params={"provider": "unknown"})
    assert response.status_code == 400


def test_map_reaches_after_bulk_ingest_returns_multiple_rows(client, db_session):
    service = ForecastService(db_session, Settings(FORECAST_BULK_INGEST_BATCH_SIZE=2), {"geoglows": FakeProvider()})
    run = service.discover_latest_run("geoglows")
    service.ingest_return_periods("geoglows", ["100", "101", "102"])
    service.prepare_bulk_artifact("geoglows", run.run_id)
    service.ingest_forecast_run("geoglows", run.run_id, ingest_mode="bulk")
    service.summarize_run("geoglows", run.run_id)

    response = client.get("/forecast/map/reaches", params={"provider": "geoglows", "run_id": run.run_id, "limit": 10})
    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["count"] == 3
    assert len(payload["data"]) == 3
