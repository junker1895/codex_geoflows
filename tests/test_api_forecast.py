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
    assert "bulk_acquisition_mode" in payload
    assert "bulk_raw_source_reachable" in payload
    assert "latest_run_has_timeseries" in payload
    assert "latest_run_has_summaries" in payload
    assert "latest_run_artifact_exists" in payload
    assert "latest_run_artifact_row_count" in payload
    assert "latest_run_summary_count" in payload
    assert "latest_run_map_count" in payload
    assert "latest_run_status" in payload
    assert "latest_run_missing_stages" in payload
    assert "latest_run_failure_stage" in payload
    assert "latest_run_failure_message" in payload
    assert "latest_run_map_ready" in payload

    run_status = client.get("/forecast/runs/geoglows/2024010100/status")
    assert run_status.status_code == 200
    status_payload = run_status.json()
    assert status_payload["provider"] == "geoglows"
    assert status_payload["run_id"] == "2024010100"
    assert "current_status" in status_payload
    assert "completed_stages" in status_payload
    assert "missing_stages" in status_payload
    assert "raw_acquisition" in status_payload
    assert "artifact" in status_payload
    assert "ingest" in status_payload
    assert "summarize" in status_payload


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


def test_health_refresh_flag_defaults_false(client, monkeypatch):
    class _Svc:
        def get_provider_health(self, provider: str, refresh_upstream: bool = False):
            from app.forecast.schemas import ProviderHealthResponse

            assert provider == "geoglows"
            assert refresh_upstream is False
            return ProviderHealthResponse(
                provider="geoglows",
                enabled=True,
                latest_run=None,
                ingest_status=None,
                summary_count=0,
            )

    monkeypatch.setattr("app.api.routes.forecast.get_forecast_service", lambda _db: _Svc())
    response = client.get("/forecast/health", params={"provider": "geoglows"})
    assert response.status_code == 200


def test_health_refresh_flag_true(client, monkeypatch):
    class _Svc:
        def get_provider_health(self, provider: str, refresh_upstream: bool = False):
            from app.forecast.schemas import ProviderHealthResponse

            assert provider == "geoglows"
            assert refresh_upstream is True
            return ProviderHealthResponse(
                provider="geoglows",
                enabled=True,
                latest_run=None,
                ingest_status=None,
                summary_count=0,
            )

    monkeypatch.setattr("app.api.routes.forecast.get_forecast_service", lambda _db: _Svc())
    response = client.get("/forecast/health", params={"provider": "geoglows", "refresh_upstream": "true"})
    assert response.status_code == 200


def test_map_latest_matches_explicit_run(client, db_session):
    _seed(db_session)
    explicit = client.get("/forecast/map/reaches", params={"provider": "geoglows", "run_id": "2024010100", "limit": 5})
    latest = client.get("/forecast/map/reaches", params={"provider": "geoglows", "run_id": "latest", "limit": 5})
    assert explicit.status_code == 200
    assert latest.status_code == 200
    assert latest.json()["meta"]["run_id"] == "2024010100"
    assert explicit.json()["data"] == latest.json()["data"]


def test_status_latest_matches_explicit_run(client, db_session):
    _seed(db_session)
    explicit = client.get("/forecast/runs/geoglows/2024010100/status")
    latest = client.get("/forecast/runs/geoglows/latest/status")
    assert explicit.status_code == 200
    assert latest.status_code == 200
    assert latest.json()["run_id"] == "2024010100"
    assert explicit.json()["current_status"] == latest.json()["current_status"]
    assert explicit.json()["map_ready"] == latest.json()["map_ready"]


def test_map_detail_parity_for_known_reach(client, db_session):
    _seed(db_session)
    map_resp = client.get("/forecast/map/reaches", params={"provider": "geoglows", "run_id": "latest", "limit": 1})
    assert map_resp.status_code == 200
    map_row = map_resp.json()["data"][0]

    detail_resp = client.get(f"/forecast/reaches/geoglows/{map_row['provider_reach_id']}", params={"run_id": "latest", "timeseries_limit": 20})
    assert detail_resp.status_code == 200
    detail_summary = detail_resp.json()["summary"]
    assert detail_summary is not None
    assert detail_summary["peak_max_cms"] == map_row["peak_max_cms"]
    assert detail_summary["return_period_band"] == map_row["return_period_band"]


def test_api_health_and_status_match_cli_semantics_after_summary_ingest(client, db_session):
    _seed(db_session)
    service = ForecastService(db_session, Settings(), {"geoglows": FakeProvider()})
    cli_status = service.get_run_status("geoglows", "latest")

    api_status = client.get("/forecast/runs/geoglows/latest/status")
    api_health = client.get("/forecast/health", params={"provider": "geoglows"})

    assert api_status.status_code == 200
    assert api_health.status_code == 200
    status_payload = api_status.json()
    health_payload = api_health.json()

    assert status_payload["current_status"] == cli_status.current_status
    assert status_payload["map_ready"] == cli_status.map_ready
    assert health_payload["latest_run_status"] == cli_status.current_status
    assert health_payload["latest_run_map_ready"] == cli_status.map_ready


def test_map_repeated_requests_do_not_fail(client, db_session):
    _seed(db_session)
    for _ in range(20):
        response = client.get("/forecast/map/reaches", params={"provider": "geoglows", "run_id": "latest", "limit": 5})
        assert response.status_code == 200
