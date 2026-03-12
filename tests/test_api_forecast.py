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
