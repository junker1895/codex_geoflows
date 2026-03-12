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

    detail = client.get("/forecast/reaches/geoglows/100")
    assert detail.status_code == 200
    assert "timeseries" in detail.json()

    health = client.get("/forecast/health", params={"provider": "geoglows"})
    assert health.status_code == 200
    payload = health.json()
    assert "supports_forecast_stats_rest" in payload
    assert "supports_return_periods_current_backend" in payload
