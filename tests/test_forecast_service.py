from app.core.config import Settings
from app.forecast.service import ForecastService
from tests.conftest import FakeProvider


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
