from collections.abc import Generator
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.db.session import get_db_session
from app.db.base import Base
from app.main import app


class FakeProvider:
    def get_provider_name(self) -> str:
        return "geoglows"

    def discover_latest_run(self):
        from app.forecast.schemas import ForecastRunSchema

        return ForecastRunSchema(
            provider="geoglows",
            run_id="2024010100",
            run_date_utc=datetime(2024, 1, 1, tzinfo=UTC),
            issued_at_utc=datetime(2024, 1, 1, tzinfo=UTC),
            source_type="geoglows_api",
            ingest_status="pending",
        )

    def fetch_return_periods(self, reach_ids):
        from app.forecast.schemas import ReturnPeriodSchema

        return [
            ReturnPeriodSchema(provider="geoglows", provider_reach_id=str(r), rp_2=10, rp_5=20, rp_10=30, rp_25=40, rp_50=50, rp_100=60)
            for r in reach_ids
        ]

    def fetch_forecast_timeseries(self, run_id, reach_ids):
        from app.forecast.schemas import TimeseriesPointSchema

        rows = []
        for r in reach_ids:
            for i, flow in enumerate([5.0, 12.0, 22.0]):
                rows.append(
                    TimeseriesPointSchema(
                        provider="geoglows",
                        run_id=run_id,
                        provider_reach_id=str(r),
                        forecast_time_utc=datetime(2024, 1, 1, i, tzinfo=UTC),
                        flow_mean_cms=flow,
                        flow_max_cms=flow,
                    )
                )
        return rows

    def summarize_reach(self, run_id, reach_id, timeseries_rows, return_period_row):
        from app.forecast.classify import classify_peak_flow
        from app.forecast.schemas import ReachSummarySchema

        peak = max((x.flow_max_cms or 0 for x in timeseries_rows), default=None)
        result = classify_peak_flow(peak, return_period_row)
        return ReachSummarySchema(
            provider="geoglows",
            run_id=run_id,
            provider_reach_id=str(reach_id),
            peak_max_cms=peak,
            return_period_band=result.return_period_band,
            severity_score=result.severity_score,
            is_flagged=result.is_flagged,
        )


@pytest.fixture
def db_session() -> Generator[Session, None, None]:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    db = SessionLocal()
    yield db
    db.close()


@pytest.fixture
def client(db_session: Session) -> Generator[TestClient, None, None]:
    def override_db():
        yield db_session

    app.dependency_overrides = {}
    app.dependency_overrides[get_db_session] = override_db
    c = TestClient(app)
    yield c
    app.dependency_overrides = {}
