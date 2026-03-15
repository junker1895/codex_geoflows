from datetime import UTC, datetime

from app.forecast.artifacts import ForecastArtifactStore
from app.forecast.schemas import BulkForecastSummaryArtifactRowSchema


def test_summary_artifact_defaults_to_parquet(tmp_path):
    store = ForecastArtifactStore(str(tmp_path), summary_format="parquet")
    path, count = store.write_summary_rows(
        "geoglows",
        "2026031400",
        [
            BulkForecastSummaryArtifactRowSchema(
                provider="geoglows",
                run_id="2026031400",
                provider_reach_id="760021611",
                peak_time_utc=datetime(2026, 3, 14, 0, 0, tzinfo=UTC),
                peak_mean_cms=1.0,
                peak_median_cms=1.0,
                peak_max_cms=2.0,
                return_period_band="2",
                severity_score=1,
                is_flagged=True,
                raw_payload_json={"debug": "drop"},
            )
        ],
    )
    assert count == 1
    assert path.suffix == ".parquet"
    rows = list(store.iter_summary_rows("geoglows", "2026031400"))
    assert len(rows) == 1
    assert rows[0].provider_reach_id == "760021611"
    assert rows[0].raw_payload_json is None
