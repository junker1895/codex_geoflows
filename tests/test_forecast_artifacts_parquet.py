from datetime import UTC, datetime

from app.forecast.artifacts import ForecastArtifactStore
from app.forecast.schemas import BulkForecastSummaryArtifactRowSchema


def _row(reach_id: str, run_id: str = "2026031400") -> BulkForecastSummaryArtifactRowSchema:
    return BulkForecastSummaryArtifactRowSchema(
        provider="geoglows",
        run_id=run_id,
        provider_reach_id=reach_id,
        peak_time_utc=datetime(2026, 3, 14, 0, 0, tzinfo=UTC),
        peak_mean_cms=1.0,
        peak_median_cms=1.0,
        peak_max_cms=2.0,
        return_period_band="2",
        severity_score=1,
        is_flagged=True,
        raw_payload_json={"debug": "drop"},
    )


def test_summary_artifact_defaults_to_parquet(tmp_path):
    store = ForecastArtifactStore(str(tmp_path), summary_format="parquet")
    path, count = store.write_summary_rows("geoglows", "2026031400", [_row("760021611")], batch_size=1)

    assert count == 1
    assert path.suffix == ".parquet"
    rows = list(store.iter_summary_rows("geoglows", "2026031400"))
    assert len(rows) == 1
    assert rows[0].provider_reach_id == "760021611"
    assert rows[0].run_id == "2026031400"
    assert rows[0].raw_payload_json is None


def test_summary_parquet_schema_is_stable_across_batches(tmp_path):
    store = ForecastArtifactStore(str(tmp_path), summary_format="parquet")
    rows = [_row("r1"), _row("r2"), _row("r3")]
    store.write_summary_rows("geoglows", "2026031400", rows, batch_size=1)

    schema_text = store.summary_schema_string("geoglows", "2026031400")
    assert "provider: string" in schema_text
    assert "run_id: string" in schema_text
    assert "provider_reach_id: string" in schema_text
    assert "severity_score: double" in schema_text


def test_summary_parquet_read_is_not_hive_partition_ambiguous(tmp_path):
    store = ForecastArtifactStore(str(tmp_path), summary_format="parquet")
    store.write_summary_rows("geoglows", "2026031400", [_row("r1"), _row("r2")], batch_size=1)
    loaded = list(store.iter_summary_rows("geoglows", "2026031400"))
    assert [x.run_id for x in loaded] == ["2026031400", "2026031400"]
