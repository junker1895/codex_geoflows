from typer.testing import CliRunner
from datetime import UTC, datetime

import app.cli as cli_mod
from app.forecast.schemas import ArtifactStatus, IngestStatus, RawAcquisitionStatus, RunReadinessStatusResponse, SummarizeStatus


class _StubService:
    pass


def test_cli_ingest_forecast_run_single_reach_uses_rest_single_mode(monkeypatch):
    calls = {}

    def _fake_build_service():
        return _StubService()

    def _fake_run(service, provider, run_id, reach_ids, ingest_mode=None):
        calls["service"] = service
        calls["provider"] = provider
        calls["run_id"] = run_id
        calls["reach_ids"] = reach_ids
        calls["ingest_mode"] = ingest_mode
        return 11

    monkeypatch.setattr(cli_mod, "_build_service", _fake_build_service)
    monkeypatch.setattr(cli_mod.ingest_forecast_run, "run", _fake_run)

    runner = CliRunner()
    result = runner.invoke(
        cli_mod.cli,
        [
            "ingest-forecast-run",
            "--provider",
            "geoglows",
            "--run-id",
            "latest",
            "--reach-id",
            "760021611",
        ],
    )

    assert result.exit_code == 0
    assert "upserted timeseries rows: 11" in result.stdout
    assert calls["reach_ids"] == ["760021611"]
    assert calls["ingest_mode"] == "rest_single"


def test_cli_ingest_forecast_run_bulk_mode_without_reach_ids(monkeypatch):
    calls = {}

    def _fake_build_service():
        return _StubService()

    def _fake_run(service, provider, run_id, reach_ids, ingest_mode=None):
        calls["provider"] = provider
        calls["run_id"] = run_id
        calls["reach_ids"] = reach_ids
        calls["ingest_mode"] = ingest_mode
        return 77

    monkeypatch.setattr(cli_mod, "_build_service", _fake_build_service)
    monkeypatch.setattr(cli_mod.ingest_forecast_run, "run", _fake_run)

    runner = CliRunner()
    result = runner.invoke(cli_mod.cli, ["ingest-forecast-run", "--provider", "geoglows", "--run-id", "latest"])

    assert result.exit_code == 0
    assert calls["reach_ids"] is None
    assert calls["ingest_mode"] == "bulk"


def test_cli_ingest_forecast_run_rejects_invalid_mode():
    runner = CliRunner()
    result = runner.invoke(cli_mod.cli, ["ingest-forecast-run", "--mode", "invalid"])
    assert result.exit_code != 0
    assert "--mode must be one of" in result.stdout


def test_cli_ingest_forecast_run_bulk_mode_rejects_reach_ids():
    runner = CliRunner()
    result = runner.invoke(
        cli_mod.cli,
        ["ingest-forecast-run", "--mode", "bulk", "--reach-id", "760021611"],
    )
    assert result.exit_code != 0
    assert "cannot be combined" in result.stdout


def test_cli_ingest_forecast_run_rest_single_requires_reach_ids():
    runner = CliRunner()
    result = runner.invoke(cli_mod.cli, ["ingest-forecast-run", "--mode", "rest_single"])
    assert result.exit_code != 0
    assert "requires at least one --reach-id" in result.stdout


def test_cli_prepare_bulk_artifact(monkeypatch):
    calls = {}

    def _fake_build_service():
        return _StubService()

    def _fake_prepare(service, provider, run_id, filter_to_supported_reaches=True, if_present="skip", overwrite_raw=False):
        calls["provider"] = provider
        calls["run_id"] = run_id
        calls["filter"] = filter_to_supported_reaches
        calls["if_present"] = if_present
        calls["overwrite_raw"] = overwrite_raw
        return "/tmp/a.jsonl", 3

    monkeypatch.setattr(cli_mod, "_build_service", _fake_build_service)
    monkeypatch.setattr(cli_mod.prepare_bulk_artifact, "run", _fake_prepare)

    runner = CliRunner()
    result = runner.invoke(
        cli_mod.cli,
        ["prepare-bulk-artifact", "--provider", "geoglows", "--run-id", "latest", "--filter-supported", "--if-present", "overwrite", "--overwrite-raw"],
    )

    assert result.exit_code == 0
    assert "prepared bulk artifact" in result.stdout
    assert calls["provider"] == "geoglows"
    assert calls["if_present"] == "overwrite"
    assert calls["overwrite_raw"] is True


def _resolved_latest_status() -> RunReadinessStatusResponse:
    return RunReadinessStatusResponse(
        provider="geoglows",
        run_id="2026031400",
        current_status="artifact_prepared",
        completed_stages=["discovered", "raw_acquired", "artifact_prepared"],
        missing_stages=["ingested", "summarized", "map_ready"],
        raw_acquisition=RawAcquisitionStatus(attempted=True, succeeded=True, mode="aws_public_zarr"),
        artifact=ArtifactStatus(exists=True, path="/tmp/a.jsonl", row_count=10),
        ingest=IngestStatus(completed=False, timeseries_row_count=0),
        summarize=SummarizeStatus(completed=False, summary_row_count=0),
        map_row_count=0,
        map_ready=False,
        map_ready_definition="x",
        last_updated_utc=datetime(2026, 3, 14, tzinfo=UTC),
        authoritative_latest_upstream_run_id="2026031400",
        upstream_run_exists=True,
        acquisition_mode="aws_public_zarr",
        source_bucket="geoglows-v2-forecasts",
        source_zarr_path="s3://geoglows-v2-forecasts/2026031400.zarr",
    )


def test_cli_inspect_run_artifact_latest_shows_authoritative_resolved_run(monkeypatch):
    class _Svc:
        artifacts = type("_A", (), {"preview_rows": staticmethod(lambda *_a, **_k: [])})()

        def get_run_status(self, provider, run_id):
            assert provider == "geoglows"
            assert run_id == "latest"
            return _resolved_latest_status()

    monkeypatch.setattr(cli_mod, "_build_service", lambda: _Svc())
    runner = CliRunner()
    result = runner.invoke(
        cli_mod.cli,
        ["inspect-run-artifact", "--provider", "geoglows", "--run-id", "latest"],
    )
    assert result.exit_code == 0
    assert "run_id: 2026031400" in result.stdout


def test_cli_run_status_latest_shows_authoritative_resolved_run(monkeypatch):
    class _Svc:
        def get_run_status(self, provider, run_id):
            assert provider == "geoglows"
            assert run_id == "latest"
            return _resolved_latest_status()

    monkeypatch.setattr(cli_mod, "_build_service", lambda: _Svc())
    runner = CliRunner()
    result = runner.invoke(
        cli_mod.cli,
        ["run-status", "--provider", "geoglows", "--run-id", "latest"],
    )
    assert result.exit_code == 0
    assert "run_id: 2026031400" in result.stdout
    assert "authoritative_latest_upstream_run_id: 2026031400" in result.stdout
