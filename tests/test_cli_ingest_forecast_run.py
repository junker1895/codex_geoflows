from typer.testing import CliRunner

import app.cli as cli_mod


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

    def _fake_prepare(service, provider, run_id, filter_to_supported_reaches=True):
        calls["provider"] = provider
        calls["run_id"] = run_id
        calls["filter"] = filter_to_supported_reaches
        return "/tmp/a.jsonl", 3

    monkeypatch.setattr(cli_mod, "_build_service", _fake_build_service)
    monkeypatch.setattr(cli_mod.prepare_bulk_artifact, "run", _fake_prepare)

    runner = CliRunner()
    result = runner.invoke(
        cli_mod.cli,
        ["prepare-bulk-artifact", "--provider", "geoglows", "--run-id", "latest", "--filter-supported"],
    )

    assert result.exit_code == 0
    assert "prepared bulk artifact" in result.stdout
    assert calls["provider"] == "geoglows"
