from typer.testing import CliRunner

import app.cli as cli_mod


class _StubService:
    pass


def test_cli_ingest_forecast_run_single_reach(monkeypatch):
    calls = {}

    def _fake_build_service():
        return _StubService()

    def _fake_run(service, provider, run_id, reach_ids):
        calls["service"] = service
        calls["provider"] = provider
        calls["run_id"] = run_id
        calls["reach_ids"] = reach_ids
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
    assert calls["provider"] == "geoglows"
    assert calls["run_id"] == "latest"
    assert calls["reach_ids"] == ["760021611"]


def test_cli_ingest_forecast_run_bulk_mode_without_reach_ids(monkeypatch):
    calls = {}

    def _fake_build_service():
        return _StubService()

    def _fake_run(service, provider, run_id, reach_ids):
        calls["service"] = service
        calls["provider"] = provider
        calls["run_id"] = run_id
        calls["reach_ids"] = reach_ids
        return 77

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
        ],
    )

    assert result.exit_code == 0
    assert "upserted timeseries rows: 77" in result.stdout
    assert calls["provider"] == "geoglows"
    assert calls["run_id"] == "latest"
    assert calls["reach_ids"] is None
