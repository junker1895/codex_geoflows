from typer.testing import CliRunner

import app.cli as cli_mod


class _FakeDataset:
    dims = {"river_id": 2, "return_period": 6}

    class _Vars(dict):
        def keys(self):
            return super().keys()

    data_vars = _Vars({"gumbel": object(), "logpearson3": object()})


def test_return_periods_zarr_open_command_uses_shared_helper(monkeypatch):
    calls = {}

    def _fake_open(*, xr, zarr_path):
        calls["zarr_path"] = zarr_path
        calls["xr_name"] = getattr(xr, "__name__", "")
        return _FakeDataset()

    monkeypatch.setattr(cli_mod, "open_geoglows_public_return_periods_zarr", _fake_open)

    runner = CliRunner()
    result = runner.invoke(
        cli_mod.cli,
        [
            "return-periods-zarr-open",
            "--zarr-path",
            "s3://geoglows-v2/retrospective/return-periods.zarr",
            "--method",
            "gumbel",
        ],
    )

    assert result.exit_code == 0
    assert calls["zarr_path"] == "s3://geoglows-v2/retrospective/return-periods.zarr"
    assert "selected method: gumbel" in result.stdout
    assert "dataset dims:" in result.stdout
    assert "dataset variables:" in result.stdout
