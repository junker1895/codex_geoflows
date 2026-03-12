from __future__ import annotations

import numpy as np
import pytest

from app.forecast.providers import geoglows_return_periods as loader


class _FakeCoord:
    def __init__(self, values):
        self.values = np.array(values)


class _FakeVar:
    def __init__(self, values):
        self._values = np.array(values)

    def transpose(self, *_dims):
        return _FakeVar(self._values)

    @property
    def values(self):
        return self._values


class _FakeChunk:
    def __init__(self, river_ids, return_periods, vars_dict):
        self.data_vars = vars_dict
        self._river_ids = np.array(river_ids)
        self._return_periods = np.array(return_periods)

    def __getitem__(self, key):
        if key == "river_id":
            return _FakeCoord(self._river_ids)
        if key == "return_period":
            return _FakeCoord(self._return_periods)
        return self.data_vars[key]


class _FakeDataset:
    def __init__(self):
        self.dims = {"river_id": 2, "return_period": 6}
        self.sizes = {"river_id": 2, "return_period": 6}
        self.data_vars = {
            "gumbel": _FakeVar([[1, 2, 3, 4, 5, 6], [10, 20, 30, 40, 50, 60]]),
            "logpearson3": _FakeVar([[2, 3, 4, 5, 6, 7], [11, 21, 31, 41, 51, 61]]),
            "max_simulated": _FakeVar([100, 200]),
        }

    def isel(self, river_id):
        assert river_id.start == 0
        assert river_id.stop == 2
        return _FakeChunk(
            river_ids=[760021611, 760021612],
            return_periods=[2, 5, 10, 25, 50, 100],
            vars_dict=self.data_vars,
        )


class _FakeXR:
    @staticmethod
    def open_zarr(_path, consolidated=False):
        assert consolidated is False
        return _FakeDataset()


def test_chunk_matrix_maps_real_return_period_coordinate_to_columns():
    chunk = _FakeChunk(
        river_ids=[760021611],
        return_periods=[2, 5, 10, 25, 50, 100],
        vars_dict={
            "gumbel": _FakeVar([[10, 20, 30, 40, 50, 60]]),
            "max_simulated": _FakeVar([123.4]),
        },
    )

    rows = loader._chunk_to_return_period_rows(
        chunk=chunk,
        method="gumbel",
        zarr_path="s3://geoglows-v2/retrospective/return-periods.zarr",
        start=0,
        end=1,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.provider_reach_id == "760021611"
    assert row.rp_2 == 10
    assert row.rp_5 == 20
    assert row.rp_10 == 30
    assert row.rp_25 == 40
    assert row.rp_50 == 50
    assert row.rp_100 == 60


def test_method_selection_gumbel_vs_logpearson3(monkeypatch):
    monkeypatch.setattr(loader, "_import_xarray", lambda: _FakeXR)

    gumbel_batches = list(
        loader.iter_geoglows_return_periods_from_zarr("s3://geoglows-v2/retrospective/return-periods.zarr", method="gumbel", batch_size=2)
    )
    lp3_batches = list(
        loader.iter_geoglows_return_periods_from_zarr("s3://geoglows-v2/retrospective/return-periods.zarr", method="logpearson3", batch_size=2)
    )

    assert gumbel_batches[0][0].rp_2 == 1
    assert lp3_batches[0][0].rp_2 == 2


def test_invalid_method_rejected():
    with pytest.raises(Exception, match="Supported methods"):
        list(loader.iter_geoglows_return_periods_from_zarr("s3://geoglows-v2/retrospective/return-periods.zarr", method="bad", batch_size=2))
