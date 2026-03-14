from datetime import UTC, datetime

import numpy as np

from app.core.config import Settings
from app.forecast.providers import geoglows_forecast_zarr as helper
from app.forecast.providers.geoglows import GeoglowsForecastProvider


class _FakeS3FSModule:
    class S3FileSystem:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def ls(self, _bucket, detail=False):
            assert detail is False
            return [
                "geoglows-v2-forecasts/2026031200.zarr",
                "geoglows-v2-forecasts/2026031400.zarr",
                "geoglows-v2-forecasts/not-a-run",
            ]

        def exists(self, path):
            return path.endswith("2026031400.zarr")


class _FakeCoord:
    def __init__(self, values):
        self.values = np.array(values)
        self.dtype = self.values.dtype


class _FakeDataArray:
    def __init__(self, values, dims, coords, chunks=None):
        self.values = np.array(values)
        self.dims = tuple(dims)
        self.coords = coords
        self.chunks = chunks

    def isel(self, indexers):
        dims = list(self.dims)
        values = self.values
        coords = dict(self.coords)
        for dim, sel in indexers.items():
            axis = dims.index(dim)
            values = np.take(values, range(sel.start, sel.stop), axis=axis) if isinstance(sel, slice) else np.take(values, [sel], axis=axis)
            if isinstance(sel, int):
                dims.pop(axis)
                coords.pop(dim, None)
        if values.ndim == 0:
            return _FakeDataArray(np.array([values.item()]), (), coords, None)
        return _FakeDataArray(values, dims, coords, self.chunks)

    def transpose(self, *dims):
        perm = [self.dims.index(d) for d in dims]
        transposed = np.transpose(self.values, axes=perm)
        transposed_chunks = None
        if self.chunks:
            transposed_chunks = tuple(self.chunks[idx] for idx in perm)
        return _FakeDataArray(transposed, dims, self.coords, transposed_chunks)


class _FakeDataset:
    def __init__(self):
        self.coords = {
            "time": _FakeCoord([np.datetime64("2026-03-14T00:00:00"), np.datetime64("2026-03-14T01:00:00")]),
            "river_id": _FakeCoord([760021611, 760021612]),
            "ensemble": _FakeCoord(["high_res", "ens_1", "ens_2"]),
        }
        self.data_vars = {
            "Qout": _FakeDataArray(
                values=[
                    [[10.0, 12.0, 14.0], [20.0, 22.0, 24.0]],
                    [[30.0, 32.0, 34.0], [40.0, 42.0, 44.0]],
                ],
                dims=("river_id", "time", "ensemble"),
                coords=self.coords,
                chunks=((1, 1), (2,), (3,)),
            )
        }
        self.sizes = {"river_id": 2, "time": 2, "ensemble": 3}
        self.attrs = {"title": "fake"}

    def __getitem__(self, key):
        if key in self.data_vars:
            return self.data_vars[key]
        return self.coords[key]


class _FakeXR:
    @staticmethod
    def open_zarr(path, storage_options=None):
        assert path == "s3://geoglows-v2-forecasts/2026031400.zarr"
        assert storage_options == {
            "anon": True,
            "client_kwargs": {"region_name": "us-west-2"},
        }
        return _FakeDataset()


def test_latest_run_discovery_parses_zarr_suffix():
    run_id = helper.discover_latest_forecast_run_id(
        s3fs_module=_FakeS3FSModule,
        bucket="geoglows-v2-forecasts",
        region="us-west-2",
        use_anon=True,
        run_suffix=".zarr",
    )
    assert run_id == "2026031400"


def test_open_config_uses_public_anon_and_region():
    ds = helper.open_geoglows_public_forecast_run_zarr(
        xr=_FakeXR,
        run_id="2026031400",
        bucket="geoglows-v2-forecasts",
        region="us-west-2",
        use_anon=True,
    )
    assert "Qout" in ds.data_vars


def test_provider_normalizes_qout_members_to_artifact_rows(monkeypatch):
    settings = Settings()
    provider = GeoglowsForecastProvider(settings)
    monkeypatch.setattr(provider, "_import_xarray", lambda: _FakeXR)

    rows = list(provider.iter_raw_bulk_records("2026031400", "ignored"))
    normalized = provider.normalize_bulk_record("2026031400", rows[0])

    assert len(rows) == 4
    assert normalized is not None
    assert normalized.provider_reach_id == "760021611"
    assert normalized.forecast_time_utc == datetime(2026, 3, 14, 0, 0, tzinfo=UTC)
    assert normalized.flow_mean_cms == 12.0
    assert normalized.flow_median_cms == 12.0
    assert normalized.flow_p25_cms == 11.0
    assert normalized.flow_p75_cms == 13.0
    assert normalized.flow_max_cms == 14.0
    assert normalized.raw_payload_json["high_res"] == 10.0
    assert {r["raw_payload_json"]["block_index"] for r in rows} == {1, 2}


def test_provider_filters_supported_reaches_within_block(monkeypatch):
    settings = Settings()
    provider = GeoglowsForecastProvider(settings)
    monkeypatch.setattr(provider, "_import_xarray", lambda: _FakeXR)
    provider.set_supported_reach_filter({"760021611"})

    rows = list(provider.iter_raw_bulk_records("2026031400", "ignored"))
    provider.set_supported_reach_filter(None)

    assert len(rows) == 2
    assert all(row["provider_reach_id"] == "760021611" for row in rows)


def test_run_exists_uses_bucket_and_suffix():
    assert helper.run_exists(
        s3fs_module=_FakeS3FSModule,
        bucket="geoglows-v2-forecasts",
        region="us-west-2",
        use_anon=True,
        run_id="2026031400",
    )


def test_chunk_aligned_windows_follow_chunk_sizes():
    assert helper.chunk_aligned_windows(10, [4, 4, 2]) == [(0, 4), (4, 8), (8, 10)]


def test_describe_forecast_dataset_reports_dims_and_chunking():
    summary = helper.describe_forecast_dataset(_FakeDataset(), "Qout")
    assert summary["dims"] == {"river_id": 2, "time": 2, "ensemble": 3}
    assert summary["chunking"]["river_id"] == [1, 1]
    assert summary["detected_time_dim"] == "time"
