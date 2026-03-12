from pathlib import Path

from app.forecast.providers.geoglows_return_periods import load_geoglows_return_periods_from_path


def test_parse_local_geoglows_return_period_rows_from_csv(tmp_path: Path):
    dataset = tmp_path / "geoglows_rp.csv"
    dataset.write_text(
        "river_id,return_period_2,return_period_5,return_period_10,return_period_25,return_period_50,return_period_100\n"
        "760021611,10,20,30,40,50,60\n"
    )

    rows = load_geoglows_return_periods_from_path(dataset)

    assert len(rows) == 1
    row = rows[0]
    assert row.provider == "geoglows"
    assert row.provider_reach_id == "760021611"
    assert row.rp_2 == 10
    assert row.rp_100 == 60
    assert row.metadata_json is not None
    assert row.metadata_json["source"] == "local_file"
