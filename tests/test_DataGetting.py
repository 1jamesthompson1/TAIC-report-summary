import os

import pytest

import engine.gather.DataGetting as DataGetting


def test_get_recommendations(tmpdir):
    output_file = tmpdir.join("recommendations.pkl")

    dataGetter = DataGetting.DataGetter(
        "data",
        "https://raw.githubusercontent.com/1jamesthompson1/TAIC-report-summary/main/data/",
        False,
    )

    dataGetter.get_recommendations(
        "cleaned_TAIC_recommendations_2024_04_04.csv", output_file
    )

    assert os.path.exists(output_file)


def test_failed(tmpdir):
    output_file = tmpdir.join("everyones_names.pkl")

    dataGetter = DataGetting.DataGetter(
        "data",
        "https://raw.githubusercontent.com/1jamesthompson1/TAIC-report-summary/main/data/",
        False,
    )

    with pytest.raises(FileNotFoundError):
        dataGetter.get_recommendations("everyones_names.csv", output_file)
