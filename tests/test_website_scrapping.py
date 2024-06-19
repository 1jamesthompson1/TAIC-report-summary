import os

import pandas as pd
import pytest

import engine.gather.WebsiteScraping as WebsiteScraping
import engine.utils.Modes as Modes


def test_collect_all(tmpdir):
    report_folder = tmpdir.join("report_folder")
    report_titles = tmpdir.join("report_titles_df.pkl")

    scraper = WebsiteScraping.ReportScraping(
        report_folder,
        report_titles,
        "report_{{report_id}}.pdf",
        2010,
        2020,
        1,
        [Modes.Mode.a],
        [],
        False,
    )

    try:
        scraper.collect_all()
    except Exception as e:
        pytest.fail("Error occurred while collecting all reports\n" + str(e))

    assert os.path.exists(report_folder)

    assert len(os.listdir(report_folder)) == 11

    assert os.path.exists(report_titles)
    report_titles = pd.read_pickle(report_titles)

    assert len(report_titles) == 11
