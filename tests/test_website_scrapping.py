import os

import pandas as pd
import pytest

import engine.gather.WebsiteScraping as WebsiteScraping
import engine.utils.Modes as Modes


@pytest.fixture(scope="function")
def report_scraping_settings(tmpdir):
    return WebsiteScraping.ReportScraperSettings(
        os.path.join(tmpdir, "report_folder"),
        os.path.join(tmpdir, "report_titles_df.pkl"),
        "{{report_id}}.pdf",
        2010,
        2020,
        1,
        [Modes.Mode.a],
        [],
        False,
    )


@pytest.mark.parametrize(
    "agency, url, expected",
    [
        pytest.param(
            "TSB",
            "https://www.tsb.gc.ca/eng/rapports-reports/marine/2020/m20c0101/m20c0101.html",
            True,
            id="TSB pass",
        ),
        pytest.param(
            "TSB",
            "https://www.tsb.gc.ca/eng/rapports-reports/marine/2020/m20c0101/m20c0102.html",
            False,
            id="TSB fail",
        ),
        pytest.param(
            "TAIC", "https://www.taic.org.nz/inquiry/mo-2021-205", True, id="TAIC pass"
        ),
        pytest.param(
            "TAIC", "https://www.taic.org.nz/inquiry/mo-2021-255", False, id="TAIC fail"
        ),
        pytest.param(
            "ATSB",
            "https://www.atsb.gov.au/publications/investigation_reports/2019/mair/mo-2019-007",
            True,
            id="ATSB pass",
        ),
        pytest.param(
            "ATSB",
            "https://www.atsb.gov.au/publications/investigation_reports/2019/mair/mo-2019-008",
            False,
            id="ATSB fail",
        ),
    ],
)
def test_report_collection(report_scraping_settings, agency, url, expected):
    scraper = WebsiteScraping.get_agency_scraper(agency, report_scraping_settings)

    result = scraper.collect_report("test_report_id", url)

    assert result == expected


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
