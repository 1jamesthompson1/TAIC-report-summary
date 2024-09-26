import itertools
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
        2005,
        2015,
        1,
        [Modes.Mode.a, Modes.Mode.r, Modes.Mode.m],
        [],
        False,
    )


def get_agency_scraper(
    agency: str, settings: WebsiteScraping.ReportScraperSettings
) -> WebsiteScraping.ReportScraper:
    if agency == "TAIC":
        return WebsiteScraping.TAICReportScraper(settings)
    elif agency == "ATSB":
        return WebsiteScraping.ATSBReportScraper(
            settings,
            os.path.join(
                pytest.output_config.get("folder_name"),
                pytest.output_config.get("atsb_historic_aviation_df_file_name"),
            ),
        )
    elif agency == "TSB":
        return WebsiteScraping.TSBReportScraper(settings)
    else:
        raise ValueError(f"Unknown agency: {agency}")


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


@pytest.mark.parametrize(
    "agency, expected_urls",
    [
        pytest.param("TSB", [54, 24, 20, 13, 19, 10, 15, 10, 13], id="TSB"),
        pytest.param("TAIC", [13, 11, 3, 29, 8, 4, 12, 3, 5], id="TAIC"),
        pytest.param("ATSB", [93, 179, 50, 6, 25, 17, 15, 8, 3], id="ATSB"),
    ],
)
def test_agency_website_scraper(report_scraping_settings, agency, expected_urls):
    scraper = get_agency_scraper(agency, report_scraping_settings)

    assert scraper

    assert scraper.agency == agency

    assert isinstance(scraper.agency_reports, pd.DataFrame)

    errors = []
    for (mode, year), expected_len in zip(
        itertools.product(
            [Modes.Mode.a, Modes.Mode.r, Modes.Mode.m], [2005, 2013, 2020]
        ),
        expected_urls,
    ):
        urls = scraper.get_report_urls(mode, year)

        try:
            assert len(urls) == expected_len
        except AssertionError:
            errors.append(f"{agency} {mode} {year}: {len(urls)} != {expected_len}")

    if errors:
        pytest.fail("\n" + "\n".join(errors))


@pytest.mark.parametrize(
    "agency, expected_count",
    [
        pytest.param("TSB", 33, id="TSB"),
        pytest.param("TAIC", 33, id="TAIC"),
        pytest.param("ATSB", 33, id="ATSB"),
    ],
)
def test_agency_website_scraper_collecting_all_reports(
    report_scraping_settings, agency, expected_count
):
    scraper = get_agency_scraper(agency, report_scraping_settings)

    assert scraper

    scraper.collect_all()

    assert len(os.listdir(report_scraping_settings.report_dir)) == expected_count
