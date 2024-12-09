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
        os.path.join(pytest.output_config["folder_name"], "report_titles.pkl"),
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
        return WebsiteScraping.TAICReportScraper(
            os.path.join(
                pytest.output_config.get("folder_name"),
                pytest.output_config.get("taic_website_reports_table_file_name"),
            ),
            settings,
        )
    elif agency == "ATSB":
        return WebsiteScraping.ATSBReportScraper(
            os.path.join(
                pytest.output_config.get("folder_name"),
                pytest.output_config.get("atsb_website_reports_table_file_name"),
            ),
            settings,
        )
    elif agency == "TSB":
        return WebsiteScraping.TSBReportScraper(settings)
    else:
        raise ValueError(f"Unknown agency: {agency}")


@pytest.mark.parametrize(
    "agency, url, report_id, expected",
    [
        pytest.param(
            "TSB",
            "https://www.tsb.gc.ca/eng/rapports-reports/marine/2020/m20c0101/m20c0101.html",
            "TSB_m_2020_c01010",
            True,
            id="TSB pass",
        ),
        pytest.param(
            "TSB",
            "https://www.tsb.gc.ca/eng/rapports-reports/marine/2020/m20c0101/m20c0102.html",
            "TSB_m_2020_c0102",
            False,
            id="TSB fail",
        ),
        pytest.param(
            "TAIC",
            "https://www.taic.org.nz/inquiry/mo-2021-205",
            "TAIC_m_2021_205",
            True,
            id="TAIC pass",
        ),
        pytest.param(
            "TAIC",
            "https://www.taic.org.nz/inquiry/mo-2021-255",
            "TAIC_m_2021_255",
            False,
            id="TAIC fail",
        ),
        pytest.param(
            "ATSB",
            "https://www.atsb.gov.au/publications/investigation_reports/2019/mair/mo-2019-007",
            "ATSB_m_2019_007",
            True,
            id="ATSB pass",
        ),
        pytest.param(
            "ATSB",
            "https://www.atsb.gov.au/publications/investigation_reports/2019/mair/mo-2019-008",
            "ATSB_m_2019_008",
            False,
            id="ATSB fail",
        ),
    ],
)
def test_report_collection(report_scraping_settings, agency, url, report_id, expected):
    scraper = get_agency_scraper(agency, report_scraping_settings)

    result = scraper.collect_report(report_id, url)

    assert result == expected


@pytest.mark.parametrize(
    "agency, expected_urls",
    [
        pytest.param("TSB", [54, 24, 20, 13, 19, 10, 15, 10, 13], id="TSB"),
        pytest.param("TAIC", [11, 12, 3, 28, 8, 4, 12, 3, 5], id="TAIC"),
        pytest.param("ATSB", [93, 179, 50, 6, 25, 17, 15, 8, 2], id="ATSB"),
    ],
)
def test_agency_website_scraper(report_scraping_settings, agency, expected_urls):
    report_scraping_settings.start_year = 2004
    report_scraping_settings.end_year = 2021
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
        pytest.param("TSB", 15, id="TSB"),
        pytest.param("TAIC", 15, id="TAIC"),
        pytest.param("ATSB", 15, id="ATSB"),
    ],
)
def test_agency_website_scraper_collecting_all_reports(
    report_scraping_settings, agency, expected_count
):
    report_scraping_settings.refresh = True

    report_scraping_settings.start_year = 2008
    report_scraping_settings.end_year = 2012

    scraper = get_agency_scraper(agency, report_scraping_settings)

    assert scraper

    scraper.collect_all()

    assert len(os.listdir(report_scraping_settings.report_dir)) == expected_count


def test_ATSB_safety_issue_scrape():
    output_path = os.path.join(
        pytest.output_config["folder_name"],
        pytest.output_config["atsb_website_safety_issues_file_name"],
    )
    report_titles = os.path.join(
        pytest.output_config["folder_name"],
        pytest.output_config["report_titles_df_file_name"],
    )
    atsb_webscraper = WebsiteScraping.ATSBSafetyIssueScraper(
        output_file_path=output_path,
        report_titles_file_path=report_titles,
        refresh=True,
    )

    atsb_webscraper.extract_safety_issues_from_website()

    output = pd.read_pickle(output_path)

    assert len(output) >= 388

    required_ids = ["ATSB_MO-2008-013-SI-04", "ATSB_AO-2023-008-SI-01"]

    output_long = pd.concat(output["safety_issues"].dropna().tolist(), axis=0)

    for id in required_ids:
        assert id in output_long["safety_issue_id"].unique()
