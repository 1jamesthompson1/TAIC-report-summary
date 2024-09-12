import os
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from ..utils import Modes


class ReportScraperSettings:
    def __init__(
        self,
        report_dir,
        report_titles_file_path,
        file_name_template,
        start_year,
        end_year,
        max_per_year,
        modes: list[Modes.Mode],
        ignored_report_ids: list[str],
        refresh,
    ):
        self.report_dir = report_dir
        self.report_titles_file_path = report_titles_file_path
        self.file_name_template = file_name_template
        self.start_year = start_year
        self.end_year = end_year
        self.max_per_year = max_per_year
        self.refresh = refresh
        self.modes = modes
        self.ignored_report_ids = ignored_report_ids


class ReportScraper:
    """
    Class that will take the output templates and download all the reports from the TAIC website
    These reports can be found manually by going to https://www.taic.org.nz/inquiries
    """

    def __init__(
        self,
        settings: ReportScraperSettings,
    ):
        self.settings = settings
        if os.path.exists(self.settings.report_titles_file_path):
            self.report_titles_df = pd.read_pickle(
                self.settings.report_titles_file_path
            )
        else:
            self.report_titles_df = pd.DataFrame(
                columns=["report_id", "title", "event_type"]
            )

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36 Edg/94.0.992.50",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Connection": "keep-alive",
        }

        # Create a folder to store the downloaded PDFs
        os.makedirs(self.settings.report_dir, exist_ok=True)

    def collect_all(self):
        print(
            "=============================================================================================================================\n"
        )
        print(
            "|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\n"
        )
        print(
            "- - - - - - - - - - - - - - - - - - - - - - - - - - - - Downloading report PDFs - - - - - - - - - - - - - - - - - - - - - -"
        )
        print(
            "|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\n"
        )
        print(
            "=============================================================================================================================\n"
        )
        print(f"  Output directory: {self.settings.report_dir}")
        print(f"  File name template: {self.settings.file_name_template}")
        print(
            f"  Start year: {self.settings.start_year},  End year: {self.settings.end_year}"
        )
        print(f"  Max reports per year: {self.settings.max_per_year}")
        print(f"  Modes: {self.settings.modes}")
        print(f"  Ignoring report ids: {self.settings.ignored_report_ids}")

        # Loop through each mode
        for mode in self.settings.modes:
            self.collect_mode(mode)

    def get_report_urls(self, mode: str, year: int) -> list[tuple[str, str]]:
        """
        Retrieves all the potential report urls and ids for a given mode and year
        """
        raise NotImplementedError

    def get_report_id(self, mode: Modes.Mode, year: int, id: str) -> str:
        return f"{self.agency}_{mode.name}_{year}_{id}"

    def collect_mode(self, mode):
        print(f"======== Downloading reports for mode: {mode.name}==========")

        year_range = [
            year for year in range(self.settings.start_year, self.settings.end_year + 1)
        ]

        for year in (pbar := tqdm(year_range)):
            pbar.set_description(
                f"Downloading reports for mode: {mode.name}, currently doing year: {year}"
            )

            self.collect_year(year, mode)

    def collect_year(self, year, mode):
        # Define the base URL and report ids and download all reports for the mode.

        number_for_year = 0
        for report_id, url in (inner_pbar := tqdm(self.get_report_urls(mode, year))):
            if report_id in self.settings.ignored_report_ids:
                continue

            outcome = self.collect_report(report_id, url, inner_pbar)

            if outcome == "End of reports for this year":
                break
            elif outcome:
                number_for_year += 1

            if number_for_year >= self.settings.max_per_year:
                break

        self.report_titles_df.to_pickle(self.settings.report_titles_file_path)

    def collect_report(self, report_id, url, pbar=None):
        file_name = os.path.join(
            self.settings.report_dir,
            self.settings.file_name_template.replace(r"{{report_id}}", report_id),
        )
        if pbar:
            pbar.set_description(f"  Collecting {url}")

        if not self.settings.refresh and os.path.exists(file_name):
            if pbar:
                pbar.set_description(f"  {file_name} already exists, skipping download")
            return True

        try:
            webpage = requests.get(url, headers=self.headers, timeout=10)
        except requests.exceptions.Timeout:
            if pbar:
                pbar.write(f"  Failed to collect {url}, timeout error")
            return False
        soup = BeautifulSoup(webpage.content, "html.parser")

        if webpage.status_code != 200 and webpage.status_code != 404:
            if pbar:
                pbar.write(
                    f"  Failed to collect {url}, status code: {webpage.status_code}"
                )
            return False
        elif webpage.status_code == 404:
            return False

        if soup.find("h1", text="Page not found"):
            return "End of reports for this year"

        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

        outcome = self.download_report(report_id, file_name, soup, base_url, pbar)
        if not outcome:
            return False

        self.__add_report_metadata_to_df(
            *self.get_report_metadata(report_id, soup, pbar)
        )

        return True

    def download_report(
        self,
        report_id: str,
        file_name: str,
        soup: BeautifulSoup,
        base_url: str,
        pbar=None,
    ):
        # Find all the links that end with .pdf and download them

        pdf_links = [
            a["href"]
            for a in soup.find_all("a", href=True)
            if a["href"].endswith(".pdf")
        ]

        if len(pdf_links) == 0:
            if pbar:
                pbar.write(
                    f"WARNING: Found no suitable PDF link for {report_id}. Will not download any."
                )
            return False
        if len(pdf_links) > 1:
            # Only take one that has "final" but not "interim"
            pdf_links = [
                link
                for link in pdf_links
                if "final" in link.lower() and "interim" not in link.lower()
            ]
            if len(pdf_links) > 1:
                if pbar:
                    links_str = "\n".join(pdf_links)
                    pbar.write(
                        f"WARNING: Found more than one PDF for {report_id}. Will not download any.\n Here are the links: \n {links_str}"
                    )
                return False
            if len(pdf_links) == 0:
                if pbar:
                    links_str = "\n".join(pdf_links)
                    pbar.write(
                        f"WARNING: Found no suitable PDF link for {report_id}. Will not download any.\n Here are the links: \n {links_str}"
                    )
                return False

        link = urljoin(base_url, pdf_links[0])
        if pbar:
            pbar.set_description(f"  Downloading {link}")

        try:
            with open(file_name, "wb") as f:
                f.write(
                    requests.get(
                        link, allow_redirects=True, headers=self.headers, timeout=10
                    ).content
                )
                if pbar:
                    pbar.set_description(f"  Downloaded {file_name}")
        except requests.ReadTimeout:
            os.remove(file_name)
            if pbar:
                pbar.write(f"  {file_name} timed out")
            return False

        return True

    def get_report_metadata(self, report_id: str, soup: BeautifulSoup, pbar=None):
        raise NotImplementedError

    def __add_report_metadata_to_df(self, report_id: str, title: str, event_type: str):
        if self.report_titles_df.query("report_id == @report_id").empty:
            self.report_titles_df.loc[len(self.report_titles_df)] = [
                report_id,
                title,
                event_type,
            ]


class TAICReportScraper(ReportScraper):
    def __init__(self, settings: ReportScraperSettings):
        super().__init__(settings)
        self.agency = "TAIC"

    def get_report_urls(self, mode, year):
        taic_ids = [f"{mode.value}{num:02}" for num in range(100)]
        return [
            (
                self.get_report_id(mode, year, taic_id),
                f"https://www.taic.org.nz/inquiry/{mode.name}o-{year}-{taic_id}",
            )
            for taic_id in taic_ids
        ]

    def get_report_metadata(self, report_id: str, soup: BeautifulSoup, pbar=None):
        title = soup.find("div", class_="field--name-field-inv-title").text

        return report_id, title, None


class ATSBReportScraper(ReportScraper):
    def __init__(self, settings: ReportScraperSettings):
        super().__init__(settings)
        self.agency = "ATSB"

    def get_report_urls(self, mode, year):
        atsb_ids = [f"{mode.value}{num:02}" for num in range(100)]
        return [
            (
                self.get_report_id(mode, year, atsb_id),
                f"https://www.atsb.gov.au/publications/investigation_reports/{year}/report/{mode.name}o-{year}-{atsb_id}",
            )
            for atsb_id in atsb_ids
        ]


class TSBReportScraper(ReportScraper):
    def __init__(self, settings: ReportScraperSettings):
        super().__init__(settings)
        self.agency = "TSB"

    def __get_tsb_mode_letters(self, mode):
        match mode:
            case Modes.Mode.a:
                return ["A", "W", "O", "C", "P", "Q"]

            case Modes.Mode.r:
                return ["H", "T", "W", "C", "D", "V", "M", "E", "S", "Q", "H"]

            case Modes.Mode.m:
                return ["C", "A", "P", "F", "M", "H", "L", "W", "N"]

    def get_report_urls(self, mode, year):
        yy_year = year % 100
        tsb_id = [
            f"{mode.name}{yy_year}{letter}{num:04}"
            for letter in self.__get_tsb_mode_letters(mode)
            for num in range(500)
        ]

        return [
            (
                self.get_report_id(mode, year, tsb_id[-5:]),
                f"https://www.tsb.gc.ca/eng/rapports-reports/{Modes.Mode.as_string(mode)}/{year}/{tsb_id}/{tsb_id}.html".lower(),
            )
            for tsb_id in tsb_id
        ]

    def get_report_metadata(self, report_id: str, soup: BeautifulSoup, pbar=None):
        title_block = soup.find("div", class_="field--name-field-occurrence")

        event_type = title_block.find("strong").text

        legacy_text_div = title_block.find(
            "div", class_="field--name-field-occurrence-legacy-text"
        )
        paragraph = legacy_text_div.find("p")
        paragraph_text = [text for text in paragraph.stripped_strings]
        date_text = (
            title_block.find("div", class_="field--name-field-occurrence-date")
            .find("time")
            .text
        )

        all_text = ", ".join(paragraph_text + [date_text])

        return report_id, all_text, event_type


def get_agency_scraper(agency: str, settings: ReportScraperSettings) -> ReportScraper:
    if agency == "TAIC":
        return TAICReportScraper(settings)
    elif agency == "ATSB":
        return ATSBReportScraper(settings)
    elif agency == "TSB":
        return TSBReportScraper(settings)
    else:
        raise ValueError(f"Unknown agency: {agency}")
