import os
import re
from urllib.parse import urljoin, urlparse

import hrequests
import hrequests.exceptions
import pandas as pd
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
                columns=["report_id", "title", "event_type", "misc"]
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

    def get_report_urls(self, mode: Modes.Mode, year: int) -> list[tuple[str, str]]:
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

            if outcome:
                number_for_year += 1

            if number_for_year >= self.settings.max_per_year:
                break

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
            webpage = hrequests.get(url, headers=self.headers, timeout=30)
        except hrequests.exceptions.ClientException as e:
            if pbar:
                pbar.write(
                    f"  Timed out while trying to collect {url}, {e}\n\nRetrying..."
                )
            try:
                webpage = hrequests.get(url, headers=self.headers, timeout=30)
            except hrequests.exceptions.ClientException as e:
                if pbar:
                    pbar.write(f"{e}")
                return False
        except hrequests.exceptions.BrowserTimeoutException:
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

        # Remove duplicates
        pdf_links = list(dict.fromkeys(pdf_links))

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
                    hrequests.get(
                        link, allow_redirects=True, headers=self.headers, timeout=30
                    ).content
                )
                if pbar:
                    pbar.set_description(f"  Downloaded {file_name}")
        except (
            hrequests.exceptions.BrowserTimeoutException
            or hrequests.exceptions.ClientException
        ):
            os.remove(file_name)
            if pbar:
                pbar.write(f"  {file_name} timed out")
            return False

        return True

    def get_report_metadata(
        self, report_id: str, soup: BeautifulSoup, pbar=None
    ) -> tuple[str, str, str, dict]:
        """
        Gets the investigation webpage and scrapes extra information about the report.

        Parameters
        ----------
        report_id : str
            The identifier of the report
        soup : BeautifulSoup
            The BeautifulSoup object for the page
        pbar : tqdm, optional
            The progress bar to update, by default None

        Returns
        -------
        tuple[str, str, str, dict]
            A tuple containing the report_id, title, event_type, misc
        """

    def __add_report_metadata_to_df(
        self, report_id: str, title: str, event_type: str, misc: dict
    ):
        if self.report_titles_df.query("report_id == @report_id").empty:
            self.report_titles_df.loc[len(self.report_titles_df)] = [
                report_id,
                title,
                event_type,
                misc,
            ]

            self.report_titles_df.to_pickle(self.settings.report_titles_file_path)


class TAICReportScraper(ReportScraper):
    def __init__(self, settings: ReportScraperSettings):
        super().__init__(settings)
        self.agency = "TAIC"

        self.agency_reports = self.__get_taic_investigations()

    def __get_taic_investigations(self):
        """TAICs websites provides an investigation table than can be easily read by pandas read_html"""
        pages = []
        page_num = 0
        while True:
            try:
                pages.append(
                    pd.read_html(
                        hrequests.get(
                            f"https://www.taic.org.nz/inquiries?page={page_num}",
                            headers=self.headers,
                        ).content,
                        flavor="lxml",
                    )[0]
                )
                page_num += 1
            except hrequests.exceptions.ClientException as e:
                print(f"Timeout while scraping TAIC investigations: {e}")
                print(f"Retrying page {page_num}")
            except ValueError:
                break

        investigations = pd.concat(pages, ignore_index=True)

        investigations.set_index(
            investigations["Number and name"].map(lambda x: Modes.Mode[x[0].lower()]),
            inplace=True,
        )

        investigations["year"] = investigations["Occurrence Date  Sort ascending"].map(
            lambda x: int(x[-4:])
        )

        investigations["id"] = investigations["Number and name"].map(
            lambda x: re.search(r"(?:[MAR]O-\d{4}-)\d{3}", x).group(0)
        )

        return investigations[["id", "year", "Status"]]

    def get_report_urls(self, mode, year):
        return [
            (
                self.get_report_id(mode, year, taic_id[-3:]),
                f"https://www.taic.org.nz/inquiry/{taic_id}",
            )
            for taic_id in self.agency_reports.loc[mode]
            .query(f"year == {year} & Status == 'Closed'")["id"]
            .to_list()
        ]

    def get_report_metadata(self, report_id: str, soup: BeautifulSoup, pbar=None):
        title = soup.find("div", class_="field--name-field-inv-title").text

        return report_id, title, None, {}


class ATSBReportScraper(ReportScraper):
    def __init__(
        self,
        settings: ReportScraperSettings,
        historic_aviation_investigations_path=None,
    ):
        super().__init__(settings)
        self.agency = "ATSB"
        self.agency_reports = self.__get_atsb_investigations(
            historic_aviation_investigations_path
        )

    def __get_atsb_investigations(self, historic_aviation_investigations_path=None):
        """ATSBs websites provides an investigation table than can be easily read by pandas read_html. The only catch is that the aviation goes all the way back to 1960s and so only the first few pages of the aviation table is scraped. It will then be combined with a complete scrape of the table to find the new ids."""
        if historic_aviation_investigations_path is None or not os.path.exists(
            historic_aviation_investigations_path
        ):
            historic_aviation_investigations = pd.DataFrame(
                columns=[
                    "Investigation title",
                    "Investigation number",
                    "Investigation webpage",
                    "Occurrence date",
                    "Report status",
                    "Report release",
                ]
            )
        else:
            historic_aviation_investigations = pd.read_pickle(
                historic_aviation_investigations_path
            )

        url_start_date = (
            f"field_occurence_date_value%5Bmin%5D={self.settings.start_year}-01-01"
        )
        url_end_date = (
            f"field_occurence_date_value%5Bmax%5D={self.settings.end_year}-12-31"
        )

        url = "https://www.atsb.gov.au/{mode}-investigation-reports?order=field_occurence_date&sort=desc&page={page_num}&field_investigation_type_target_id=162&field_report_status_target_id=93&{url_start_date}&{url_end_date}"

        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        dfs = []
        for mode in (
            pbar := tqdm(
                [Modes.Mode.as_string(mode).lower() for mode in self.settings.modes]
            )
        ):
            pages = []
            page_num = 0
            while True:
                pbar.set_description(f"Scraping mode: {mode}, page: {page_num}")
                try:
                    page = hrequests.get(
                        url.format(
                            mode=mode,
                            page_num=page_num,
                            url_start_date=url_start_date,
                            url_end_date=url_end_date,
                        ),
                        headers=self.headers,
                    ).content

                    page_df = pd.read_html(
                        page,
                        flavor="lxml",
                        extract_links="body",
                    )[0]

                    page_df["Investigation webpage"] = page_df[
                        "Investigation title"
                    ].apply(lambda x: urljoin(base_url, x[1]))

                    page_df = page_df.map(lambda x: x[0] if isinstance(x, tuple) else x)

                    page_df.rename(
                        {
                            "Occurrence date  Sort ascending": "Occurrence date",
                            "Occurrence date  Sort descending": "Occurrence date",
                        },
                        axis=1,
                        inplace=True,
                    )

                    if mode == "aviation":
                        # Check if any investigation number match.
                        new_investigations = page_df[
                            ~page_df["Investigation number"].isin(
                                historic_aviation_investigations["Investigation number"]
                            )
                        ]
                        if len(new_investigations) == 0:
                            break
                        else:
                            pages.append(new_investigations)
                    else:
                        pages.append(page_df)

                    page_num += 1
                except hrequests.exceptions.ClientException as e:
                    print(f"Timeout while trying to scrape {mode} page {page_num}: {e}")
                    print("Retrying...")
                    continue
                except (ValueError, TypeError) as e:
                    print(f"Failed to scrape {mode} page {page_num}: {e}")
                    print(f"Assuming end of {mode} reports")
                    break

            mode_investigations = pd.concat(
                pages
                + ([historic_aviation_investigations] if mode == "aviation" else []),
                ignore_index=True,
            )

            dfs.append(mode_investigations)

        df = pd.concat(dfs, axis=0, keys=self.settings.modes)

        df["year"] = pd.to_datetime(
            df["Occurrence date"].to_list(), format="%d/%m/%Y", errors="coerce"
        ).year

        df = df.query(f"year >= {self.settings.start_year}")

        df["id"] = df["Investigation number"].map(
            lambda x: re.search(r"(\d{3})$|(?:(?:\d{5})(\d{4}))$", str(x))
        )

        df = df.dropna(subset=["id"])
        df["id"] = df["id"].map(
            lambda x: x.group(1) if x.group(1) is not None else x.group(2)
        )

        return df

    def get_report_urls(self, mode, year):
        return [
            (self.get_report_id(mode, year, str(atsb_id)[-3:]), url)
            for atsb_id, url in self.agency_reports.loc[mode]
            .query(f"year == {year} & `Report status` == 'Final'")
            .dropna(subset=["Investigation webpage"])[
                [
                    "Investigation number",
                    "Investigation webpage",
                ]
            ]
            .to_records(index=False)
        ]

    def get_report_metadata(
        self, report_id: str, soup: BeautifulSoup, pbar=None
    ) -> tuple[str, str, str]:
        report_mode = Modes.get_report_mode_from_id(report_id)
        event_type = None
        if report_mode is Modes.Mode.a:
            event_type_div = soup.find(
                "div", class_="field--name-field-aviation-occurrence-type"
            )
            if event_type_div is not None:
                event_type = event_type_div.find("div", class_="field__item").text

        investigation_level = soup.find(
            "div", class_="field--name-field-investigation-level"
        )
        if investigation_level is not None:
            investigation_level = investigation_level.find(
                "div", class_="field__item"
            ).text

        title_div = soup.find("div", class_="field--name-title")

        title = title_div.text

        return (
            report_id,
            title,
            event_type,
            [{"investigation_level": investigation_level}],
        )


class TSBReportScraper(ReportScraper):
    def __init__(self, settings: ReportScraperSettings):
        super().__init__(settings)
        self.agency = "TSB"

        self.agency_reports = self.__get_tsb_investigations()

    def __get_tsb_investigations(self):
        """
        The TSB is very well setup and works friendly with the pandas read_html. Therefore I can just read all of the investigation tables from the TSB website and then have the exact IDs I need.
        """
        modes = ["aviation", "rail", "marine"]

        modes_df = [
            pd.read_html(
                hrequests.get(
                    f"https://www.tsb.gc.ca/eng/rapports-reports/{mode}/index.html",
                    headers=self.headers,
                ).content
            )[0]
            for mode in tqdm(modes)
        ]

        # Add dataframes togather with extra column identifier

        merged_modes_df = pd.concat(
            modes_df, keys=[Modes.Mode.a, Modes.Mode.r, Modes.Mode.m]
        )

        merged_modes_df["Occurrence date"] = pd.to_datetime(
            merged_modes_df["Occurrence date"], format="%Y-%m-%d", errors="coerce"
        )

        merged_modes_df["year"] = merged_modes_df["Occurrence date"].dt.year

        return merged_modes_df

    def get_report_urls(self, mode, year):
        return [
            (
                self.get_report_id(mode, year, tsb_id[-5:]),
                f"https://www.tsb.gc.ca/eng/rapports-reports/{Modes.Mode.as_string(mode)}/{year}/{tsb_id}/{tsb_id}.html".lower(),
            )
            for tsb_id in self.agency_reports.loc[mode]
            .query(f"year == {year} & `Investigation status` == 'Completed'")[
                "Investigation number"
            ]
            .to_list()
        ]

    def get_report_metadata(self, report_id: str, soup: BeautifulSoup, pbar=None):
        title_block = soup.find("div", class_="field--name-field-occurrence")
        if title_block is None:
            return report_id, None, None
        else:
            event_type = title_block.find("strong")
            if event_type:
                event_type = event_type.text

        legacy_text_div = title_block.find(
            "div", class_="field--name-field-occurrence-legacy-text"
        )
        paragraph_text = []
        if legacy_text_div:
            # Handle case where <p> tag is present
            paragraph = legacy_text_div.find("p")
            if paragraph:
                paragraph_text = [text for text in paragraph.stripped_strings]
            else:
                # Handle case where <p> tag is not present
                paragraph_text = [text for text in legacy_text_div.stripped_strings]

        date_div = title_block.find("div", class_="field--name-field-occurrence-date")
        date_text = ""
        if date_div:
            time_tag = date_div.find("time")
            if time_tag:
                date_text = time_tag.text

        all_text = ", ".join(paragraph_text + [date_text])

        return report_id, all_text, event_type, {}
