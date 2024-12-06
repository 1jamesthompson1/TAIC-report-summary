import os
import re
import time
from datetime import datetime
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


class WebsiteScraper:
    def __init__(self, report_titles_file_path):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36 Edg/94.0.992.50",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Connection": "keep-alive",
        }

        if os.path.exists(report_titles_file_path):
            report_titles_df = pd.read_pickle(report_titles_file_path)
        else:
            raise ValueError(f"{report_titles_file_path} does not exist")
        self.id_dict = {
            agency: {
                agency_id: report_id
                for report_id, agency_id in ids[["report_id", "agency_id"]].values
            }
            for agency, ids in report_titles_df.assign(
                agency=report_titles_df["report_id"].map(lambda x: x.split("_")[0])
            ).groupby("agency")
        }

    def id_converter(self, agency, agency_id):
        """
        This uses the COMPLETE report titles dataframe and creates a dictionary of agency_id:report_id.
        The agency argument is needed as some of the agency ids are not globally unique but instead just unique within an agency.
        """
        if agency not in self.id_dict:
            raise ValueError(f"{agency} is not a valid agency")
        else:
            return self.id_dict[agency].get(agency_id)


class ReportScraper(WebsiteScraper):
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
                columns=[
                    "report_id",
                    "title",
                    "event_type",
                    "investigation_type",
                    "misc",
                    "url",
                    "agency_id",
                ]
            )
        super().__init__(self.settings.report_titles_file_path)

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

        if not self.settings.refresh and (
            os.path.exists(file_name)
            or self.report_titles_df.query(f"report_id == '{report_id}'").shape[0] > 0
        ):
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

        outcome = False
        if not os.path.exists(file_name):
            outcome = self.download_report(report_id, file_name, soup, base_url, pbar)

        if self.report_titles_df.query("report_id == @report_id").empty:
            report_id, title, event_type, investigation_type, agency_id, misc = (
                self.get_report_metadata(report_id, soup, pbar)
            )
            self.__add_report_metadata_to_df(
                report_id,
                title,
                event_type,
                investigation_type,
                misc,
                report_url=url,
                agency_id=agency_id,
            )

        return outcome

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
            hrequests.exceptions.BrowserTimeoutException,
            hrequests.exceptions.ClientException,
        ):
            os.remove(file_name)
            if pbar:
                pbar.write(f"  {file_name} timed out")
            return False

        return True

    def get_report_metadata(
        self, report_id: str, soup: BeautifulSoup, pbar=None
    ) -> tuple[str, str, str, str, dict]:
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
            A tuple containing the report_id, title, event_type, investigation_type,misc
        """

    def __add_report_metadata_to_df(
        self,
        report_id: str,
        title: str,
        event_type: str,
        investigation_type,
        misc: dict,
        report_url,
        agency_id,
    ):
        if self.report_titles_df.query("report_id == @report_id").empty:
            self.report_titles_df.loc[len(self.report_titles_df)] = [
                report_id,
                title,
                event_type,
                investigation_type,
                misc,
                report_url,
                agency_id,
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

        investigations["year"] = investigations["Number and name"].map(
            lambda x: int(re.search(r"-(\d{4})-", x).group(1))
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

        agency_id = soup.find("h1", class_="page-title").get_text().strip()

        return report_id, title, None, "full", agency_id, {}


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
    ) -> tuple[str, str, str, dict]:
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

        agency_id = soup.find("div", class_="field--name-field-report-id")
        if agency_id is not None:
            agency_id = agency_id.find("div", class_="field__item").text.strip()

        return (
            report_id,
            title,
            event_type,
            "unknown"
            if investigation_level is None
            else "full"
            if investigation_level in ["Defined", "Systemic"]
            else "short",
            agency_id,
            {"investigation_level": investigation_level},
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
        # Due to TSB having the metadata on a page separate from the report pdf link, we need to get the new page
        split_id = report_id.split("_")
        tsb_id = f"{split_id[1]}{split_id[2][2:4]}{split_id[3]}"
        page = hrequests.get(
            f"https://www.tsb.gc.ca/eng/enquetes-investigations/{Modes.Mode.as_string(Modes.get_report_mode_from_id(report_id))}/{split_id[2]}/{tsb_id}/{tsb_id}.html",
            headers=self.headers,
            timeout=30,
        )
        overview_page = BeautifulSoup(page.content, "html.parser")

        if (
            overview_page.find("h1", string="Page not found") is None
        ):  # Some of the older reports dont have the overview page
            soup = overview_page

        title_block = soup.find("div", class_="field--name-field-occurrence")
        if title_block is None:
            return report_id, None, None, None, None
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

        # Get investigation level
        investigation_level = None

        h3_element = soup.find("h3", string="Class of investigation")
        if h3_element:
            text = h3_element.find_next_sibling("p").text
            match = re.match(r"This is a class (\d) investigation", text)
            if match:
                investigation_level = match.group(1)

        agency_id = soup.find("h1", class_="page-header").text
        if agency_id is not None:
            agency_id = agency_id.strip().split(" ")[-1]

        return (
            report_id,
            all_text,
            event_type,
            "unknown"
            if investigation_level is None
            else "full"
            if investigation_level in ["1", "2", "3"]
            else "short",
            agency_id,
            {"investigation_class": investigation_level},
        )


class ATSBSafetyIssueScraper(WebsiteScraper):
    def __init__(
        self, output_file_path: str, report_titles_file_path, refresh: bool = False
    ):
        super().__init__(report_titles_file_path)
        self.output_file_path = output_file_path
        self.refresh = refresh

    def extract_safety_issues_from_website(self):
        if os.path.exists(self.output_file_path) and not self.refresh:
            safety_issues_df = pd.read_pickle(self.output_file_path)
        else:
            safety_issues_df = pd.DataFrame(columns=["report_id", "safety_issues"])

        base_url = "https://www.atsb.gov.au/safety-issues-and-actions?field_issue_number_value={mode}O&page={page}"
        if len(safety_issues_df["safety_issues"]) > 0:
            widened_safety_issues_df = pd.concat(
                safety_issues_df["safety_issues"].tolist()
            )
        else:
            widened_safety_issues_df = pd.DataFrame(
                columns=["safety_issue_id", "safety_issue", "quality"]
            )

        print(
            "-------------------------- Scraping safety issues from ATSB ----------------------"
        )
        print(f"     Output file: {self.output_file_path}")
        print(f"     Currently have {widened_safety_issues_df.shape[0]} safety issues")
        print(f"     Spread across {len(safety_issues_df)} reports")
        print(
            "----------------------------------------------------------------------------------"
        )

        for mode in (pbar := tqdm(["A", "R", "M"])):
            current_page = 0
            pbar.set_description(f"Scraping {mode} safety issues")

            failed = 0
            while True:
                pbar.set_description(f"Scraping page {current_page} of mode {mode}")
                url = base_url.format(mode=mode, page=current_page)
                response = hrequests.get(url, headers=self.headers)

                if response.status_code != 200:
                    pbar.write(
                        f"Failed to scrape page {current_page} of mode {mode}\nWith error {response.status_code}"
                    )
                    failed += 1
                    if failed > 5:
                        failed = 0
                        current_page += 1
                    continue

                soup = BeautifulSoup(response.content, "html.parser")

                if not soup.find("div", class_="view-content"):
                    pbar.write(
                        f"Failed to scrape page {current_page} of mode {mode}. No table found"
                    )
                    break

                safety_issues = [
                    {
                        field.find(class_="field__label").get_text(
                            strip=True
                        ): field.find(class_="field__item").get_text(strip=True)
                        for field in row.find_all(class_="field--label-inline")
                    }
                    for row in soup.find("div", class_="view-content").children
                    if not isinstance(row, str)
                ]
                table = pd.DataFrame(safety_issues)

                if "Safety issue title" not in table.columns:
                    pbar.write(
                        f"Failed to scrape page {current_page} of mode {mode}. Page contains no safety issues"
                    )
                    break

                table["safety_issue"] = table.apply(
                    lambda row: f"{row['Safety issue title']}\n{row['Safety Issue Description']}",
                    axis=1,
                )

                table["safety_issue_id"] = table["Issue number"].map(
                    lambda number: f"ATSB_{number}"
                )

                new_safety_issues = table[
                    ~table["safety_issue_id"].isin(
                        widened_safety_issues_df["safety_issue_id"]
                    )
                ]

                if new_safety_issues.empty:
                    pbar.write(
                        f"No new safety issues found on page {current_page} of mode {mode}, moving onto next mode"
                    )
                    break

                widened_safety_issues_df = pd.concat(
                    [widened_safety_issues_df, new_safety_issues], ignore_index=True
                )

                current_page += 1

        widened_safety_issues_df = widened_safety_issues_df.drop_duplicates(
            subset=["safety_issue_id"]
        )

        widened_safety_issues_df["report_id"] = (
            widened_safety_issues_df["safety_issue_id"]
            .map(lambda x: "-".join(x.split("_")[1].split("-")[0:3]))
            .map(
                lambda x: self.id_converter("ATSB", x)
                if self.id_converter("ATSB", x)
                else f"Unmatched ({x})"
            )
        )
        widened_safety_issues_df["quality"] = "exact"
        widened_safety_issues_df = widened_safety_issues_df[
            ["report_id", "safety_issue_id", "safety_issue", "quality"]
        ]

        print(f"  Now there are {widened_safety_issues_df.shape[0]} safety issues")

        grouped = widened_safety_issues_df.groupby("report_id")
        formatted_df = pd.DataFrame(
            {
                "report_id": grouped.groups.keys(),
                "safety_issues": [group.reset_index(drop=True) for _, group in grouped],
            }
        )

        print(f"  Spread across {formatted_df.shape[0]} reports")

        formatted_df.to_pickle(self.output_file_path)


class RecommendationScraper(WebsiteScraper):
    def __init__(self, output_file_path, report_titles_file_path, refresh=False):
        super().__init__(report_titles_file_path)

        self.output_file_path = output_file_path

        self.refresh = refresh

    def extract_recommendations_from_website(self):
        if not self.refresh and os.path.exists(self.output_file_path):
            recommendations_df = pd.read_pickle(self.output_file_path)
        else:
            recommendations_df = pd.DataFrame(columns=["report_id", "recommendations"])

        new_recommendations = pd.DataFrame(columns=self.columns)

        print(
            "------------------------ Scraping recommendations ------------------------"
        )
        print(f"    Output directory: {self.output_file_path}")
        print(f"    Scraping from base url {self.base_url}")
        print(
            f"    Currently have {recommendations_df.shape[0]} reports with recommendations"
        )
        print(
            f"    With {recommendations_df['recommendations'].apply(len).sum()} recommendations"
        )
        print(
            "----------------------------------------------------------------------------------"
        )

        print("  Reading recommendation tables to get recommendations webpages")

        for element in (phbar := tqdm(self.loop_iter)):
            phbar.set_description(f"Scraping recommendations for {element}")

            url = self.get_url(element)

            response = hrequests.get(url, headers=self.headers)

            if response.status_code != 200:
                raise ValueError(
                    f"Failed to scrape recommendations for {element}. Error code {response.status_code}"
                )

            table = pd.read_html(response.content, flavor="lxml", extract_links="body")

            if len(table) == 0 or table[0].empty:
                break
            table = table[0]

            table = self.process_new_table(table)

            if len(recommendations_df) > 0:
                table = table[
                    ~table["recommendation_id"].isin(
                        pd.concat(recommendations_df["recommendations"].tolist())[
                            "recommendation_id"
                        ]
                    )
                ]

            if len(table) == 0:
                break
            new_recommendations = pd.concat(
                [new_recommendations, table], ignore_index=True
            )

        print(
            f"  Found {new_recommendations.shape[0]} new recommendations, reading each individual webpage now"
        )

        for i, row in (phbar := tqdm(list(new_recommendations.iterrows()))):
            phbar.set_description(
                f"Processing recommendation {row['recommendation_id']} from {row['agency_id']} with i:{i}"
            )
            recommendation_data = self.extract_recommendation_data(row["url"])
            for key, value in recommendation_data.items():
                new_recommendations.at[i, key] = value

        new_recommendations["report_id"] = new_recommendations["agency_id"].map(
            lambda x: self.id_converter(self.agency, x)
            if self.id_converter(self.agency, x)
            else f"Unmatched {self.agency} ({x})"
        )

        recommendations_df = pd.concat(
            [
                recommendations_df,
                pd.DataFrame(
                    {
                        "report_id": new_recommendations.groupby(
                            "report_id"
                        ).groups.keys(),
                        "recommendations": [
                            group.reset_index(drop=True).drop("report_id", axis=1)
                            for _, group in new_recommendations.groupby("report_id")
                        ],
                    }
                ),
            ],
            ignore_index=True,
        )

        recommendations_df.to_pickle(self.output_file_path)

    def extract_recommendation_data(self, url):
        """
        Goes to the URL and extracts the needed data.
        """
        raise NotImplementedError

    def process_new_table(self, table):
        """
        This takes a recently read table and processes it
        """
        raise NotImplementedError


class TSBRecommendationsScraper(RecommendationScraper):
    def __init__(self, output_file_path, report_titles_file_path, refresh=False):
        super().__init__(output_file_path, report_titles_file_path, refresh)
        self.columns = [
            "recommendation_id",
            "recommendation",
            "agency_id",
            "current_assessment",
            "status",
            "watchlist",
            "url",
            "made",
            "recommendation_context",
        ]
        self.base_url = "https://www.tsb.gc.ca"

        self.loop_iter = ["rail", "marine", "aviation"]

        self.agency = "TSB"

    def get_url(self, mode):
        return f"{self.base_url}/eng/recommandations-recommendations/{mode}/index.html"

    def extract_recommendation_data(self, url, retry=3):
        """
        This will read the webpage and extract:
        - recommendation (This is because sometimes the recommendation inside the website table is not complete)
        - recommednation date
        - recommendation context
        ## TODO: Add in the recipient and reply text. This is not done at the moment as it is not needed
        - recipient
        - reply text
        """
        if url is None:
            return {
                "recommendation": None,
                "made": None,
                "recommendation_context": None,
            }

        response = hrequests.get(url, headers=self.headers)

        if response.status_code != 200:
            if retry > 0:
                time.sleep(1)
                return self.extract_recommendation_data(url, retry - 1)
            raise ValueError(
                f"Failed to scrape recommendations from {url}. Error code {response.status_code}"
            )

        soup = BeautifulSoup(response.content, "html.parser")

        recommendation = soup.find(
            "div", class_="field--name-field-recommendation-well"
        )
        recommendation = recommendation.get_text() if recommendation else None

        recommendation_date = soup.find(
            "div", class_="field--name-field-recommendation-issued"
        )
        if recommendation_date is not None:
            recommendation_date = recommendation_date.find("time")["datetime"]
            recommendation_date = datetime.fromisoformat(
                recommendation_date.replace("Z", "+00:00")
            )

        context = soup.find("div", class_="field--name-field-recommendation-rationale")
        recommendation_context = None
        if context is not None:
            recommendation_context = "\n".join(
                [child.get_text() for child in context.children if child.name == "p"]
            )

        return {
            "recommendation": recommendation,
            "made": recommendation_date,
            "recommendation_context": recommendation_context,
        }

    def process_new_table(self, table):
        # filter out older recommendations
        table = table[
            table["Number"]
            .map(
                lambda x: int(
                    re.match(r"[amr](\d{2})-\d{2}", x[0], re.IGNORECASE).group(1)
                )
            )
            .between(0, 80)
        ]

        table["url"] = table["Number"].map(
            lambda x: f"{self.base_url}{x[1]}" if x[1] else None
        )

        table = table.map(lambda x: x[0] if isinstance(x, tuple) else x)

        table.columns = self.columns[:7]
        table.drop("recommendation", axis=1, inplace=True)
        return table


class TAICRecommendationsScraper(RecommendationScraper):
    def __init__(self, output_file_path, report_titles_file_path, refresh=False):
        super().__init__(output_file_path, report_titles_file_path, refresh)

        self.columns = [
            "recommendation_id",
            "made",
            "agency_id",
            "recipient",
            "recommendation",
            "reply_text",
        ]
        self.base_url = "https://www.taic.org.nz"

        self.loop_iter = range(300)

        self.agency = "TAIC"

    def get_url(self, page):
        return f"{self.base_url}/recommendations?page={page}"

    def process_new_table(self, table):
        table = table.iloc[:, :4]

        table.columns = self.columns[:4]
        table["url"] = table["recommendation_id"].map(lambda x: self.base_url + x[1])

        table = table.map(lambda x: x[0] if isinstance(x, tuple) else x)

        table["recommendation_id"] = table["recommendation_id"].map(
            lambda x: re.sub(" (Aviation)|(Rail)|(Marine)", "", x)
        )
        table = table[
            table["recommendation_id"]
            .map(
                lambda x: int(re.match(r"\d{3}\w?/(\d{2})", x, re.IGNORECASE).group(1))
            )
            .between(0, 80)
        ]

        return table

    def extract_recommendation_data(self, url):
        """
        This will extract information that is not found in the able
        - recommendation_text
        - reply_text
        """

        if url is None:
            return None, None

        response = hrequests.get(url, headers=self.headers)

        if response.status_code != 200:
            retry = 3
            if retry > 0:
                time.sleep(1)
                return self.extract_recommendation_data(url, retry - 1)
            raise ValueError(
                f"Failed to scrape recommendations from {url}. Error code {response.status_code}"
            )

        soup = BeautifulSoup(response.content, "html.parser")

        text = soup.find("div", class_="field--name-field-sr-text")

        recommendation_text = (
            text.find("div", class_="field__item").get_text() if text else None
        )

        reply_text = soup.find("div", class_="field--name-field-sr-replytext")
        reply_text = (
            reply_text.find("div", class_="field__item").get_text()
            if reply_text
            else None
        )

        return {
            "recommendation": recommendation_text,
            "reply_text": reply_text,
        }
