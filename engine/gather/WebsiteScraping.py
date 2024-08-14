import os

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from ..utils import Modes


class ReportScraping:
    """
    Class that will take the output templates and download all the reports from the TAIC website
    These reports can be found manually by going to https://www.taic.org.nz/inquiries
    """

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
        self.modes = modes
        self.refresh = refresh
        self.ignored_report_ids = ignored_report_ids
        if os.path.exists(self.report_titles_file_path):
            self.report_titles_df = pd.read_pickle(self.report_titles_file_path)
        else:
            self.report_titles_df = pd.DataFrame(columns=["report_id", "title"])

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
        print(f"  Output directory: {self.report_dir}")
        print(f"  File name template: {self.file_name_template}")
        print(f"  Start year: {self.start_year},  End year: {self.end_year}")
        print(f"  Max reports per year: {self.max_per_year}")
        print(f"  Modes: {self.modes}")
        print(f"  Ignoring report ids: {self.ignored_report_ids}")

        # Create a folder to store the downloaded PDFs
        if not os.path.exists(self.report_dir):
            os.makedirs(self.report_dir)

        # Loop through each mode
        for mode in self.modes:
            self.collect_mode(mode)

    def collect_mode(self, mode):
        print(f"======== Downloading reports for mode: {mode.name}==========")

        year_range = [year for year in range(self.start_year, self.end_year + 1)]

        for year in (pbar := tqdm(year_range)):
            pbar.set_description(
                f"Downloading reports for mode: {mode.name}, currently doing year: {year}"
            )

            self.collect_year(year, mode)

    def collect_year(self, year, mode):
        # Define the base URL and report ids and download all reports for the mode.
        mode_id_base = mode.value * 100 + 1
        id_range = ["{0:03d}".format(i) for i in range(mode_id_base, mode_id_base + 99)]

        base_url = "https://www.taic.org.nz/inquiry/{}o-{}-{}"
        number_for_year = 0
        for i in (inner_pbar := tqdm(id_range)):
            url = base_url.format(mode.name, year, i)
            report_id = f"{year}_{i}"

            if report_id in self.ignored_report_ids:
                continue

            outcome = self.collect_report(report_id, url, inner_pbar)

            if outcome == "End of reports for this year":
                break
            elif outcome:
                number_for_year += 1

            if number_for_year >= self.max_per_year:
                break

        self.report_titles_df.to_pickle(self.report_titles_file_path)

    def collect_report(self, report_id, url, pbar=None):
        file_name = os.path.join(
            self.report_dir,
            self.file_name_template.replace(r"{{report_id}}", report_id),
        )

        if not self.refresh and os.path.exists(file_name):
            if pbar:
                pbar.set_description(f"  {file_name} already exists, skipping download")
            return True
        webpage = requests.get(url)

        soup = BeautifulSoup(webpage.content, "html.parser")

        if soup.find("h1", text="Page not found"):
            return "End of reports for this year"

        outcome = self.download_report(report_id, file_name, soup, pbar)
        if not outcome:
            return False

        self.get_report_title(report_id, soup, pbar)

        return True

    def get_report_title(self, report_id: str, soup: BeautifulSoup, pbar=None):
        title = soup.find("div", class_="field--name-field-inv-title").text

        if self.report_titles_df.query("report_id == @report_id").empty:
            self.report_titles_df.loc[len(self.report_titles_df)] = [
                report_id,
                title,
            ]

        pass

    def download_report(
        self, report_id: str, file_name: str, soup: BeautifulSoup, pbar=None
    ):
        # Find all the links that end with .pdf and download them

        pdf_links = [
            a["href"]
            for a in soup.find_all("a", href=True)
            if a["href"].endswith(".pdf")
        ]

        if len(pdf_links) == 0:
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

        link = pdf_links[0]
        try:
            with open(file_name, "wb") as f:
                f.write(requests.get(link, allow_redirects=True, timeout=10).content)
                if pbar:
                    pbar.set_description(f"  Downloaded {file_name}")
        except requests.ReadTimeout:
            os.remove(file_name)
            if pbar:
                pbar.write(f"  {file_name} timed out")
            return False

        return True
