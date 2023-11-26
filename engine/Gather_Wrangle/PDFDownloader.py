import os
import requests
from bs4 import BeautifulSoup
from enum import Enum

class Mode(Enum):
    a = 0 # Aviation
    r = 1 # Rail
    m = 2 # Marine

class ReportDownloader:
    """
    Class that will take the output templates and download all the reports from the TAIC website
    These reports can be found manually by going to https://www.taic.org.nz/inquiries
    """
    def __init__(self, output_dir, report_dir_template, file_name_template, start_year, end_year, max_per_year, modes: list[str]):
        self.output_dir = output_dir
        self.report_dir_template = report_dir_template
        self.file_name_template = file_name_template
        self.start_year = start_year
        self.end_year = end_year
        self.max_per_year = max_per_year
        self.modes = [Mode[mode] for mode in modes]

    def download_all(self):
        print("Downloading reports from TAIC website with config: ")
        print(f"  Output directory: {self.output_dir}")
        print(f"  Report directory template: {self.report_dir_template}")
        print(f"  File name template: {self.file_name_template}")
        print(f"  Start year: {self.start_year},  End year: {self.end_year}")
        print(f"  Max reports per year: {self.max_per_year}")
        print(f"  Modes: {self.modes}")

        # Create a folder to store the downloaded PDFs
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)

        # Loop through each mode
        for mode in self.modes:
            print(f"Downloading reports for mode: {mode.name}")
            
            # Define the base URL and report ids and download all reports for the mode.
            mode_id_base = mode.value * 100
            year_range = [(year, "{0:03d}".format(i)) for year in range(self.start_year, self.end_year) for i in range(mode_id_base, mode_id_base+self.max_per_year)]

            base_url = "https://www.taic.org.nz/inquiry/{}o-{}-{}"
            for year, i in year_range:
                url = base_url.format(mode.name, year, i)
                report_id = f"{year}_{i}"
                self.download_report(report_id, url)

                
    def download_report(self, report_id, url):
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        # Find all the links that end with .pdf and download them       

        pdf_links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".pdf")]

        if (len(pdf_links) == 0):
            print(f"  No PDFs found for {url} assuming report {report_id} does not exist")
            return
        if (len(pdf_links) > 1):
            print(f"WARNING: Found more than one PDF for {report_id} at {url}. Will not download any")
            return

        link = pdf_links[0]

        report_dir = os.path.join(self.output_dir, self.report_dir_template.replace(r"{{report_id}}", report_id))
        
        if not os.path.exists(report_dir):
            os.mkdir(report_dir)

        file_name = os.path.join(report_dir, self.file_name_template.replace(r"{{report_id}}", report_id))
        if not os.path.exists(file_name):
            with open(file_name, "wb") as f:
                f.write(requests.get(link, allow_redirects=True).content)                
                print(f"  Downloaded {file_name}")
        else:
            print(f"  {file_name} already exists, skipping download")