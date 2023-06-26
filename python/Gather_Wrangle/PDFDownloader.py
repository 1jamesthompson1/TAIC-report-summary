import os
import requests
from bs4 import BeautifulSoup

def downloadPDFs(download_dir, start_year, end_year, max_per_year):
    # Define the base URL and the range of years to scrape
    base_url = "https://www.taic.org.nz/inquiry/mo-{}-{}"
    year_range = [(year, i) for year in range(start_year, end_year) for i in range(200, 200+max_per_year)]

    # Create a folder to store the downloaded PDFs
    if not os.path.exists(download_dir):
        os.mkdir(download_dir)

    # Loop through each URL, find the links to the PDFs, and download them
    for year, i in year_range:
        url = base_url.format(year, i)
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        pdf_links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".pdf")]
        for link in pdf_links:
            file_name = os.path.join(download_dir, f"{year}_{i}.pdf")
            with open(file_name, "wb") as f:
                f.write(requests.get(link, allow_redirects=True).content)
            print(f"Downloaded {file_name}")