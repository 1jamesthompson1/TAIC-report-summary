This folder is intended to provide a spot to put data that is used by this engine that can't be retrieved any other way.

It was introduced when the recommendation extraction was replaced with a dataset provided by TAIC (https://github.com/1jamesthompson1/TAIC-report-summary/issues/130#issuecomment-2041618860).

Here is a list of the files with their purpose or a bit of description.

| file | description |
| --- | ---- |
| cleaned_TAIC_recommendations_2024_11_19.csv | This file initially comes from TAIC directly and is simply a dataset they update each year with the new recommendations manually. It was first introduced [here](https://github.com/1jamesthompson1/TAIC-report-summary/issues/130#issuecomment-2041618860). Its most recent processing has happened in the `categoriesing_recommendations.ipynb` notebook. This does two things. Cleans up the file and the column names etc. As well as adds in the official responses from the yearly reports to the minister. The reports are first mentioned [here](https://github.com/1jamesthompson1/TAIC-report-summary/commit/999d9a28f192313ef81dcbbb078302bca6320023).|
| event_types.csv | As event types have been added to the webviewer these need to be assigned to all of the reports. These event types are defined by TAIC and are used within the Hubstream ecosystem. This set is used by `ReportTypeAssignment.py`
| atsb_historic_aviation_investigations.csv | This file is to speed up the web scraping process. This is because there are about 350 pages to the ATSB aviation report table (https://www.atsb.gov.au/aviation-investigation-reports). Therefore I have done a complete run through and `WebsiteScraping.py` uses this file to not scrape more pages then it has too. Potentially this could be moved to the output file and each agency could maintain its own table within the output folder. Currently this is not done and is discussed here: https://github.com/1jamesthompson1/TAIC-report-summary/issues/259