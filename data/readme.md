This folder is intended to provide a spot to put data that is used by this engine that can't be retrieved any other way.

It was introduced when the recommendation extraction was replaced with a dataset provided by TAIC (https://github.com/1jamesthompson1/TAIC-report-summary/issues/130#issuecomment-2041618860).

Here is a list of the files with their purpose or a bit of description.

| file | description |
| --- | ---- |
| cleaned_TAIC_recommendations_2024_04_04.csv | This file initially comes from TAIC directly and is simply a dataset they update each year with the new recommendations manually. It was first introduced [here](https://github.com/1jamesthompson1/TAIC-report-summary/issues/130#issuecomment-2041618860). Its most recent processing has happened in the `categoriesing_recommendations.ipynb` notebook. This does two things. Cleans up the file and the column names etc. As well as adds in the official responses from the yearly reports to the minister. The reports are first mentioned [here](https://github.com/1jamesthompson1/TAIC-report-summary/commit/999d9a28f192313ef81dcbbb078302bca6320023).|