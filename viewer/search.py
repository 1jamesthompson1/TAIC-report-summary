import pandas as pd
import os
import re
from nltk.corpus import wordnet

import engine.Config as Config
import engine.Extract_Analyze.Themes as Themes
from engine.Extract_Analyze.OutputFolderReader import OutputFolderReader

class Search:
    def __init__(self, query: str):
        self.query = query

    def getQuery(self):
        return self.query

class SearchResult:
    def __init__(self, report_matches: int, theme_matches: int):
        self.report_matches = report_matches
        self.theme_matches = theme_matches

    def include(self):
        return self.matches() > 0
    
    def matches(self):
        return self.report_matches + self.theme_matches

class Searcher:
    def __init__(self):
        self.output_config = Config.configReader.get_config()['engine']['output']
        self.input_dir = self.output_config.get("folder_name")
        self.themes = Themes.ThemeReader().get_theme_titles()
        self.summary = pd.read_csv(os.path.join(self.input_dir, self.output_config.get("summary_file_name")))

    def search(self, query: str) -> pd.DataFrame:
        reports = []

        if query == "":
            return pd.DataFrame(reports)
            
        for dir in OutputFolderReader()._get_report_ids():
            report_dir = os.path.join(self.input_dir, self.output_config.get("reports").get("folder_name").replace(r'{{report_id}}', dir))
            if not os.path.isdir(report_dir):
                continue

            theme_summary_path = os.path.join(report_dir, self.output_config.get("reports").get("themes_file_name").replace(r'{{report_id}}', dir))
            if os.path.exists(theme_summary_path):
                with open(theme_summary_path, "r",  encoding='utf-8', errors='replace') as f:
                    theme_summary = f.read()
            else:
                theme_summary = "Not found"

            report_text_path = os.path.join(report_dir, self.output_config.get("reports").get("text_file_name").replace(r'{{report_id}}', dir))
            if os.path.exists(report_text_path):
                with open(report_text_path, "r",  encoding='utf-8', errors='replace') as f:
                    report_text = f.read()
            else:
                report_text = "Not found"

            if theme_summary == "Not found" and report_text == "Not found":
                continue
            
            search_result = self.search_report(report_text, theme_summary, Search(query))
            if not search_result.include():
                continue

            report_row = {
                "ReportID": dir,
                "NoMatches": search_result.matches(),
                "ThemeSummary": theme_summary,
            }

            reportID_summary_row = self.summary.loc[self.summary["ReportID"] == dir]
            if len(reportID_summary_row) == 0:
                continue
            for theme in self.themes:
                report_row[theme] = round(reportID_summary_row[theme].values[0], 6)

            report_link = f"https://www.taic.org.nz/inquiry/mo-{dir.replace('_', '-')}"
            report_row["PDF"] = f'<a href="{report_link}" target="_blank">🌐</a>'

            reports.append(report_row)
        
        return pd.DataFrame(reports).sort_values(by=['NoMatches'], ascending=False)
    
    def search_report(self, report_text: str, theme_text: str, search: Search) -> SearchResult:

        synonyms = [search.getQuery().lower()]

        for syn in wordnet.synsets(search.getQuery().lower()):
            for i in syn.lemmas():
                synonyms.append(i.name())

        
        synonyms = set(
            map(lambda x: x.lower().replace("_", " "), synonyms)
        )

        pattern = '|'.join(['(' + syn + ')' for syn in synonyms])

        regex = re.compile(pattern)

        print(pattern)

        report_result = regex.findall(report_text.lower())
        theme_result = regex.findall(theme_text.lower())

        return SearchResult(
            report_matches = len(report_result),
            theme_matches = len(theme_result)
        )
    


