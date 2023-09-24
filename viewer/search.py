import pandas as pd
import os
import re
from nltk.corpus import wordnet
import nltk
nltk.download('wordnet')

import engine.Config as Config
import engine.Extract_Analyze.Themes as Themes
from engine.Extract_Analyze.OutputFolderReader import OutputFolderReader

class Search:
    def __init__(self, query: str, settings: dict):
        self.query = query
        self.settings = settings

    def getQuery(self):
        return self.query
    
    def getSettings(self):
        return self.settings

class SearchResult:
    def __init__(self, report_matches: int, theme_matches: int, theme_text_matches: str):
        self.report_matches = report_matches
        self.theme_matches = theme_matches
        self.theme_text_matches = theme_text_matches

    def include(self):
        return self.matches() > 0
    
    def matches(self):
        return self.report_matches + self.theme_matches

class Searcher:
    def __init__(self):
        self.output_config = Config.configReader.get_config()['engine']['output']
        self.input_dir = os.path.join("viewer", self.output_config.get("folder_name"))
        self.themes = Themes.ThemeReader(self.input_dir).get_theme_titles()
        self.summary = pd.read_csv(os.path.join(self.input_dir, self.output_config.get("summary_file_name")))

    def search(self, query: str, settings) -> pd.DataFrame:
        reports = []

        if query == "":
            return None
            
        for dir in OutputFolderReader(self.input_dir)._get_report_ids():
            report_dir = os.path.join(self.input_dir, self.output_config.get("reports").get("folder_name").replace(r'{{report_id}}', dir))
            if not os.path.isdir(report_dir):
                continue

            theme_summary_path = os.path.join(report_dir, self.output_config.get("reports").get("themes_file_name").replace(r'{{report_id}}', dir))
            if os.path.exists(theme_summary_path):
                with open(theme_summary_path, "r",  encoding='utf-8', errors='replace') as f:
                    theme_summary = f.read()

                theme_summary = theme_summary.replace("\n", "<br>")
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
            
            search_result = self.search_report(report_text, theme_summary, Search(query, settings))
            if not search_result.include():
                continue

            report_row = {
                "ReportID": dir,
                "NoMatches": search_result.matches(),
                "ThemeSummary": search_result.theme_text_matches,
            }

            reportID_summary_row = self.summary.loc[self.summary["ReportID"] == dir]
            if len(reportID_summary_row) == 0:
                continue
            if settings['include_incomplete_reports'] == True:
                report_row['ErrorMessage'] = reportID_summary_row['ErrorMessage'].values[0]
            elif reportID_summary_row['Complete'].values[0] == False:
                continue

            for theme in self.themes:
                report_row[theme] = round(reportID_summary_row[theme].values[0], 6) if reportID_summary_row['Complete'].values[0] else "N/A"

            report_link = f"https://www.taic.org.nz/inquiry/mo-{dir.replace('_', '-')}"
            report_row["PDF"] = f'<a href="{report_link}" target="_blank">🌐</a>'

            reports.append(report_row)

        if len(reports) == 0:
            return None
        
        return pd.DataFrame(reports).sort_values(by=['NoMatches'], ascending=False)
    
    def search_report(self, report_text: str, theme_text: str, search: Search) -> SearchResult:
        if search.getSettings()['simple_search']:
            regex = re.compile(r'\b(' + search.getQuery().lower() + r')|(' + search.getQuery().lower() + r')\b')
        else:
            synonyms = [search.getQuery().lower()]

            for syn in wordnet.synsets(search.getQuery().lower()):
                for i in syn.lemmas():
                    synonyms.append(i.name())

            
            synonyms = set(
                map(lambda x: x.lower().replace("_", " "), synonyms)
            )

            pattern = '(' + '|'.join(['(' + syn + ')' for syn in synonyms]) + ')'

            regex = re.compile(r'(\b' + pattern + ')|(' + pattern + r'\b)')

        report_result = regex.findall(report_text.lower())
        theme_result = regex.findall(theme_text.lower())

        # highlight the theme_matches
        theme_text_highlighted = regex.sub(r'<span style="background-color: #FFFF00">\1</span>', theme_text.lower())

        return SearchResult(
            report_matches = len(report_result) if search.getSettings()['search_report_text'] else 0,
            theme_matches = len(theme_result) if search.getSettings()['search_theme_text'] else 0,
            theme_text_matches = theme_text_highlighted
        )
    


