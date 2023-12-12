import pandas as pd
import os
import yaml
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
        self.input_dir = os.path.join("viewer", self.output_config.get("folder_name"))
        self.themes = Themes.ThemeReader(self.input_dir).get_theme_titles()
        self.summary = pd.read_csv(os.path.join(self.input_dir, self.output_config.get("summary_file_name")))

    def search(self, query: str, settings, theme_ranges) -> pd.DataFrame:
        reports = []

        if query == "":
            return None
            
        for dir in OutputFolderReader(self.input_dir)._get_report_ids():
            report_dir = os.path.join(self.input_dir, self.output_config.get("reports").get("folder_name").replace(r'{{report_id}}', dir))
            if not os.path.isdir(report_dir):
                continue


            report_text_path = os.path.join(report_dir, self.output_config.get("reports").get("text_file_name").replace(r'{{report_id}}', dir))
            if os.path.exists(report_text_path):
                with open(report_text_path, "r",  encoding='utf-8', errors='replace') as f:
                    report_text = f.read()
                    
            else:
                report_text = "Not found"

            theme_summary = self.get_theme_text(dir)

            if theme_summary == "Not found" and report_text == "Not found":
                continue
            
            search_result = self.search_report(report_text, theme_summary, Search(query, settings))
            if not search_result.include():
                continue

            theme_summary_obj = self.read_theme_file(dir)

            report_row = {
                "ReportID": dir,
                "NoMatches": search_result.matches(),
                "ThemeSummary": "<br>".join([theme['name'] for theme in theme_summary_obj]) if theme_summary_obj is not None else "Could not be completed",
            }

            reportID_summary_row = self.summary.loc[self.summary["ReportID"] == dir]
            if len(reportID_summary_row) == 0:
                continue
            if settings['include_incomplete_reports'] == True:
                report_row['ErrorMessage'] = reportID_summary_row['ErrorMessage'].values[0]
            elif reportID_summary_row['Complete'].values[0] == False:
                continue

            inside_theme_range = True

            for theme in self.themes:
                if not theme_ranges[theme][0] <= reportID_summary_row[theme + "_weighting"].values[0] <= theme_ranges[theme][1]:
                    inside_theme_range = False
                    break

                report_row[theme] = round(reportID_summary_row[theme + "_weighting"].values[0], 6) if reportID_summary_row['Complete'].values[0] else "N/A"
            report_link = f"https://www.taic.org.nz/inquiry/mo-{dir.replace('_', '-')}"
            report_row["PDF"] = f'<a href="{report_link}" target="_blank">🌐</a>'

            if not inside_theme_range:
                continue

            reports.append(report_row)

        if len(reports) == 0:
            return None
        
        return pd.DataFrame(reports).sort_values(by=['NoMatches'], ascending=False)
    
    def search_report(self, report_text: str, theme_text: str, search: Search) -> SearchResult:
        regex = self.get_regex(search)
        
        report_result = regex.findall(report_text.lower())
        theme_result = regex.findall(theme_text.lower())

        return SearchResult(
            report_matches = len(report_result) if search.getSettings()['search_report_text'] else 0,
            theme_matches = len(theme_result) if search.getSettings()['search_theme_text'] else 0
        )
    
    def get_regex(self, search: Search):
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
        
        return regex

    def get_highlighted_report_text(self, report_id, search_query, settings):
        report_dir = os.path.join(self.input_dir, self.output_config.get("reports").get("folder_name").replace(r'{{report_id}}', report_id))
        report_text_path = os.path.join(report_dir, self.output_config.get("reports").get("text_file_name").replace(r'{{report_id}}', report_id))
        
        if os.path.exists(report_text_path):
            with open(report_text_path, "r", encoding='utf-8', errors='replace') as f:
                report_text = f.read()

            # Highlight the matching text
            report_text = self.highlight_matches(report_text, self.get_regex(Search(search_query, settings)))

            report_text = report_text.replace("\n", "<br>")

            return report_text

        return "Not found"

    # Add this function to highlight matches
    def highlight_matches(self, text, regex):
        highlighted_text = regex.sub(r'<span class="match-highlight">\1</span>', text)
        return highlighted_text
    
    def get_weighting_explanation(self, report_id, theme):
        # Search the csv file for the weighting explanation
        
        row = self.summary.loc[self.summary["ReportID"] == report_id]

        if len(row) == 0:
            return "Not found"
        
        return row[theme + "_explanation"].values[0]
    
    def get_theme_text(self, report_id):
        theme_summary_obj = self.read_theme_file(report_id)
        if theme_summary_obj is None:
            return "Not found"
        theme_summary = ""
        for theme in theme_summary_obj:
            theme_summary += f"<h4>{theme['name']}</h4>"
            theme_summary += theme['explanation']
            theme_summary += "<br>"

        theme_summary = theme_summary.replace("\n", "<br>")


        return theme_summary

    def read_theme_file(self, report_id):
        report_dir = os.path.join(self.input_dir, self.output_config.get("reports").get("folder_name").replace(r'{{report_id}}', report_id))

        theme_summary_path = os.path.join(report_dir, self.output_config.get("reports").get("themes_file_name").replace(r'{{report_id}}', report_id))

        if os.path.exists(theme_summary_path):
            with open(theme_summary_path, "r", encoding='utf-8', errors='replace') as f:
                theme_summary_obj = yaml.safe_load(f)
            return theme_summary_obj
        
        return None
