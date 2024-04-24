import pandas as pd
import os
import yaml
import re
from nltk.corpus import wordnet
import nltk
nltk.download('wordnet')

import engine.Config as Config
import engine.Modes as Modes
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
    def __init__(self, matches: {str: int}):
        self.matches = matches

    def include(self):
        # Include a report if it has at least one match for all search queries in atleast one of the three categories
        return any([all(value) for _, value in self.matches.items()])
    
    def num_matches(self):
        return sum([sum(self.matches[key]) for key in self.matches])

class Searcher:
    def __init__(self):
        self.config = Config.configReader.get_config()['engine']

        self.output_config = self.config['output']

        self.input_dir = os.path.join("viewer", self.output_config.get("folder_name"))
        self.themes = Themes.ThemeReader(self.input_dir).get_theme_titles()
        self.summary = pd.read_csv(os.path.join(self.input_dir, self.output_config.get("summary_file_name")))

        self.recommendations = pd.read_csv(os.path.join(self.input_dir, self.config.get('data').get("recommendations_file_name")))

    def search(self, query: str, settings, theme_ranges, theme_group_ranges, transport_modes, year_range) -> pd.DataFrame:
        reports = []

        for dir in OutputFolderReader(self.input_dir)._get_report_ids():

            # Check to see if the report is in the correct mode
            report_mode = Modes.get_report_mode_from_id(dir)
            if report_mode not in transport_modes:
                continue

            # Check to see if the report is in the correct year range
            report_year = int(dir.split("_")[0])
            if not year_range[0] <= report_year <= year_range[1]:
                continue
                    
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
            

            theme_summary_obj = self.read_theme_file(dir)

            reportID_summary_row = self.summary.loc[self.summary["ReportID"] == dir]
            if len(reportID_summary_row) == 0:
                continue

            if reportID_summary_row['Complete'].values[0] == False:
                continue

            report_weighting_reasoning = [reportID_summary_row[theme + "_explanation"].values[0] for theme in self.themes]
            

            search_result = self.search_report(report_text, theme_summary, report_weighting_reasoning, Search(query, settings))
            if not search_result.include() and not query == "":
                continue

            safety_issues_file = os.path.join(report_dir, self.output_config.get("reports").get("safety_issues").replace(r'{{report_id}}', dir))
            safety_issues = []
            if os.path.exists(safety_issues_file):
                with open(safety_issues_file, "r") as f:
                    safety_issues = yaml.safe_load(f)

            report_recommendations = self.get_recommendations(dir)

            report_row = {
                "ReportID": dir,
                "NoMatches": search_result.num_matches(),
                "ThemeSummary": "<br><br>".join([theme['name'] for theme in theme_summary_obj]) if theme_summary_obj is not None else "Could not be completed",
                "CompleteThemeSummary": theme_summary,
                "SafetyIssues": str(len(safety_issues)),
                "CompleteSafetyIssues": safety_issues,
                "Recommendations": str(len(report_recommendations)),
                "CompleteRecommendations": report_recommendations
            }

            inside_theme_range = True

            theme_weighting = {theme: reportID_summary_row[theme + "_weighting"].values[0] for theme in self.themes}

            for theme in self.themes:
                if not theme_ranges[theme][0] <= theme_weighting[theme] <= theme_ranges[theme][1]:
                    inside_theme_range = False
                    break

                report_row[theme] = round(theme_weighting[theme], 6) if reportID_summary_row['Complete'].values[0] else "N/A"
                report_row["Complete"+theme+"Reasoning"]  = reportID_summary_row[theme + "_explanation"].values[0]


            # Check the theme group ranges        
            theme_groups = Themes.ThemeReader(self.input_dir).get_groups()

            for group in theme_groups:
                min = theme_group_ranges[group['title']][0]
                max = theme_group_ranges[group['title']][1]

                any_in_range = any([min <= theme_weighting[theme] <= max for theme in group['themes']])

                if not any_in_range:
                    inside_theme_range = False
                    break

            if not inside_theme_range and not inside_theme_range:
                continue

            # Add other weighting
            report_row['Other'] = reportID_summary_row['Other_weighting'].values[0]

            report_link = f"https://www.taic.org.nz/inquiry/{Modes.Mode.as_char(Modes.get_report_mode_from_id(dir))}o-{dir.replace('_', '-')}"
            report_row["PDF"] = f'<a href="{report_link}" target="_blank">🌐</a>'
            if settings['include_incomplete_reports'] == True:
                report_row['ErrorMessage'] = reportID_summary_row['ErrorMessage'].values[0]

            reports.append(report_row)

        if len(reports) == 0:
            return None
        
        return pd.DataFrame(reports).sort_values(by=['NoMatches'], ascending=False)
    
    def search_report(self, report_text: str, theme_text: str, weighting_reasoning: str, search: Search) -> SearchResult:
        if search.getQuery() == "":
            return SearchResult({})
        
        regexes = Searcher.get_regex(search)
        all_weight_reasoning = ' '.join(filter(lambda x: isinstance(x, str), weighting_reasoning))

        matches = {
            'report_result': [],
            'theme_result': [],
            'weighting_reasoning_result': []
        }
        for regex in regexes:
            matches['report_result'].append(regex.findall(report_text))
            matches['theme_result'].append(regex.findall(theme_text))
            matches['weighting_reasoning_result'].append(regex.findall(all_weight_reasoning))
        
        # Remove thuings that should not have been searched
        if not search.getSettings()['search_report_text']:
            matches.pop('report_result')
        if not search.getSettings()['search_theme_text']:
            matches.pop('theme_result')
        if not search.getSettings()['search_weighting_reasoning']:
            matches.pop('weighting_reasoning_result')

        for key in matches:
            matches[key] = [len(regex_result) for regex_result in matches[key]]

        return SearchResult(
            matches
        )
    

    def get_regex(search: Search):

        if search.getQuery() == "" or search.getQuery() is None:
            return []
        
        split_queries = search.getQuery().split(" AND ")

        regexes = []

        for query in split_queries:

            if search.getSettings()['use_synonyms']:
                synonyms = [query.lower()]

                for syn in wordnet.synsets(query.lower()):
                    for i in syn.lemmas():
                        synonyms.append(i.name())

                
                synonyms = set(
                    map(lambda x: x.replace("_", " "), synonyms)
                )
            else:
                synonyms = [query]

            for i in range(len(synonyms)):
                # Exact
                synonyms[i] = re.sub(r'"(.*?)"', r'\\b(\1)\\b', synonyms[i])

                # Or
                synonyms[i] = synonyms[i].replace(' OR ', '|').replace(' | ', '|')

                # Exclusion
                synonyms[i] = re.sub(r'^-(\w+) ', r'(?<!\1 )', synonyms[i])
                synonyms[i] = re.sub(r' -(\w+)', r'(?! \1)', synonyms[i])

                # Wildcard
                synonyms[i] = synonyms[i].replace('*', '.*')


            regexes.append("|".join(synonyms))

        return [re.compile(regex, re.IGNORECASE) for regex in regexes]

    def get_highlighted_report_text(self, report_id, search_query, settings):
        report_dir = os.path.join(self.input_dir, self.output_config.get("reports").get("folder_name").replace(r'{{report_id}}', report_id))
        report_text_path = os.path.join(report_dir, self.output_config.get("reports").get("text_file_name").replace(r'{{report_id}}', report_id))
        
        if os.path.exists(report_text_path):
            with open(report_text_path, "r", encoding='utf-8', errors='replace') as f:
                report_text = f.read()

            # Highlight the matching text
            report_text = self.highlight_matches(
                report_text,
                Searcher.get_regex(Search(search_query, settings)))

            report_text = report_text.replace("\n", "<br>")

            return report_text

        return "Not found"

    # Add this function to highlight matches
    def highlight_matches(self, text, regexes):
        highlighted_text = text
        for regex in regexes:
            highlighted_text = regex.sub(r'<span class="match-highlight">\g<0></span>', highlighted_text)
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
    
    def get_safety_issues(self, report_id):
        report_dir = os.path.join(self.input_dir, self.output_config.get("reports").get("folder_name").replace(r'{{report_id}}', report_id))

        safety_issues_path = os.path.join(report_dir, self.output_config.get("reports").get("safety_issues").replace(r'{{report_id}}', report_id))

        if not os.path.exists(safety_issues_path):
            return "No safety issues found.<br><br>Not that some safety issues may of been missed when extracting. Furthermore older reports <2012 do not support safety issue extracting at all."
    
        with open(safety_issues_path, "r") as f:
            safety_issues = yaml.safe_load(f)
        
        return safety_issues
            
    def get_recommendations(self, report_id):
        reports_recommendations = self.recommendations.loc[self.recommendations["report_id"] == report_id]['recommendation'].to_list()
       
        return reports_recommendations

    def read_theme_file(self, report_id):
        report_dir = os.path.join(self.input_dir, self.output_config.get("reports").get("folder_name").replace(r'{{report_id}}', report_id))

        theme_summary_path = os.path.join(report_dir, self.output_config.get("reports").get("themes_file_name").replace(r'{{report_id}}', report_id))

        if os.path.exists(theme_summary_path):
            with open(theme_summary_path, "r", encoding='utf-8', errors='replace') as f:
                theme_summary_obj = yaml.safe_load(f)
            return theme_summary_obj
        
        return None
