import pandas as pd
import os    
# import yaml

class Searcher:
    def __init__(self):
        self.input_dir = "output"
        # self.themes = yaml.safe_load(open(os.path.join(self.input_dir, "themes.yaml"), "r",  encoding='utf-8', errors='replace'))['']

    def search(self, query: str) -> pd.DataFrame:
        reports = []

        for dir in os.listdir(self.input_dir):
            report_dir = os.path.join(self.input_dir, dir)
            if not os.path.isdir(report_dir):
                continue
            number_of_files = len(os.listdir(report_dir))

            theme_summary_path = os.path.join(report_dir, f"{dir}_themes.txt")
            if os.path.exists(theme_summary_path):
                with open(theme_summary_path, "r",  encoding='utf-8', errors='replace') as f:
                    theme_summary = f.read()
            else:
                theme_summary = "Not found"

            report_text_path = os.path.join(report_dir, f"{dir}.txt")
            if os.path.exists(report_text_path):
                with open(report_text_path, "r",  encoding='utf-8', errors='replace') as f:
                    report_text = f.read()
            else:
                report_text = "Not found"

            if theme_summary == "Not found" and report_text == "Not found":
                continue
            
            if not self.search_report(report_text, theme_summary, query):
                continue

            
            reports.append({"ReportID": dir, "NoFiles": number_of_files, "Theme": theme_summary})

        return pd.DataFrame(reports)
    
    def search_report(self, report_text: str, theme_text: str, query: str) -> bool:
        return query in report_text or query in theme_text