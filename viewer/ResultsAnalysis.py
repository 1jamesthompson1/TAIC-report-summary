from engine import OpenAICaller

import yaml
import time

class ResultsAnalyzer:
    def __init__(self, results):
        self.results = results

    def run_analysis(self):
        self.analyze_safety_issues()
        self.analyze_safety_themes()

    def analyze_safety_themes(self):
        self.theme_weightings = self.results.loc[:, 'CompleteSafetyIssues':'PDF'].iloc[:, 1:-1]
    
    def analyze_safety_issues(self):
        all_safety_issues = self.results['CompleteSafetyIssues'].to_list()
        all_safety_issues = map(
            lambda x: "No safety issues" if not x else "\n".join(f"- {item}" for item in x),
            all_safety_issues
        )
        report_ids = self.results['ReportID'].to_list()

        safety_issues_str = "\n\n".join(
            map(
                lambda tup: f"{tup[0]}:\n" + tup[1],
                zip(report_ids, all_safety_issues),
            )
        )

        response = OpenAICaller.openAICaller.query(
            system="""
I want you to help me read a list of items and help summarize these into a single list.

The list you will be given will be inside triple quotes.

Your output needs to be in yaml format. Just output the yaml structure with no extra text (This means no ```yaml and ```). What your output entails will be described in the question.""",
            user = f"""
'''
{safety_issues_str}
'''

Question:
I have a list of safety issues found in each accident investigation report.

Can you please read all of these and respond with a list of all the unique safety issues identified. Note that each the same safety issue may be written in a slightly differnet way.

For each unique safety issue can you add what reports it is found in.

The format should look like

- description: "abc"
  reports:
    - 2019_201
    - etc
"""
        )

        try: 
            self.safety_issues = yaml.safe_load(response)
        except yaml.YAMLError as exc:
            print(response)
            print(exc)
            time.sleep(1)
            self.analyze_safety_issues()
