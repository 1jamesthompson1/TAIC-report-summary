from engine.OpenAICaller import openAICaller
from engine.Extract_Analyze.OutputFolderReader import OutputFolderReader

import pandas as pd
import yaml
import os
import io

class RecommendationSafetyIssueLinker:

    def __init__(self, output_folder, reports_config):
        self.output_folder = output_folder

        self.reports_config = reports_config
        pass

    def _link_recommendation_with_safety_issue(self, recommendation: str, safety_issue: str):
        response =openAICaller.query(
            system = f"""
            You are going to help me find find links between recommendations and safety issues identified in transport accident investigation reports.

            Each transport accident investigation report will identify safety issues. These reports will then issue recommendation that will address one or more of the safety issues identified in the report.

            For each pair given you need to respond with one of three answers.

            - None (The recommendation is not directly related to the safety issue)
            - Possible (The recommendation is reasonably likely to directly address the safety issue)
            - Confirmed (The recommendation explicitly mention that safety issue that it is trying address)
            """,
            user = f"""
            Here is the safety issue:

            {safety_issue}


            Here is the recommendation:

            {recommendation}

            Now can you please respond with one of three options

            - None (The recommendation is not directly related to the safety issue)
            - Possible (The recommendation is reasonably likely to directly address the safety issue)
            - Confirmed (The recommendation explicitly mention that safety issue that it is trying address)
            """,
            model = "gpt-4",
            temp = 0)
        
        if response in ['None', 'Possible', 'Confirmed']:
            return response
        
        print(f"Model response is incorrect and is {response}")
        return 'undetermined'
    
    def _evaluate_all_possible_links(self, report_id, recommendations, safety_issues):

        print(" Evaluating links for report " + report_id)
        
        recommendations = pd.read_csv(io.StringIO(recommendations))
        safety_issues = pd.DataFrame(yaml.safe_load(safety_issues))

        combined_df = pd.merge(recommendations, safety_issues, how='cross')

        combined_df['link'] = combined_df.apply(lambda x: self._link_recommendation_with_safety_issue(x['recommendation'], x['safety_issue']), axis=1)

        combined_df.to_csv(
            os.path.join(self.output_folder,
                         self.reports_config.get("folder_name").replace(r'{{report_id}}', report_id),
                         self.reports_config.get("recommendation_safety_issue_links_file_name").replace(r'{{report_id}}', report_id)), index=False)
        

    def evaluate_links_for_report(self):
        
        print("  Linking recommendations with safety issues")

        output_folder_reader = OutputFolderReader()

        output_folder_reader.process_reports_with_specific_files(self._evaluate_all_possible_links, ["recommendations_file_name", "safety_issues"])