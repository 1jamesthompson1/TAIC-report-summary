from engine.OpenAICaller import openAICaller
from engine.Extract_Analyze.OutputFolderReader import OutputFolderReader

import pandas as pd
import yaml
import os
import io
import matplotlib.pyplot as plt
import networkx as nx
import textwrap

class RecommendationSafetyIssueLinker:

    def __init__(self, output_folder, reports_config):
        self.output_folder = output_folder

        self.reports_config = reports_config
        pass

    def _generate_visualization_of_links(self, df, report_id, output_path):
        '''
        Create a picture that shows both the recommendations and the safety issues fro a report. There will be arrows showing the link between the two.
        '''
        # Create a directed graph
        print("Creating visualization")
        G = nx.DiGraph()

        NODE_WIDTH = 50

        # Preemptively wrap the text
        df['recommendation'] = df['recommendation'].apply(lambda x: "\n".join(textwrap.wrap(x, width=NODE_WIDTH)))
        df['safety_issue'] = df['safety_issue'].apply(lambda x: "\n".join(textwrap.wrap(x, width=NODE_WIDTH)))

        # Add nodes for the 'extracted' and 'inferred' indices
        for i, text in enumerate(df['recommendation'].unique()):
            G.add_node(text, pos=(0, i))

        for i, issue in enumerate(df['safety_issue'].unique()):
            G.add_node(issue, pos=(3, i))


        # Add edges between the matched indices
        for _, row in df.iterrows():
            # Add solid arrow
            if row['link'] == 'Confirmed':
                G.add_edge(row['recommendation'], row['safety_issue'], color='red', style='solid', alpha =1)

            # Add dotted arrow
            elif row['link'] == 'Possible':
                G.add_edge(row['recommendation'], row['safety_issue'], color='blue', style='dashed', alpha = 0.5)


        max_num_of_nodes_column = max(
            len(df['recommendation'].unique()),
            len(df['safety_issue'].unique())
            )

        # Draw the graph
        pos = nx.get_node_attributes(G, 'pos')

        plt.figure(figsize=(10, max_num_of_nodes_column * 5))  
        nx.draw_networkx_nodes(G, pos, node_color='lightblue', node_size=[len(node) * NODE_WIDTH for node in G.nodes()])
        nx.draw_networkx_labels(G, pos, font_size=7)
        nx.draw_networkx_edges(G, pos, arrows=True, edge_color=nx.get_edge_attributes(G, 'color').values(), style=list(nx.get_edge_attributes(G, 'style').values()), alpha = list(nx.get_edge_attributes(G, 'alpha').values()))

        plt.xlim(-1, 4.5)  # Add buffer to the outside side edges
        plt.ylim(-1, max_num_of_nodes_column*1)  # Add buffer to the outside top and bottom edges

        # Add report ID and headers
        plt.title(f'Report ID: {report_id}')
        plt.text(0, max_num_of_nodes_column-0.2, 'Recommendations', fontsize=12, ha='center')
        plt.text(3, max_num_of_nodes_column-0.2, 'Safety issue', fontsize=12, ha='center')
        plt.text(1.5, max_num_of_nodes_column-0.5, 'Arrow indicates that the recommendation is indicated to solve the safety issue by the LLM', fontsize=10, ha='center')


        plt.savefig(output_path)
        plt.close()

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
        
        # Check for previous links
        

        links_csv_path = os.path.join(self.output_folder,
                         self.reports_config.get("folder_name").replace(r'{{report_id}}', report_id),
                         self.reports_config.get("recommendation_safety_issue_links_file_name").replace(r'{{report_id}}', report_id))

        if os.path.exists(links_csv_path):
            combined_df = pd.read_csv(links_csv_path)
        else:
            combined_df['link'] = combined_df.apply(lambda x: self._link_recommendation_with_safety_issue(x['recommendation'], x['safety_issue']), axis=1)

            combined_df.to_csv(links_csv_path, index=False) 
        
        visual_path = os.path.join(self.output_folder,
                               self.reports_config.get("folder_name").replace(r'{{report_id}}', report_id),
                               self.reports_config.get("recommendation_safety_issue_links_visual_file_name").replace(r'{{report_id}}', report_id))
        
        if not os.path.exists(visual_path):
            self._generate_visualization_of_links(combined_df, report_id,visual_path)
        

    def evaluate_links_for_report(self):
        
        print("  Linking recommendations with safety issues")

        output_folder_reader = OutputFolderReader()

        output_folder_reader.process_reports_with_specific_files(self._evaluate_all_possible_links, ["recommendations_file_name", "safety_issues"])