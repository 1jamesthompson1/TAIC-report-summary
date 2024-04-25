from engine.Extract_Analyze.OutputFolderReader import OutputFolderReader


import pandas as pd
import os


def split_recommendations(config):
    """
    This will open up the recommendation csv and split the csv into a csv for each report.
    """

    print("Copying recommendations from the recommendations csv to a separate csv for each report...")

    output_folder_reader = OutputFolderReader()

    recommendations = pd.read_csv(os.path.join("data", config.get("data").get("recommendations_file_name")))

    report_ids = output_folder_reader._get_report_ids()

    recommendations = recommendations[recommendations["report_id"].isin(report_ids)]

    reports_config = config.get('output').get('reports')

    recommendations.groupby("report_id").apply(lambda x: x[['report_id', "recommendation_id", "recommendation", "extra_recommendation_context"]].to_csv(os.path.join(output_folder_reader.output_folder,
                     reports_config.get("folder_name").replace(r'{{report_id}}', x.name),
                     reports_config.get("recommendations_file_name").replace(r'{{report_id}}', x.name)),
                     index=False))
