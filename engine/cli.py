from .utils import Config, OutputFolderReader, Modes

from .gather import PDFDownloader, PDFParser, DataDownloading
from .extract import ReportExtracting 
from .analyze import RecommendationSafetyIssueLinking, RecommendationResponseClassification

import os
import argparse

def gather(output_dir, config, modes, refresh):
    output_config = config.get('output')
    download_config = config.get('download')

    # Download the PDFs
    PDFDownloader.ReportDownloader(os.path.join(output_dir, output_config.get('report_pdf_folder_name')),
                                output_config.get('report_pdf_file_name'),
                                download_config.get('start_year'),
                                download_config.get('end_year'),
                                download_config.get('max_per_year'),
                                modes,
                                download_config.get('ignored_reports'),
                                refresh).download_all()

    # Extract the text from the PDFsconfig
    PDFParser.convertPDFToText(os.path.join(output_dir, output_config.get('report_pdf_folder_name')),
                               os.path.join(output_dir, output_config.get('parsed_reports_df_file_name')),
                                refresh)
    
    DataDownloading.get_recommendations(config.get('data').get('data_hosted_folder_location') + config.get('data').get('recommendations_file_name'),
                                                output_config.get('recommendations_df_file_name'),
                                                refresh)

def extract(output_dir, config, refresh):

    output_config = config.get('output')

    report_extractor = ReportExtracting.ReportExtractingProcessor(os.path.join(output_dir, output_config.get('parsed_reports_df_file_name')), refresh)

    report_extractor.extract_safety_issues_from_reports(os.path.join(output_dir, output_config.get('important_text_df_file_name')),os.path.join(output_dir, output_config.get('safety_issues_df_file_name')))


def analyze(output_dir, config, refresh):

    reports_config = config.get('output').get('reports')

    RecommendationSafetyIssueLinking.RecommendationSafetyIssueLinker(output_dir, reports_config).evaluate_links_for_report()

    RecommendationResponseClassification.RecommendationResponseClassificationProcessor().process(
            os.path.join(
                'data',
                config.get('data').get('recommendations_file_name')
            )
            ,
            os.path.join(
                output_dir,
                config.get('output').get('recommendation_responses_file_name')
            ),
            (config.get('download').get('start_year'), config.get('download').get('end_year'))
        )
    

def cli():
    parser = argparse.ArgumentParser(description='A engine that will download, extract, and summarize PDFs from the marine accident investigation reports. More information can be found here: https://github.com/1jamesthompson1/TAIC-report-summary/')
    parser.add_argument("-r", "--refresh", help="Clears the output directory, otherwise functions will be run with what is already there.", action="store_true")
    parser.add_argument("-c", "--calculate_cost", help="Calculate the API cost of doing a summarize. Note this action itself will use some API token, however it should be a negligible amount. Currently not going to give an accurate response", action="store_true")
    parser.add_argument("-t", "--run_type", choices=["gather", "extract", "analyze", "all"], required=True, help="This is the sort of actions you want to")
    parser.add_argument("-m", "--modes", choices=["a", "r", "m"], nargs="+", help="The modes of the reports to be processed. a for aviation, r for rail, m for marine. Defaults to all.", default=["a", "r", "m"])

    args = parser.parse_args()

    modes = [Modes.Mode[arg] for arg in args.modes]

        # Get the config settings for the engine.
    engine_settings = Config.configReader.get_config()['engine']
   
    # Set working directory to output folder
    output_path = engine_settings.get('output').get("folder_name")

    if not os.path.exists(output_path):
        # Create the directory
        os.makedirs(output_path)
        
    match args.run_type:
        case "gather":
            gather(output_path, engine_settings, modes, args.refresh)
        case "extract":
            extract(output_path, engine_settings, args.refresh)
        case "analyze":
            analyze(output_path, engine_settings, args.refresh)
        case "all":
            gather(output_path, engine_settings, modes, args.refresh)
            extract(output_path, engine_settings, args.refresh)
            analyze(output_path, engine_settings, args.refresh)

if __name__ == "__main__":
    cli()