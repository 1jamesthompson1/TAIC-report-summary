from .Extract_Analyze import ThemeGenerator, APICostEstimator, Summarizer, ReportExtracting, OutputFolderReader, RecommendationSafetyIssueLinking, RecommendationResponseClassification

from .Gather_Wrangle import PDFDownloader, PDFParser, RecommendationSplitting

from .Verify import ThemeComparer, WeightingComparer

from . import Config, Modes

import pandas as pd
import os
import argparse
import shutil

def download_extract(output_dir, config, modes, refresh):
    reports_config = config.get('output').get('reports')
    download_config = config.get('download')

    # Download the PDFs
    PDFDownloader.ReportDownloader(output_dir,
                                reports_config.get('folder_name'),
                                reports_config.get('pdf_file_name'),
                                download_config.get('start_year'),
                                download_config.get('end_year'),
                                download_config.get('max_per_year'),
                                modes,
                                refresh).download_all()

    # Extract the text from the PDFs
    PDFParser.convertPDFToText(output_dir,
                                reports_config.get('pdf_file_name'),
                                reports_config.get('text_file_name'),
                                reports_config.get('folder_name'),
                                refresh)
    
    RecommendationSplitting.split_recommendations(config)
    

def safety_issue_and_recommendations(output_dir, config, refresh):

    reports_config = config.get('output').get('reports')

    # ReportExtracting.ReportExtractingProcessor(output_dir,
    #                                            reports_config.get('folder_name'),
    #                                               reports_config.get('safety_issues'),
    #                                               refresh).extract_safety_issues_from_reports(OutputFolderReader.OutputFolderReader(output_dir))
    
    # RecommendationSafetyIssueLinking.RecommendationSafetyIssueLinker(output_dir, reports_config).evaluate_links_for_report()

    RecommendationResponseClassification.RecommendationResponseClassificationProcessor().process(
            os.path.join(
                'data',
                config.get('data').get('recommendations_file_name')
            )
            ,
            os.path.join(
                output_dir,
                config.get('output').get('recommendation_responses_file_name')
            )
        )
    


def generate_themes(output_dir, reports_config, modes, refresh):
    ThemeGenerator.ThemeGenerator(output_dir,
                                  reports_config.get("folder_name"),
                                  reports_config.get("themes_file_name"),
                                  modes, refresh).generate_themes()

def summarize(output_config, use_predefined, modes, refresh):
    Summarizer.ReportSummarizer(output_config,
                                use_predefined,
                                modes, refresh).summarize_reports()

def printout_cost_summary(run_type):
    summary_strs = APICostEstimator.APICostEstimator().get_cost_summary_strings()

    match run_type:
        case "download":
            print(f"Downloading does not cost anything")
        case "summarize":
            print(summary_strs["summarize"])
        case "themes":
            print(summary_strs["themes"])
        case "all":
            print(summary_strs["all"])

def validate():
    ThemeComparer.ThemeComparer().compare_themes()
    WeightingComparer.WeightingComparer().compare_weightings()

def cli():
    parser = argparse.ArgumentParser(description='A engine that will download, extract, and summarize PDFs from the marine accident investigation reports. More information can be found here: https://github.com/1jamesthompson1/TAIC-report-summary/')
    parser.add_argument("-r", "--refresh", help="Clears the output directory, otherwise functions will be run with what is already there.", action="store_true")
    parser.add_argument("-c", "--calculate_cost", help="Calculate the API cost of doing a summarize. Note this action itself will use some API token, however it should be a negligible amount. Currently not going to give an accurate response", action="store_true")
    parser.add_argument("-t", "--run_type", choices=["download", "safety_issues_and_recommendations", "themes", "summarize", "all", "validate"], required=True, help="The type of action the program will do. Download will download the PDFs and extraact the text. themes generates the themes from all of the downloaded reports. While Summarize will summarize the downloaded text using the themes extracted. All will do all actions. validate will run through and do all of the validation check to make sure the engine is working correctly. It will require the output folder to exist as well as some human generated output in a validation folder (which will follow the same structure as the output folder).")
    parser.add_argument("-p", "--predefined", help="Use predefined themes that will be used for the summarize weightings. The predefined themes must follow a psecifc format, be in the outputfolder and be called predefined_themes.yaml (or whatever the config.yaml file is set it as). It will also skip the themes step if you run all.", action="store_true", default=False)
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

    if args.calculate_cost:
        get_cost = printout_cost_summary(args.run_type)
        return
        
    match args.run_type:
        case "download":
            download_extract(output_path,
                             engine_settings,
                             modes, args.refresh)
            
        case "safety_issues_and_recommendations":
            safety_issue_and_recommendations(output_path,
                                             engine_settings,
                                             args.refresh)

        case "themes":
            generate_themes(output_path,
                            engine_settings.get('output').get('reports'),
                            modes, args.refresh)
        case "summarize":
            summarize(engine_settings.get('output'),
                      args.predefined,
                      modes, args.refresh)
        case "all":
            download_extract(output_path,
                             engine_settings,
                             modes, args.refresh)
            if not args.predefined:
                generate_themes(output_path,
                                engine_settings.get('output').get('reports'),
                                modes, args.refresh)
            summarize(engine_settings.get('output'),
                      args.predefined , 
                      modes, args.refresh)
        case "validate":
            validate()

if __name__ == "__main__":
    cli()