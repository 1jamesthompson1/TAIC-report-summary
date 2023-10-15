from .Extract_Analyze import ThemeGenerator, APICostEstimator, Summarizer

from .Gather_Wrangle import PDFDownloader, PDFParser

from .Verify import ThemeComparer, WeightingComparer

from . import Config


import os
import argparse
import shutil

def download_extract(output_dir, download_config, output_config):
    reports_config = output_config.get('reports')

    # Download the PDFs
    PDFDownloader.downloadPDFs(output_dir,
                                reports_config.get('folder_name'),
                                reports_config.get('pdf_file_name'),
                                download_config.get('start_year'),
                                download_config.get('end_year'),
                                download_config.get('max_per_year'))

    # Extract the text from the PDFs
    PDFParser.convertPDFToText(output_dir,
                                reports_config.get('pdf_file_name'),
                                reports_config.get('text_file_name'),
                                reports_config.get('folder_name'))

def generate_themes(output_dir, reports_config):
    ThemeGenerator.ThemeGenerator(output_dir,
                                  reports_config.get("folder_name"),
                                  reports_config.get("themes_file_name")).generate_themes()

def summarize(output_config):
    Summarizer.ReportSummarizer(output_config).summarize_reports()

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
    parser.add_argument("-t", "--run_type", choices=["download", "themes", "summarize", "all", "validate"], required=True, help="The type of action the program will do. Download will download the PDFs and extraact the text. themes generates the themes from all of the downloaded reports. While Summarize will summarize the downloaded text using the themes extracted. All will do all actions. validate will run through and do all of the validtion check to make sure the engine is working correctly. It will require the output folder to exist as well as some human generated output in a validation folder (which will follow the same structure as the output folder).")

    args = parser.parse_args()

        # Get the config settings for the engine.
    engine_settings = Config.configReader.get_config()['engine']
   
    # Set working directory to output folder
    output_path = engine_settings.get('output').get("folder_name")

    if not os.path.exists(output_path):
        # Create the directory
        os.makedirs(output_path)
    elif args.refresh:
        # Delete the directory and recreate it
        shutil.rmtree(output_path, ignore_errors=True)
        os.makedirs(output_path)

    if args.calculate_cost:
        get_cost = printout_cost_summary(args.run_type)
        return
        
    match args.run_type:
        case "download":
            download_extract(output_path, engine_settings.get('download'), engine_settings.get('output'))
        case "themes":
            generate_themes(output_path, engine_settings.get('output').get('reports'))
        case "summarize":
            summarize(engine_settings.get('output'))
        case "all":
            download_extract(output_path, engine_settings.get('download'), engine_settings.get('output'))
            generate_themes(output_path, engine_settings.get('output').get('reports'))
            summarize(engine_settings.get('output'))
        case "validate":
            validate()

if __name__ == "__main__":
    cli()