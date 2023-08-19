import Gather_Wrangle.PDFDownloader as PDFDownloader
import Gather_Wrangle.PDFParser as PDFParser
import Extract_Analyze.TextSummarizer as TextSummarizer
import os
import argparse
import shutil
import ConfigReader

def download_extract(output_dir, download_config):
    # Download the PDFs
    PDFDownloader.downloadPDFs(output_dir, download_config.get('start_year'), download_config.get('end_year'), download_config.get('max_per_year'))

    # Extract the text from the PDFs
    PDFParser.convertPDFToText(output_dir)

def summarize(output_dir, get_cost):
    TextSummarizer.summarizeFiles(output_dir, get_cost)

def main():
    parser = argparse.ArgumentParser(description='A engine that will download, extract, and summarize PDFs from the marine accident investigation reports. More information can be found here: https://github.com/1jamesthompson1/TAIC-report-summary/')
    parser.add_argument("-r", "--refresh", help="Clears the output directory, otherwise functions will be run with what is already there.", action="store_true")
    parser.add_argument("-c", "--calculate_cost", help="Calculate the API cost of doing a summarize. Note this action itself will use some API token, however it should be a negligible amount. Currently not going to give an accurate response", action="store_true")
    parser.add_argument("-t", "--run_type", choices=["download", "summarize", "all"], required=True, help="The type of action the program will do. Download will download the PDFs and extraact the text. While Summarize will summarize the downloaded text. All will do both actions.")

    args = parser.parse_args()
   
    # Set working directory to output folder
    output_path = "output"
    if not os.path.exists(output_path):
        # Create the directory
        os.makedirs(output_path)
    elif args.refresh:
        # Delete the directory and recreate it
        shutil.rmtree(output_path, ignore_errors=True)
        os.makedirs(output_path)

    # Get the config settings for the engine.
    engine_settings = ConfigReader.configReader.get_config()['engine']

    get_cost = args.calculate_cost

    match args.run_type:
        case "download":
            download_extract(output_path, engine_settings.get('download'))
        case "summarize":
            summarize(output_path, get_cost)
        case "all":
            download_extract(output_path)
            summarize(output_path, get_cost)

if __name__ == "__main__":
    main()