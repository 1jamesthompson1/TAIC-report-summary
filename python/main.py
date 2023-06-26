import Gather_Wrangle.PDFDownloader as PDFDownloader
import Gather_Wrangle.PDFParser as PDFParser
import Extract_Analyze.TextSummarizer as TextSummarizer
import shutil
import os

# Take cmd arguments

download_dir = "downloaded_pdfs"
text_dir = "extracted_text"
summarized_dir = "summarised"

# Set working directory to output folder
output_path = "output"
if not os.path.exists(output_path):
    # Create the directory
    os.makedirs(output_path)

# Change the current directory to the specified directory
os.chdir(output_path)

shutil.rmtree(download_dir, ignore_errors=True)
shutil.rmtree(text_dir, ignore_errors=True)
shutil.rmtree(summarized_dir, ignore_errors=True)

PDFDownloader.downloadPDFs(download_dir, 2000,2002, 4)
PDFParser.convertPDFToText(download_dir, text_dir)
TextSummarizer.summarizeFiles(text_dir, summarized_dir, 5)

os.chdir("../python")