import PDFDownloader
import PDFParser
import TextSummarizer
import shutil

download_dir = "downloaded_pdfs"
text_dir = "extracted_text"
summarized_dir = "summarised"

shutil.rmtree(download_dir, ignore_errors=True)
shutil.rmtree(text_dir, ignore_errors=True)
shutil.rmtree(summarized_dir, ignore_errors=True)

PDFDownloader.downloadPDFs(download_dir, 2000, 2003, 5)
PDFParser.convertPDFToText(download_dir, text_dir)
TextSummarizer.summarizeFiles(text_dir, summarized_dir, 5)