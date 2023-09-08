from pypdf import PdfReader
import os
import re

def convertPDFToText(output_dir, pdf_file_name_template, text_file_name_template):
    if not os.path.exists(output_dir):
        print("No reports have been downloaded so far. Please make sure that reports have been downloaded before running this function.")
        return

    for report_id in os.listdir(output_dir):
        # Go into each folder and find the pdf
        report_dir = os.path.join(output_dir, report_id)
        pdf_path = os.path.join(report_dir, pdf_file_name_template.replace(r'{{report_id}}', report_id))
        if os.path.exists(pdf_path):
            text_path = os.path.join(report_dir, text_file_name_template.replace(r'{{report_id}}', report_id))
            try:
                with open(pdf_path, 'rb') as pdf_file:
                    reader = PdfReader(pdf_file)
                    text = ""
                    for page in reader.pages:
                        text += page.extract_text() + "\n"

                    text = formatText(text, 'old')

                    # Cleaning up the text a bit
                    # text = cleanText(text)

                    with open(text_path, 'w', encoding='utf-8-sig') as text_file:
                        text_file.write(text)
                    print(f'Extracted text from {pdf_path} and saved to {text_path}')
            except Exception as e:
                print(f'Error processing {pdf_path}: {e}')


def formatText(text, style):
    """Format the string
    This will make the headers and page numbers easier to find.
    """
    
    # Clean up page numbers
    text = re.sub(r'(\| )?(Page \d+)( \|)?', r'\n<< \2 >>\n', text)

    return text

