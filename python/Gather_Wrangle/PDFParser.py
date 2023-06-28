from pypdf import PdfReader
import os
import re

def convertPDFToText(folder_path, text_out_path):
    if not os.path.exists(text_out_path):
        os.mkdir(text_out_path)
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.pdf'):
            pdf_path = os.path.join(folder_path, file_name)
            text_path = os.path.join(text_out_path, file_name.replace('.pdf', '.txt'))
            try:
                with open(pdf_path, 'rb') as pdf_file:
                    reader = PdfReader(pdf_file)
                    text = ""
                    for page in reader.pages:
                        text += page.extract_text() + "\n"
                    with open(text_path, 'w', encoding='utf-8-sig') as text_file:
                        text_file.write(text)
                    print(f'Extracted text from {pdf_path} and saved to {text_path}')
            except Exception as e:
                print(f'Error processing {pdf_path}: {e}')
