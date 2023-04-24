import os
import PyPDF2

def convertPDFToText(folder_path, text_out_path):
    if not os.path.exists(text_out_path):
        os.mkdir(text_out_path)
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.pdf'):
            pdf_path = os.path.join(folder_path, file_name)
            with open(pdf_path, 'rb') as pdf_file:
                pdf_reader = PyPDF2.PdfReader(pdf_file)
                text = ''
                for page in pdf_reader.pages:
                    text += page.extract_text()
                text_path = os.path.join(text_out_path, file_name.replace('.pdf', '.txt'))
                with open(text_path, 'w') as text_file:
                    text_file.write(text)
            print(f'Extracted text from {pdf_path} and saved to {text_path}')

# convertPDFToText("downloaded_pdfs", "extracted_text")