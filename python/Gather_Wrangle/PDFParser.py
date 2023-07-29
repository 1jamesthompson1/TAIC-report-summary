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

                    text = formatText(text, 'old')

                    # Cleaing up the text a bit
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

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer

nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

def cleanText(text):
    tokens = word_tokenize(text.lower())

    # Remove stopwords
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [token for token in tokens if token not in stop_words]

    # Lemmatize the words
    lemmatizer = WordNetLemmatizer()
    lemmatized_tokens = [lemmatizer.lemmatize(token) for token in filtered_tokens]

    # Join the preprocessed tokens back into a single string
    return' '.join(lemmatized_tokens)
