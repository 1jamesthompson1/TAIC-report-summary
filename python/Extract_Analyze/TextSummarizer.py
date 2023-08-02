import os
import re
import shutil
from OpenAICaller import openAICaller
import pandas as pd

def summarizeFiles(input_folder, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    for filename in os.listdir(input_folder):
        if filename.endswith('.txt'):
            with open(os.path.join(input_folder, filename), 'r', encoding='utf-8', errors='replace') as f:
                input_text = f.read()
            if len(input_text) < 100:
                    continue
                
            
            # create summarizetion folder for each report
            report_summarization_folder = os.path.join(output_folder, filename.replace('.txt', ''))
            if not os.path.exists(report_summarization_folder):
                os.makedirs(report_summarization_folder)
            else: # delete old folder
                shutil.rmtree(report_summarization_folder)
                os.makedirs(report_summarization_folder) 

            # extract contents section and output to file
            # check to see if contents section has already been extracted
            if os.path.exists(os.path.join(report_summarization_folder, 'contents_section.csv')):
                print("Contents section already extracted for " + filename)
            else:
                contents_sections = extractContentsSection(input_text)
                if contents_sections:
                    contents_section_csv = openAICaller.query(
                        "Please turn this extracted content section from a pdf into a csv. Two columns one is \"title\" and the other is the \"page\". Make sure to include the header row in the CSV output and surround all cell values with double quotes. Include the header row.",
                        contents_sections)
                    with open(os.path.join(report_summarization_folder, 'contents_section.csv'), 'w', encoding='utf-8') as text_file:
                        text_file.write(contents_section_csv)
                else:
                    print(f'Could not find contents section in {filename}')
                    continue

            # Summarize the text
            # Read csv file as a dataframe
            df = pd.read_csv(os.path.join(report_summarization_folder, 'contents_section.csv'), on_bad_lines = "warn")
            df['page'] = pd.to_numeric(df['page'], errors='coerce')

            summary = summarizeText(re.sub(".txt", "", filename), input_text, df)
            with open(os.path.join(report_summarization_folder, "summary"), 'w', encoding='utf-8') as summary_file:
                summary_file.write(str(summary))
            print(f'Summarized {filename} and saved summary to {os.path.join(report_summarization_folder, filename.replace(".txt", "_summary.txt"))}')

# extract the contents section of the reports
def extractContentsSection(pdf_string):
    startRegex = r'((Content)|(content)|(Contents)|(contents))([ \w]{0,30}.+)'
    endRegex = r'[\.]{2,} {1,2}[\d]{1,2}'

    # Get the entire string between the start and end regex
    startMatch = re.search(startRegex, pdf_string)
    endMatches = list(re.finditer(endRegex, pdf_string))
    if endMatches:
        endMatch = endMatches[-1]
    else:
        print("Error cant find the end of the contents section")
        return None
    
    if startMatch and endMatch:
        contents_section = pdf_string[startMatch.start():endMatch.end()]
    else:
        return None

    return contents_section
def extract_text_between_page_numbers(text, page_number_1, page_number_2):
    # Create a regular expression pattern to match the page numbers and the text between them
    pattern = r"<< Page {} >>.*<< Page {} >>".format(page_number_1, page_number_2)
    matches = re.findall(pattern, text, re.DOTALL)

    if matches:
        return matches[0]
    else:
        return None