import os
import re
import shutil
from OpenAICaller import openAICaller

def summarizeText(input_text):
    return "Not yet implemented"

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
            contents_sections = extractContentsSection(input_text)
            if contents_sections:
                contents_section_csv = openAICaller.query(
                    "You are reading the contents section of a report and turning it into csv. There should be two columns; title, page. Title is the title listed. Page is the page number. Note that this text is extracted from a PDF and has some weird annomalies as well as the headers which should not be included when turning to csv. Please put double quotes around the header. Please ignore the figures",
                    contents_sections)
                with open(os.path.join(report_summarization_folder, 'contents_section.csv'), 'w', encoding='utf-8') as text_file:
                    text_file.write(contents_section_csv)

            # Summarize the text
            summary = summarizeText(input_text)
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