import os
import re
import shutil
from OpenAICaller import openAICaller
import pandas as pd

def summarizeFiles(input_folder, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Create the summary csv
    with open(os.path.join(output_folder, "summary.csv"), 'w', encoding='utf-8') as summary_file:
        summary_file.write("ReportID,\"Human Error\",\"Equipment Failure\",\"Weather and Environmental Factors\",\"Operational Practices\",\"Safety Management\"\n")


    for filename in os.listdir(input_folder):
        if filename.endswith('.txt'):
            with open(os.path.join(input_folder, filename), 'r', encoding='utf-8', errors='replace') as f:
                input_text = f.read()
            if len(input_text) < 100:
                    continue
                
            print(f'Summarizing {filename}')
            
            # create summarizetion folder for each report
            report_summarization_folder = os.path.join(output_folder, filename.replace('.txt', ''))
            if not os.path.exists(report_summarization_folder):
                os.makedirs(report_summarization_folder)

            # Get the pages to read
            contents_sections = extractContentsSection(input_text)
            if contents_sections == None:
                print(f'Could not find contents section in {filename}')
                continue

            # Repeat query until valid response is given
            while True:
                try: 
                    pagesToRead = openAICaller.query(
                            "What page does the analysis start on. What page does the findings finish on? Your response is only a list of integers. No words are allowed in your response. e.g '12,45' or '10,23'",
                            contents_sections,
                            temp = 1)
                    pagesToRead_array = [int(num) for num in pagesToRead.split(",")]
                    # Make the array every page between first and last
                    pagesToRead_array = list(range(pagesToRead_array[0], pagesToRead_array[-1] + 1))
                    break
                except ValueError:
                    print(f"  Incorrect repsonse from model retrying. \n  Response was: '{pagesToRead}'")

            # Summarize the text
            summary = summarizeText(re.sub(".txt", "", filename), input_text, pagesToRead_array)
            with open(os.path.join(report_summarization_folder, "summary"), 'w', encoding='utf-8') as summary_file:
                summary_file.write(str(summary))

            # Add text to overall csv
            with open(os.path.join(output_folder, "summary.csv"), 'a', encoding='utf-8') as summary_file:
                summary_file.write(str(summary) + "\n")

            print(f'Summarized {filename} and saved summary to {os.path.join(report_summarization_folder, filename.replace(".txt", "_summary.txt"))}')

def summarizeText(reportID, input_text, pagesToRead):
    print("  I am going to be reading these pages")
    print(pagesToRead)

    # Loop through the pages and extract the text
    text = ""
    for page in pagesToRead:
        text += extract_text_between_page_numbers(input_text, page, page+1)


   
    while True:
        try: 
            weightings = openAICaller.query(
                "Please read this report and determine what themes had the most contribution. Themes are: Human Error, Equipment Failure, Weather and Environmental Factors, Operational Practices, Safety Management. Your response should be 5 numbers that add up to 100. For example '25,35,10,13,17' or '0,40,50,10,0'.\n\nHere is a summary of the 5 themes:\n1. Human Error:\n\n- Inexperience: Lack of experience or training leading to mistakes.\n- Fatigue: Tiredness or sleep deprivation affecting performance.\n- Complacency: Overconfidence leading to oversight of safety procedures.\n- Distractions: External factors diverting attention from critical tasks.\n- Miscommunication: Errors in conveying or understanding information.\n\n2. Equipment Failure:\n\n- Mechanical Failure: Issues with machinery or equipment on board.\n- Electrical Failure: Problems with electrical systems and components.\n- Navigation Equipment Failure: Faulty or inaccurate navigational instruments.\n- Communication Equipment Failure: Breakdown in communication systems.\n\n3. Weather and Environmental Factors:\n\n- Rough Weather: Adverse sea conditions impacting vessel stability.\n- Visibility: Poor visibility leading to navigation challenges.\n- Ice/Icing: Ice accumulation affecting vessel performance.\n- Currents: Strong currents affecting vessel maneuverability.\n\n4. Operational Practices:\n\n- Improper Navigation: Errors in route planning or navigation execution.\n- Unsafe Speed: Operating the vessel at an unsafe speed for the conditions.\n- Improper Loading: Incorrect distribution or excessive cargo load.\n- Improper Watchkeeping: Negligence in monitoring and lookout duties.\n\n5. Safety Management:\n\n- Lack of Safety Procedures: Absence or inadequate safety protocols.\n- Non-Compliance: Failure to follow safety regulations and guidelines.\n- Safety Culture: Poor safety culture onboard or within the organization.\n- Bridge Resource Management\n\n\n",
                text,
                large_model = True)

            weightings_array = [int(num) for num in weightings.split(",")]
            if sum(weightings_array) != 100:
                print("The numbers you provided do not add up to 100. Please try again.")
                continue
            break
        except ValueError:
            print(f"Incorrect repsonse from model retrying. \n  Response was: '{weightings}'")
    
    return reportID + "," + weightings

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

def extractSectionText(text, header, startPage, endPage, NextHeader):
    print("Looking for header: " + header + " from page " + str(startPage) + " to " + str(endPage) + " with next header: " + NextHeader)

    return extract_text_between_page_numbers(text, startPage, endPage)

def extract_text_between_page_numbers(text, page_number_1, page_number_2):
    # Create a regular expression pattern to match the page numbers and the text between them
    pattern = r"<< Page {} >>.*<< Page {} >>".format(page_number_1, page_number_2)
    matches = re.findall(pattern, text, re.DOTALL)

    if matches:
        return matches[0]
    else:
        return None