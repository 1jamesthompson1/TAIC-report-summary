import os
import random
import re
from OpenAICaller import openAICaller
from Extract_Analyze.ThemeReader import ThemeReader
import pandas as pd


def summarizeFiles(output_dir, get_cost):
    if not os.path.exists(output_dir):
        print("Output folder and hence extracted text does not exist. Reports cannot be summarized.")
        return

    themeReader = ThemeReader()

    # Create the summary csv
    with open(os.path.join(output_dir, "summary.csv"), 'w', encoding='utf-8') as summary_file:
        summary_file.write("ReportID," + themeReader.get_theme_str() + "\n")


    if get_cost:
        # Create the cost csv
        cost_csv_path = os.path.join(output_dir, "cost.csv")
        with open(cost_csv_path, 'w', encoding='utf-8') as cost_file:
            cost_file.write("ReportID,Tokens,normal,large\n")

    for report_id in os.listdir(output_dir):
        report_dir = os.path.join(output_dir, report_id)
        if not os.path.isdir(report_dir):
            continue
        text_path = os.path.join(report_dir, f'{report_id}.txt')
        if not os.path.exists(text_path):
            print(f"Could not find text file for {report_id}, skipping report.")
            continue
           
        with open(text_path, 'r', encoding='utf-8', errors='replace') as f:
            input_text = f.read()
        if len(input_text) < 100:
                continue
            
        print(f'Summarizing {report_id}')

        # Get the pages that should be read
        contents_sections = extractContentsSection(input_text)
        if contents_sections == None:
            print(f'Could not find contents section in {report_id}')
            continue
        while True: # Repeat until the LLMs gives a valid response
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

        # Retrieve that actual text for the page numbers.
        print(f"  I am going to be reading these pages: {pagesToRead_array}")     
        text = ""
        for page in pagesToRead_array: # Loop through the pages and extract the text
            extracted_text = extract_text_between_page_numbers(input_text, page, page+1)
            if extracted_text == None:
                print(f"  Could not extract text from page {page}")
                continue
            text += extracted_text

        # Summarize the text
        if get_cost:
            tokens = openAICaller.get_tokens(openAICaller.model, [text])[0]
            with open(cost_csv_path, 'a', encoding='utf-8') as cost_file:
                cost_file.write(f"{report_id},{tokens},{tokens/1000 * 0.0015},{tokens/1000 * 0.003}\n")
        else:
            summary = summarizeText(report_id, text, themeReader)
            if (summary == None):
                print(f'  Could not summarize {report_id}')
                continue
            
            summary_path = os.path.join(report_dir, f"{report_id}_summary.txt")
            with open(summary_path, 'w', encoding='utf-8') as summary_file:
                summary_file.write(str(summary))

            # Add text to overall csv
            csv_path = os.path.join(output_dir, "summary.csv")
            with open(csv_path, 'a', encoding='utf-8') as summary_file:
                summary_file.write(str(summary) + "\n")

            print(f'Summarized {report_id} and saved summary to {summary_path}, line also added to {csv_path}')

    # If getting cost print out summary.
    if get_cost:
        cost_df = pd.read_csv(cost_csv_path)
        print(f"Summary of API costs:\nNote this is only a lower bound\nAverage cost of summarizing a report: ${cost_df['large'].mean()}, with a total cost for all {len(os.listdir(output_dir))} reports of ${cost_df['large'].sum()}")
        

def summarizeText(reportID, text, themeReader: ThemeReader):

    if len(text) < 100:
        print("  The text is too short to summarize")
        return None

    # example weightings
    example_weightings = ""
    for i in range(0, 2):
        weighting = []
        for j in range(0, themeReader.get_num_themes()):
            if j == themeReader.get_num_themes()-1:
                weighting.append(100-sum(weighting))
            else:
                weighting.append(random.randint(0, 100-sum(weighting) if len(weighting) > 0 else 0))

        example_weightings += "'" + ",".join([str(weight_int) for weight_int in weighting]) + "' "

    while True:
        try: 
            numberOfResponses = 5
            responses = openAICaller.query(
                "Please read this report and determine what themes had the most contribution. Your response should be " + str(themeReader.get_num_themes()) + " numbers that add up to 100. For example: " + example_weightings + ".\n\nHere is a summary of the 5 themes:\n" + themeReader.get_theme_description_str() + "\n",
                text,
                large_model = True,
                n=numberOfResponses)
            
            if responses == None:
                return None
            
            # Convert the responses into a list of lists
            weightings = [[int(num) for num in response.split(",")] for response in responses]

            # Check that each row of weightings add up to 100
            if any(sum(weighting) != 100 for weighting in weightings):
                print("  The numbers you provided do not add up to 100. Please try again.")
                continue

                
            # Take the average of the weightings
            weighting_average = [sum(weight[i] for weight in weightings) / numberOfResponses for i in range(len(weightings[0]))]
            # Scale the average to add up to 100
            weighting_average = [weight * 100 / sum(weighting_average) for weight in weighting_average]

            if sum(weighting_average) != 100:
                print("Error weightings should add up to 100 after scaling.")
                continue

            # Convert the weightings into a string
            weighting_str = ",".join([str(weight_int) for weight_int in weighting_average])

            print("  The weightings are: " + str(weighting_str))
            break

        except ValueError:
            print(f"  Incorrect repsonse from model retrying. \n  Response was: '{responses}'")
    
    return reportID + "," + weighting_str

# extract the contents section of the reports
def extractContentsSection(pdf_string):
    startRegex = r'((Content)|(content)|(Contents)|(contents))([ \w]{0,30}.+)([\n\w\d\sāēīōūĀĒĪŌŪ]*)(.*\.{5,})'
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
        # Return everything after the first page number match
        pattern = r"<< Page {} >>.*".format(page_number_1)
        matches = re.findall(pattern, text, re.DOTALL)
        if matches:
            return matches[0]
        else:
            print("Error: Could not find text between pages " + str(page_number_1) + " and " + str(page_number_2))
            return None


