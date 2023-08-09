import os
import random
import re
from OpenAICaller import openAICaller
from Extract_Analyze.ThemeReader import ThemeReader


def summarizeFiles(input_folder, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    themeReader = ThemeReader('../config.yaml')

    # Create the summary csv
    with open(os.path.join(output_folder, "summary.csv"), 'w', encoding='utf-8') as summary_file:
        summary_file.write("ReportID," + themeReader.get_theme_str() + "\n")


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
            summary = summarizeText(re.sub(".txt", "", filename), input_text, pagesToRead_array, themeReader)
            with open(os.path.join(report_summarization_folder, "summary"), 'w', encoding='utf-8') as summary_file:
                summary_file.write(str(summary))

            # Add text to overall csv
            with open(os.path.join(output_folder, "summary.csv"), 'a', encoding='utf-8') as summary_file:
                summary_file.write(str(summary) + "\n")

            print(f'Summarized {filename} and saved summary to {os.path.join(report_summarization_folder, filename.replace(".txt", "_summary.txt"))}')

def summarizeText(reportID, input_text, pagesToRead, themeReader: ThemeReader):
    print("  I am going to be reading these pages")
    print(pagesToRead)

    # Loop through the pages and extract the text
    text = ""
    for page in pagesToRead:
        text += extract_text_between_page_numbers(input_text, page, page+1)

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
            
            # Convert the responses into a list of lists
            weightings = [[int(num) for num in response.split(",")] for response in responses]

            # Check that each row of weightings add up to 100
            if any(sum(weighting) != 100 for weighting in weightings):
                print("The numbers you provided do not add up to 100. Please try again.")
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
            print(f"Incorrect repsonse from model retrying. \n  Response was: '{responses}'")
    
    return reportID + "," + weighting_str

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