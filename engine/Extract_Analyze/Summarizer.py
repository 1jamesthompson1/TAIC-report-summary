import os
import random
import regex as re
from ..OpenAICaller import openAICaller
import pandas as pd

from .Themes import ThemeReader
from .OutputFolderReader import OutputFolderReader

class ReportSummarizer:
    def __init__(self, output_config):
        self.output_folder = output_config.get("folder_name")
        self.theme_reader = ThemeReader()
        self.report_reader = OutputFolderReader()
        self.open_ai_caller = openAICaller


        self.overall_summary_path = os.path.join(self.output_folder, output_config.get("summary_file_name"))

        self.report_dir = output_config.get("reports").get("folder_name")
        self.report_summary_file_name = output_config.get("reports").get("summary_file_name")


    def summarize_reports(self):
        if not os.path.exists(self.output_folder):
            print("Output folder and hence extracted text does not exist. Reports cannot be summarized.")
            return
        
        with open(self.overall_summary_path, 'w', encoding='utf-8') as summary_file:
            summary_file.write("ReportID," "PagesRead," + self.theme_reader.get_theme_str() + "\n")

        self.report_reader.process_reports(self.summarize_report)      

    def summarize_report(self, report_id, report_text):
        print(f'Summarizing {report_id}')


        # Get the pages that should be read
        text_to_be_summarized, pages_read = ReportExtractor(report_text, report_id).extract_important_text()
        if text_to_be_summarized == None:
            print(f'Could not extract text to be summarized from {report_id}')
            return
        
        summary = self.summarize_text(report_id, text_to_be_summarized)
        if (summary == None):
            print(f'  Could not summarize {report_id}')
            return
        
        report_summary_path = os.path.join(self.output_folder,
                                           self.report_dir.replace(r'{{report_id}}', report_id),
                                           self.report_summary_file_name.replace(r'{{report_id}}', report_id))
        summary_str = report_id + "," + str(pages_read).replace(",", " ") + "," + summary + "\n"

        with open(report_summary_path, 'w', encoding='utf-8') as summary_file:
            summary_file.write(summary_str)

        # Add text to overall csv
        with open(self.overall_summary_path, 'a', encoding='utf-8') as summary_file:
            summary_file.write(summary_str)

        print(f'Summarized {report_id} and saved summary to {report_summary_path}, line also added to {self.overall_summary_path}')
    
    def summarize_text(self, report_id, text) -> str:
        # example weightings
        example_weightings = ""
        for i in range(0, 2):
            weighting = []
            for j in range(0, self.theme_reader.get_num_themes()):
                if j == self.theme_reader.get_num_themes()-1:
                    weighting.append(100-sum(weighting))
                else:
                    weighting.append(random.randint(0, 100-sum(weighting) if len(weighting) > 0 else 0))

            example_weightings += "'" + ",".join([str(weight_int) for weight_int in weighting]) + "' "

        while True:
            try: 
                numberOfResponses = 5
                responses = openAICaller.query(
                    "Please read this report and determine what themes had the most contribution. Your response should be " + str(self.theme_reader.get_num_themes()) + " numbers that add up to 100. For example: " + example_weightings + ".\n\nHere is a summary of the 5 themes:\n" + self.theme_reader.get_theme_description_str() + "\n",
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
                print(f"  Incorrect response from model retrying. \n  Response was: '{responses}'")
        
        return weighting_str
    



class ReportExtractor:
    def __init__(self, report_text, report_id):
        self.report_text = report_text
        self.report_id = report_id

    def extract_important_text(self) -> (str, list):
        # Get the pages that should be read
        contents_sections = self.extract_contents_section()
        if contents_sections == None:
            print(f'  Could not find contents section in {self.report_id}')
            return None, None
        
        pages_to_read = self.extract_pages_to_read(contents_sections)

        if pages_to_read == None:
            print(f'  Could not find the findings or analysis section for {self.report_id}')
            return None, None

        # Retrieve that actual text for the page numbers.
        print(f"  I am going to be reading these pages: {pages_to_read}")     
        text = ""
        for page in pages_to_read: # Loop through the pages and extract the text
            extracted_text = self.extract_text_between_page_numbers(page, page+1)
            if extracted_text == None:
                print(f"  Could not extract text from page {page}")
                continue
            text += extracted_text

        return text, pages_to_read

    def extract_text_between_page_numbers(self, page_number_1, page_number_2) -> str:
        # Create a regular expression pattern to match the page numbers and the text between them
        pattern = r"<< Page {} >>.*<< Page {} >>".format(page_number_1, page_number_2)
        matches = re.findall(pattern, self.report_text, re.DOTALL)


        if matches:
            return matches[0]
        else:
            # Return everything after the first page number match
            pattern = r"<< Page {} >>.*".format(page_number_1)
            matches = re.findall(pattern, self.report_text, re.DOTALL)
            if matches:
                return matches[0]
            else:
                print("Error: Could not find text between pages " + str(page_number_1) + " and " + str(page_number_2))
                return None          

    def extract_contents_section(self) -> str:
        startRegex = r'((Content)|(content)|(Contents)|(contents))([ \w]{0,30}.+)([\n\w\d\sāēīōūĀĒĪŌŪ]*)(.*\.{5,})'
        endRegex = r'(?<!<< Page \d+ >>[,/.\w\s]*)[\.]{2,} {1,2}[\d]{1,2}'

        # Get the entire string between the start and end regex
        startMatch = re.search(startRegex, self.report_text)
        endMatches = list(re.finditer(endRegex, self.report_text))
        if endMatches:
            endMatch = endMatches[-1]
        else:
            print("Error cant find the end of the contents section")
            return None
        
        if startMatch and endMatch:
            contents_section = self.report_text[startMatch.start():endMatch.end()]
        else:
            return None

        return contents_section

    def extract_pages_to_read(self, content_section) -> list:

        while True: # Repeat until the LLMs gives a valid response
            try: 
                # Get 5 responses and only includes pages that are in atleast 3 of the responses
                model_response = openAICaller.query(
                        "What page does the analysis start on. What page does the findings finish on? Your response is only a list of integers. No words are allowed in your response. e.g '12,45' or '10,23'. If you cant find the analysis and findings section just return 'None'",
                        content_section,
                        temp = 0)
                
                if model_response == "None":
                    return None

                pages_to_read = [int(num) for num in model_response.split(",")]

                # Make the array every page between first and last
                pages_to_read = list(range(pages_to_read[0], pages_to_read[-1] + 1))
                break
            except ValueError:
                print(f"  Incorrect response from model retrying. \n  Response was: '{model_response}'")

        return pages_to_read