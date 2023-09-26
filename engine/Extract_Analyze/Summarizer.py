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
            summary_file.write("ReportID," +  "PagesRead," + self.theme_reader.get_theme_str() +  "," + self.theme_reader.get_theme_str().replace('",', '_std",') + ",Complete" + ",ErrorMessage" + "\n")
        
        # Prepare system prompt
        number_of_themes = self.theme_reader.get_num_themes()
        self.system_prompt = f"Please read this report and determine what themes had the most contribution. Can you please provide a paragraph for each theme with how much you think it contributed to the accident? You should provide percentages for each of the {number_of_themes} themes, with all the percentages adding up to 100.\n\nHere is a summary of the {number_of_themes} themes:\n{self.theme_reader.get_theme_description_str()}\n\n---\nNote that I want this to be repeatable and deterministic as possible."
        

        print("Summarizing reports...")
        print(f"  System prompt: {self.system_prompt}")

        self.report_reader.process_reports(self.summarize_report)      

    def summarize_report(self, report_id, report_text):
        print(f'Summarizing {report_id}')


        # Get the pages that should be read
        text_to_be_summarized, pages_read = ReportExtractor(report_text, report_id).extract_important_text()
        if text_to_be_summarized == None:
            print(f'  Could not extract text to be summarized from {report_id}')
            summary_str = report_id + "," + "Error" + "," + "N/A" + ",false" + ",Could not extract text to summarize report with" + "\n"
            return
        
        summary = self.summarize_text(report_id, text_to_be_summarized)
        if (summary == None):
            print(f'  Could not summarize {report_id}')
            summary_str = report_id + "," + str(pages_read).replace(",", " ") + "," + "Error" + ",false" + ",Could not summarize report" + "\n"
            return
        
        report_summary_path = os.path.join(self.output_folder,
                                           self.report_dir.replace(r'{{report_id}}', report_id),
                                           self.report_summary_file_name.replace(r'{{report_id}}', report_id))
        summary_str = report_id + "," + str(pages_read).replace(",", " ") + "," + summary + ",true" + ",N/A" + "\n"

        with open(report_summary_path, 'w', encoding='utf-8') as summary_file:
            summary_file.write(summary_str)

        # Add text to overall csv
        with open(self.overall_summary_path, 'a', encoding='utf-8') as summary_file:
            summary_file.write(summary_str)

        print(f'Summarized {report_id} and saved summary to {report_summary_path}, line also added to {self.overall_summary_path}')
    
    def summarize_text(self, report_id, text) -> str:
        examples = self.get_example_weightings()
        max_attempts = 5
        while True:
            max_attempts -= 1
            if max_attempts == 0:
                return None
            numberOfResponses = 1
            responses = openAICaller.query(
                self.system_prompt,
                text,
                large_model = True,
                n=numberOfResponses,
                temp = 0)
            
            if responses == None:
                return None
            
            # Convert the responses into a list of lists
            if numberOfResponses == 1:
                responses = [responses]

            weightings = [self.convert_response_to_list(response, examples) for response in responses]

            # Convert to a pandas dataframe
            weightings = pd.DataFrame(weightings)

            # Remove all rows that dont add up to 100
            weightings = weightings[weightings.sum(axis=1).eq(100)]
            # Get an average of all of the rows
            weighting_average = list(weightings.mean(axis=0))

            # Scale the average to add up to 100
            weighting_average = [round(weight * 100 / sum(weighting_average), ndigits =3 ) for weight in weighting_average]

            if round(sum(weighting_average),3) != 100:
                print("Error weightings should add up to 100 after scaling.")
                continue

            # Calculate the standard deviation of the weightings
            weighting_std = list(weightings.std(axis=0))

            if weightings.shape[0] < numberOfResponses-2:
                print(f"  Did not get enough valid responses. Retrying. \n  Responses were: {responses}")
                continue
            
            # Convert the weightings into a string
            all_data = weighting_average + weighting_std
            weighting_str = ",".join([str(weight_int) for weight_int in all_data])

            print("  The weightings are: " + str(weighting_str))
            break
        
        return weighting_str
    
    def get_example_weightings(self):
        number_of_themes = self.theme_reader.get_num_themes()
        example_weightings = ""
        for i in range(0, 2):
            weighting = []
            for j in range(0, number_of_themes):
                if j == number_of_themes-1:
                    weighting.append(100-sum(weighting))
                else:
                    weighting.append(random.randint(0, 100-sum(weighting) if len(weighting) > 0 else 0))

            example_weightings += "'" + ",".join([str(weight_int) for weight_int in weighting]) + "' "

        return example_weightings
    
    def convert_response_to_list(self, response, examples):
        max_attempts = 3
        while True:
            max_attempts -= 1
            if max_attempts == 0:
                print(f"  Could not convert response to list")
                return None
            weighting_str = openAICaller.query(
                    f"Please convert this into a comma-separated list of percentages. Examples are: {examples}.",
                    response,
                    temp = 0)
            
            if weighting_str == None or weighting_str == "None":
                return None
            
            weightings = []
            try: 
                weightings = [int(num) for num in weighting_str.split(",")]
                return weightings
            except ValueError:
                print(f"  Incorrect response from model retrying. \n  Response from request was: '{weighting_str}' and the response to be converted was: '{response}'")
                continue
        


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