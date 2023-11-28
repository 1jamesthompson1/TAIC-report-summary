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
        self.report_summary_file_name = output_config.get("reports").get("full_summary_file_name")
        self.report_weightings_file_name = output_config.get("reports").get("weightings_file_name")


    def summarize_reports(self):
        if not os.path.exists(self.output_folder):
            print("WARNING: Output folder and hence extracted text does not exist. Reports cannot be summarized.")
            return
        
        with open(self.overall_summary_path, 'w', encoding='utf-8') as summary_file:
            summary_file.write("ReportID," +  "PagesRead," + self.theme_reader.get_theme_str() +  "," + self.theme_reader.get_theme_str().replace('",', '_std",') + ",Complete" + ",ErrorMessage" + "\n")
        
        # Prepare system prompt
        number_of_themes = self.theme_reader.get_num_themes()
        self.system_prompt = f"""
You will be provided with a Transport Accident investigation report. In particular you will be given the analysis and the finding sections of this report.

Please read this report and determine what safety themes had the most contribution. This means I want to find out what are the most important and relevant safety themes for this particular accident.

For each safety theme can you please explain the safety factors and safety issues in this accident that are related to the safety theme. If there was nothing in the accident that is realated to the safety theme then simply say so.
Please also assign a releative percentage to each of the safety theme indicating how related it is to this partiuclar accident. These percentages need to add up to 100. If there is no connection between the safety theme and the accident then the percentage should be zero.

Here is a summary of the {number_of_themes} themes:

{self.theme_reader.get_theme_description_str()}

---
Note that I want this to be repeatable and deterministic as possible.

Definition for the terms used can be found below:

Safety factor - Any (non-trivial) events or conditions, which increases safety risk. If they occurred in the future, these would
increase the likelihood of an occurrence, and/or the
severity of any adverse consequences associated with the
occurrence.

Safety issue - A safety factor that:
• can reasonably be regarded as having the
potential to adversely affect the safety of future
operations, and
• is characteristic of an organisation, a system, or an
operational environment at a specific point in time.
Safety Issues are derived from safety factors classified
either as Risk Controls or Organisational Influences.

Safety theme - Indication of recurring circumstances or causes, either across transport modes or over time. A safety theme may
cover a single safety issue, or two or more related safety
issues.
---
        """
        

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
        
        weightings, full_summary_parsed, full_summary_unparsed = summary # unpack tuple response

        report_folder_path = os.path.join(self.output_folder,
                                            self.report_dir.replace(r'{{report_id}}', report_id))
        # Output the weightings to a file

        report_weightings_path = os.path.join(report_folder_path,
                                           self.report_weightings_file_name.replace(r'{{report_id}}', report_id))
        summary_str = report_id + "," + str(pages_read).replace(",", " ") + "," + weightings + ",true" + ",N/A" + "\n"

        with open(report_weightings_path, 'w', encoding='utf-8') as summary_file:
            summary_file.write(summary_str)

        with open(self.overall_summary_path, 'a', encoding='utf-8') as summary_file:
            summary_file.write(summary_str)

        # Output the full summary to a file

        report_summary_path = os.path.join(report_folder_path,
                                           self.report_summary_file_name.replace(r'{{report_id}}', report_id))
        
        with open(report_summary_path, 'w', encoding='utf-8') as summary_file:
            summary_file.write("-----------------------------\n---Full Summary Unparsed---\n-----------------------------\n\n")
            summary_file.write(full_summary_unparsed)
            summary_file.write("\n\n-----------------------------\n---Full Summary parsed---\n-----------------------------\n\n")
            for theme in full_summary_parsed:
                summary_file.write(f"{theme['name']} - {theme['percentage']}% - \n{theme['reason']}\n\n")

        print(f'Summarized {report_id} and saved full_ summary to {report_summary_path} and the weightings to {report_weightings_path}, report line also added to {self.overall_summary_path}')
    
    def summarize_text(self, report_id, text) -> (str, str, str):
        max_attempts = 3
        attempts = 0
        while True:
            attempts += 1
            if attempts == max_attempts+1:
                return None
            numberOfResponses = 1
            responses = openAICaller.query(
                self.system_prompt,
                text,
                n=numberOfResponses,
                temp = 0)
            
            if responses == None:
                return None
            
            # Convert the responses into a list of lists
            if numberOfResponses == 1:
                responses = [responses]
            parsed_responses = [self.parse_weighting_response(response,self.generate_parse_template()) for response in responses]

            if parsed_responses is None:
                continue


            weightings = [[theme['percentage'] for theme in response] for response in parsed_responses]
            print(f"  The weightings are:\n  {weightings}")
            weightings = pd.DataFrame(weightings)

            # Remove all rows that dont add up to 100
            weightings = weightings[weightings.sum(axis=1).eq(100)]

            if weightings.shape[0] == 0:
                print(f"  WARNING: No valid responses with a sum of 100 retrying.")
                continue

            # Get an average of all of the rows
            weighting_average = list(weightings.mean(axis=0))

            print(f"  The average weightings are: {weighting_average}")
            
            # Scale the average to add up to 100
            weighting_average = [round((weight * 100) / sum(weighting_average), ndigits = 3) for weight in weighting_average]

            if round(sum(weighting_average)) != 100:
                print(f"  WARNING: weightings should add up to 100 after scaling. Where it currently adds up to {round(sum(weighting_average),3)}")
                print(f"   Weightings were: {weighting_average}")
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

        return weighting_str, parsed_responses[0], responses[0] # Currently assuming that there is only going to be one response.
        
    def generate_parse_template(self):
        template = ""

        for theme_title in self.theme_reader.get_theme_titles():
            template += f"xy% - {theme_title}\n[given reason here]\n\n"

        return template
    
    def parse_weighting_response(self, response, template):
        max_attempts = 3
        attempts = 0
        
        while True:
            attempts += 1
            if attempts == max_attempts:
                print(f"  WARNING: Could not get a parsable response in time.")
                return None
            response = openAICaller.query(
                    f"""
Please take this response and parse it into the specific template below.

It is important that you follow the specific template as laid out below. This should be exact and there should be no extra characters before or after the template.

Both the percentages and the reasoning behind the percentages should be parsed verbatim

Here is the template:

{template}
                    """,
                    response,
                    large_model=True,
                    temp = 0)
            
            if response == None:
                return None
            
            themes = response.split('\n\n')
            result_list = []
            
            try: 
                if len(themes) != self.theme_reader.get_num_themes():
                    raise ValueError(f"  Incorrect number of themes in the response")
                # Iterate over each theme
                for theme in themes:
                    lines = theme.split('\n')

                    if len(lines) != 2:
                        raise ValueError(f"  Incorrect number of lines in theme")
                    
                    # Extract percentage, name, and reason
                    percentage, name = lines[0].split(' - ', 1)
                    reason = lines[1].strip()

                    # Convert percentage to integer
                    percentage = int(percentage.strip('%'))

                    # Create a dictionary and append to the result list
                    result_list.append({'name': name.strip(), 'percentage': percentage, 'reason': reason})
                
                break

            except ValueError:
                print(f"  WARNING: Incorrect response from model retrying. \n  Response from request was: '{response}'\n\n\nand the response to be converted was:\n\n'{response}'")
                continue

        return result_list

        


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