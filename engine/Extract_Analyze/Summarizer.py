import os
import yaml
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

        self.system_prompt = '''
You will be provided with a document delimited by triple quotes and a question. Your task is to answer the question using only the provided document and to cite the passage(s) of the document used to answer the question. There may be multiple citations needed. If the document does not contain the information needed to answer this question then simply write: "Insufficient information." If an answer to the question is provided, it must include quotes with citation.

You must follow these formats exactly.
For direct quotes there can only ever be one section mentioned:
"quote in here" (section.paragraph.subparagraph)
For indirect quotes there may be one section, multiple or a range: 
sentence in here (section.paragraph.subparagraph)
sentence in here (section.paragraph.subparagraph, section.paragraph.subparagraph, etc)
sentence in here (section.paragraph.subparagraph-section.paragraph.subparagraph)

Example quote formats would be:
"it was a wednesday afternoon when the boat struck" (3.4)
It was both the lack of fresh paint and the old radar dish that caused this accident (4.5.2, 5.4.4)
The lack of consitant training different language dialects caused a breakdown in communication (3.9-3.12)
'''


    def summarize_reports(self):
        if not os.path.exists(self.output_folder):
            print("WARNING: Output folder and hence extracted text does not exist. Reports cannot be summarized.")
            return
        
        with open(self.overall_summary_path, 'w', encoding='utf-8') as summary_file:
            summary_file.write("ReportID," +  "PagesRead," + self.theme_reader.get_theme_str() +  "," + self.theme_reader.get_theme_str().replace('",', '_std",') + ",Complete" + ",ErrorMessage" + "\n")
        
        # Prepare system prompt
        self.user_message_template = lambda report_text: f"""
'''
{report_text}
'''

Question:
Please take the provided safety themes below and assign a weighting to each of them. These weightings should be how much each safety theme contributed to the accident. All the weightings should add up to no more than 100. There should be a section for each theme even if the weightings is zero. If the cause of the accident cannot be attributed to one of the predfined safety themes you can add a "Other" theme with an explanation of what this cause is.

Please output your answer as straight yaml without using code blocks.
The yaml format should have a name (must be verbatim), precentage and explanation field (which uses a literal scalar block) for each safety theme.
Your yaml ouput should look like this:
 - name: safety theme name
  percentage: xy
  explanation: |
    multi line explanation goes here>
    with evidence and "quotes" etc.

----
=Here is a summary of the {self.theme_reader.get_num_themes()} safety themes=
{self.theme_reader.get_theme_str()}

=Here are some definitions=

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
        
        summary = self.summarize_text(text_to_be_summarized)
        
        if (summary == None):
            print(f'  Could not summarize {report_id}')
            summary_str = report_id + "," + str(pages_read).replace(",", " ") + "," + "Error" + ",false" + ",Could not summarize report" + "\n"
            with open(self.overall_summary_path, 'a', encoding='utf-8') as summary_file:
                summary_file.write(summary_str)
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
            yaml.dump(full_summary_parsed, summary_file, default_flow_style=False, width=float('inf'), sort_keys=False)

        print(f'Summarized {report_id} and saved full_ summary to {report_summary_path} and the weightings to {report_weightings_path}, report line also added to {self.overall_summary_path}')
    
    def summarize_text(self, text) -> (str, str, str):
        max_attempts = 3
        attempts = 0
        while True:
            attempts += 1
            if attempts == max_attempts+1:
                return None
            numberOfResponses = 1
            responses = openAICaller.query(
                self.system_prompt,
                self.user_message_template(text),
                n=numberOfResponses,
                large_model=True,
                temp = 0)
            
            if responses == None:
                return None
            
            # Convert the responses into a list of lists
            if numberOfResponses == 1:
                responses = [responses]
            parsed_responses = [self.parse_weighting_response(response) for response in responses]
            parsed_responses = [response for response in parsed_responses if response is not None]

            # Make sure that the response has the right number of themes and with all the correct names
            for response in parsed_responses:                
                if not 0 <= (len(response) - self.theme_reader.get_num_themes()) <= 1 :
                    print(f"  WARNING: Response does not have the correct number of themes. Expected {self.theme_reader.get_num_themes()} but got {len(response)}.")
                    parsed_responses.remove(response)
                    continue
                
                for theme in response:
                    if theme['name'] not in self.theme_reader.get_theme_titles() + ["Other"]:
                        print(f"  WARNING: Response has a theme with an incorrect name. Expected one of {self.theme_reader.get_theme_names()} but got {theme['name']}")
                        parsed_responses.remove(response)
                        continue

            if len(parsed_responses) == 0:
                print(f"  WARNING: No valid responses. Retrying.")
                continue

            # Get the weightings from the repsonse in the same order as the themes
            weightings_dicts = [{theme['name']: theme['percentage'] for theme in response} for response in parsed_responses]
            weightings = [[weightings_dict[title] for title in self.theme_reader.get_theme_titles()] for weightings_dict in weightings_dicts]
            
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
    
    def parse_weighting_response(self, response):
        if response[:3] == '```':
            clean_response = response[7:-3]
        else :
            clean_response = response

        try:
            weighting_obj = yaml.safe_load(clean_response)
            return weighting_obj
        except yaml.YAMLError as exc:
            print(exc)
            return None

        

        


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
    
    def extract_section(self, section_str: str):
        base_regex_template = lambda section: fr"(( {section}) {{1,3}}(?![\s\S]*^{section}))|((^{section}) {{1,3}})(?![\w\s()]{{1,100}}\.{{2,}})"

        split_section = section_str.split(".")
        section = split_section[0]
        endRegex_nextSection = base_regex_template(fr"{int(section)+1}\.1\.?")
        startRegex = base_regex_template(fr"{int(section)}\.1\.?")
        endRegexs = [endRegex_nextSection]
        if len(split_section) > 1:
            paragraph = split_section[1]
            endRegex_nextParagraph = base_regex_template(fr"{section}\.{int(paragraph)+1}\.?")
            endRegexs.insert(0, endRegex_nextParagraph)
            startRegex = base_regex_template(fr"{section}\.{int(paragraph)}\.?")

        if len(split_section) > 2:
            sub_paragraph = split_section[2]
            endRegex_nextSubParagraph = base_regex_template(fr"{section}\.{paragraph}\.{int(sub_paragraph)+1}\.?")
            endRegexs.insert(0, endRegex_nextSubParagraph)
            startRegex = base_regex_template(fr"{section}\.{paragraph}\.{int(sub_paragraph)}\.?")
        
        # Get the entire string between the start and end regex
        # Start by looking for just the next subparagraph, then paragraph, then section
        startMatch = re.search(startRegex, self.report_text, re.MULTILINE)

        endMatch = None

        for endRegex in endRegexs:
            endMatch = re.search(endRegex, self.report_text, re.MULTILINE)
            if endMatch:
                break

        if startMatch == None or endMatch == None:
            return None

        if endMatch.end() < startMatch.end():
            print(f"Error: endMatch is before startMatch")
            print(f"  startMatch: {startMatch.match} \n  endMatch: {endMatch.match}")
            print(f"  Regexs: {startRegex} \n  {endRegex}")
            return None
        
        if startMatch and endMatch:
            section_text = self.report_text[startMatch.start():endMatch.end()]
            return section_text

        print(f"Error: could not find section")
        return None

