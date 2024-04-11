import os
import yaml
import csv

from ..OpenAICaller import openAICaller
import pandas as pd

from .Themes import ThemeReader
from .OutputFolderReader import OutputFolderReader
from .ReportExtracting import ReportExtractor
from . import ReferenceChecking

from .. import Modes

class ReportSummarizer:
    def __init__(self, output_config, use_predefined_themes = False, modes = Modes.all_modes, discard_old= True):
        self.output_folder = output_config.get("folder_name")
        self.theme_reader = ThemeReader(None, use_predefined_themes, modes)
        self.report_reader = OutputFolderReader()
        self.open_ai_caller = openAICaller
        self.modes = modes
        self.discard_old = discard_old

        self.overall_summary_path = os.path.join(self.output_folder, output_config.get("summary_file_name"))

        self.report_dir = output_config.get("reports").get("folder_name")
        self.report_summary_file_name = output_config.get("reports").get("full_summary_file_name")
        self.report_weightings_file_name = output_config.get("reports").get("weightings_file_name")

        self.system_prompt = '''
You will be provided with a document delimited by triple quotes and a question. Your task is to answer the question using only the provided document and to cite the passage(s) of the document used to answer the question. There may be multiple citations needed. If the document does not contain the information needed to answer this question then simply write: "Insufficient information." If an answer to the question is provided, it must include quotes with citation. Each sentence with a new claim should be cited. It is best to have inline quotes.

You must follow these reference conventions exactly. See examples for implementation
For direct quotes there can only ever be one section mentioned:
"quote in here" (section.paragraph.subparagraph)
For indirect quotes there may be one section, multiple or a range: 
sentence in here (section.paragraph.subparagraph)
sentence in here (section.paragraph.subparagraph, section.paragraph.subparagraph, etc)
sentence in here (section.paragraph.subparagraph-section.paragraph.subparagraph)

Example quotes would be:
"it was a wednesday afternoon when the boat struck" (3.4)
It was both the lack of fresh paint and the old radar dish that caused this accident (4.5.2, 5.4.4)
The lack of consitant training different language dialects caused a breakdown in communication (3.9-3.12)

Under no circumstances should a citation look like these:
Of the "22 people onboard only 2 had experience at sea" ("3.2")
The hazards had been indentified in the past and ignored ("4.5")

'''


    def summarize_reports(self):
        if not os.path.exists(self.output_folder):
            print("WARNING: Output folder and hence extracted text does not exist. Reports cannot be summarized.")
            return
        
        start = ["ReportID", "PagesRead"]

        update_theme_str = lambda ending: map(lambda title: f"{title}_{ending}",self.theme_reader.get_theme_titles(self.modes))

        themes_name = update_theme_str("weighting")
        themes_names_explanation = update_theme_str("explanation")
        themes_names_std = update_theme_str("std")

        zipped_themes_titles = [item for sublist in zip(themes_name, themes_names_explanation, themes_names_std) for item in sublist]

        zipped_themes_titles.extend(["Other_weighting", "Other_explanation", "Other_std"])

        ending = ["Complete", "ErrorMessage"]

        if os.path.exists(self.overall_summary_path) and not self.discard_old:
            # Check to make sure that it has the same first row
            with open(self.overall_summary_path, 'r', newline="") as summary_file:
                reader = csv.reader(summary_file)
                first_row = next(reader)
                if first_row  != start + zipped_themes_titles + ending:
                    print("WARNING: The overall summary file does not have the same first row as expected. It will be overwritten.")
                    self.discard_old == True

        if self.discard_old or not os.path.exists(self.overall_summary_path):
            with open(self.overall_summary_path, 'w', newline="") as summary_file:
                writer = csv.writer(summary_file, quotechar='"', quoting=csv.QUOTE_ALL)
                writer.writerow(start + zipped_themes_titles + ending)
        
        # Prepare system prompt
        self.user_message_template = lambda report_text, modes: f"""
'''
{report_text}
'''

Question:
Please take the provided safety themes below and assign a weighting to each of them. These weightings should be how much each safety theme contributed to the accident. All the weightings should add up to no more than 100. There should be a section for all the provided themes even if the weightings is zero. If the cause of the accident cannot be attributed to one of the predfined safety themes you can add a "Other" safety theme with an explanation of what this theme is. Your reponse should have exactly either {self.theme_reader.get_num_themes()} or {self.theme_reader.get_num_themes()+1} themes in total.

Please remember to keep the quote format as specified.

Please output your answer as straight yaml without using code blocks.
The yaml format should have a name (must be verbatim), precentage and explanation field (which uses a literal scalar block) for each safety theme.
Your yaml ouput should look like this:
 - name: |-
    safety theme name
  percentage: xy
  explanation: |
    multi line explanation goes here>
    with evidence and "quotes" etc.


----
=Here is a summary of the {self.theme_reader.get_num_themes()} safety themes=
{self.theme_reader.get_theme_description_str(modes)}

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

        self.report_reader.process_reports(self.summarize_report, self.modes)      

    def summarize_report(self, report_id, report_text):
        print(f'Summarizing {report_id}')

        report_folder_path = os.path.join(self.output_folder,
                                                self.report_dir.replace(r'{{report_id}}', report_id))
        
        report_weightings_path = os.path.join(report_folder_path,
                                            self.report_weightings_file_name.replace(r'{{report_id}}', report_id))

        ## Check if it has already been summarized
        if not self.discard_old and os.path.exists(report_weightings_path):
            print(f"  Skipping {report_id} as it has already been summarized")
            return


        # Get the pages that should be read
        text_to_be_summarized, pages_read = ReportExtractor(report_text, report_id).extract_important_text(self.output_folder)
        if text_to_be_summarized == None:
            print(f'  Could not extract text to be summarized from {report_id}')
            summary_str = report_id + "," + "Error" + "," + "N/A" + ",false" + ",Could not extract text to summarize report with" + "\n"
            return

        summary = self.summarize_text(text_to_be_summarized, Modes.get_report_mode_from_id(report_id))
        
        if (summary == None):
            print(f'  Could not summarize {report_id}')
            summary_final = [report_id, str(pages_read).replace(",", " ")]
            summary_final.extend([pd.NA] * (self.theme_reader.get_num_themes()+1)*(3))
            summary_final.extend(["false", "Could not summarize report"])
        else:
            weightings, full_summary_parsed = summary # unpack tuple response

            summary_final = [report_id, str(pages_read).replace(",", " ")]
            summary_final.extend(weightings)
            summary_final.extend(["true", "N/A"])

            report_summary_path = os.path.join(report_folder_path,
                                           self.report_summary_file_name.replace(r'{{report_id}}', report_id))
        
            with open(report_summary_path, 'w', encoding='utf-8') as summary_file:
                yaml.dump(full_summary_parsed, summary_file, default_flow_style=False, width=float('inf'), sort_keys=False)
            print(f" Saved full summary to {report_summary_path}")


        with open(report_weightings_path, 'w', encoding='utf-8') as summary_file:
            writer = csv.writer(summary_file, quoting=csv.QUOTE_ALL)
            writer.writerow(summary_final)

        with open(self.overall_summary_path, 'a', encoding='utf-8') as summary_file:
            writer = csv.writer(summary_file, quoting=csv.QUOTE_ALL)
            writer.writerow(summary_final)

        print(f'Finished summarizing {report_id}, it was a {"success" if summary_final[-2] == "true" else "failure"}. The summary has been saved to {report_weightings_path} and the overall summary has been saved to {self.overall_summary_path}')
    
    def summarize_text(self, text, mode) -> (str, str, str):
        max_attempts = 3
        attempts = 0
        while True:
            attempts += 1
            if attempts == max_attempts+1:
                return None
            numberOfResponses = 3
            responses = openAICaller.query(
                self.system_prompt,
                self.user_message_template(text, mode),
                n=numberOfResponses,
                model="gpt-4",
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
                number_expected_themes = self.theme_reader.get_num_themes(mode)
                if not 0 <= (len(response) - number_expected_themes) <= 1 :
                    print(f"  WARNING: Response does not have the correct number of themes. Expected {number_expected_themes} but got {len(response)}.")
                    print(f"   Response was: {response}")
                    parsed_responses.remove(response)

                    continue
                
                for theme in response:
                    potential_theme_names = self.theme_reader.get_theme_titles(mode) + ["Other"]
                    if theme['name'].strip().strip("\n") not in potential_theme_names:
                        print(f"  WARNING: Response has a theme with an incorrect name. Expected one of {potential_theme_names} but got '{theme['name']}'")
                        parsed_responses.remove(response)
                        continue

            if len(parsed_responses) == 0:
                print(f"  WARNING: No valid responses. Retrying.")
                continue

            # Get the weightings from the repsonse in the same order as the themes
            # Add in weightings as NA for themes that are not applicable
            weightings = list()
            explanations = list()
            for response in parsed_responses:
                response_weighting = list()
                response_explanation = list()
                weighting_dict = {theme['name']: theme['percentage'] for theme in response}
                explantion_dict = {theme['name']: theme['explanation'] for theme in response}
                for theme in self.theme_reader.get_theme_titles() + ["Other"]:
                    weighting = weighting_dict.get(theme)
                    response_weighting.append(weighting if weighting is not None else pd.NA)
                    explanation = explantion_dict.get(theme)
                    response_explanation.append(explanation if explanation is not None else pd.NA)
                weightings.append(response_weighting)
                explanations.append(response_explanation)

            
            weightings = pd.DataFrame(weightings)

            # Remove all rows that dont add up to 100
            weightings = weightings[weightings.sum(axis=1).eq(100)]

            if weightings.shape[0] == 0:
                print(f"  WARNING: No valid responses with a sum of 100 retrying.")
                continue

            # Calculate the average of each column
            averages = weightings.mean()

            # Calculate the sum of the averages, ignoring NA values
            sum_averages = averages.sum()

            # Scale the averages to sum to 100, leaving NA values as they are
            scaled_averages = averages.apply(lambda x: round((x * 100) / sum_averages, 3) if pd.notnull(x) else pd.NA)

            # Check references
            referenceCheckor = ReferenceChecking.ReferenceValidator(text, True)
            invalid_reference = False
            for theme in parsed_responses[0]:
                result = referenceCheckor.validate_references(theme['explanation'])
                if result is None:
                    print(f"   No refrences in this theme: {theme['name']} to validate.")
                    continue
                elif isinstance(result, str):
                    invalid_reference = True
                    break

                processed_text, _, _ = result
                if isinstance(processed_text, str):
                    theme['explanation'] = processed_text
            if invalid_reference:
                print(f"  WARNING: Invalid reference in response. Retrying.")
                continue

            # Calculate the standard deviation of the weightings
            weighting_std = list(weightings.std(axis=0))
            weighting_std = [round(std, 3) if pd.notnull(std) else pd.NA for std in weighting_std]

            if weightings.shape[0] < numberOfResponses-2:
                print(f"  Did not get enough valid responses. Retrying. \n  Responses were: {responses}")
                continue

            zipped_data = [str(item) for sublist in zip(scaled_averages, explanations[0], weighting_std) for item in sublist]

            print("  The weightings are: " + str(list(scaled_averages)))
            break

        return zipped_data, parsed_responses[0] # Currently assuming that there is only going to be one response.
    
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

