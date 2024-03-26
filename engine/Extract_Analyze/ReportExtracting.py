from engine.OpenAICaller import openAICaller

from engine.Extract_Analyze import OutputFolderReader

import yaml
import os
import regex as re


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
        matches = re.findall(pattern, self.report_text, re.DOTALL | re.IGNORECASE)


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
        base_regex_template = lambda section: fr"(?<!\.{{3,}} {{0,4}}\d{{1,3}} ?\s)((( {section}) {{1,3}}(?![\s\S]*^{section}))|((^{section}) {{1,3}}))(?![\S\s()]{{1,100}}\.{{2,}})"

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
            print(f"  startMatch: {startMatch[0]} \n  endMatch: {endMatch[0]}")
            print(f"  Regexs: {startRegex} \n  {endRegex}")
            return None

        if startMatch and endMatch:
            section_text = self.report_text[startMatch.start():endMatch.end()]
            return section_text

        print(f"Error: could not find section")
        return None
    
    def extract_safety_issues(self):
        """
        Extract safety issues from a report.
        """

        safety_issues = self.__extract_safety_issues_with_regex()
        exact_safety_issues = True

        if safety_issues == None:
            exact_safety_issues = False
            safety_issues = self.__extract_safety_issues_with_inference()

        # Add the quality of safety issue to the yaml
        if safety_issues == None:
            return None
        
        safety_issues_with_quality = [{"safety_issue": issue, "quality": "exact" if exact_safety_issues else "inferred"} for issue in safety_issues ]

        return safety_issues_with_quality
        
    def __extract_safety_issues_with_regex(self):
        """
        Search for safety issues using regex
        """
        safety_regex = r's ?a ?f ?e ?t ?y ? ?i ?s ?s ?u ?e ?s?'
        end_regex = r'([\s\S]*?)(?=(\d+\.(\d+\.)?(\d+)?)|(^ [A-Z]))'
        preamble_regex = r'([\s\S]{50})'
        postamble_regex = r'([\s\S]{300})'

        
        # Search for safety issues throughout the report
        safety_issues_regexes = [
            preamble_regex + r'(' + safety_regex + r' -' +  ')' + end_regex + postamble_regex,
            preamble_regex + r'(' + safety_regex + r': ' +  ')' + end_regex + postamble_regex
        ]
        safety_issues_regexes = [re.compile(regex, re.MULTILINE | re.IGNORECASE) for regex in safety_issues_regexes]

        safety_issue_matches = []
        # Only one of the regexes should match
        for regex in safety_issues_regexes:
            if len(safety_issue_matches) > 0 and regex.search(self.report_text):
                print("Error: multiple regexes matched")
                return None

            if len(safety_issue_matches) == 0 and regex.search(self.report_text):
                safety_issue_matches.extend(regex.findall(self.report_text))

        # Collapse the tuples into a string
        safety_issues_uncleaned = [''.join(match) for match in safety_issue_matches]

        ## Remove excess whitespace
        safety_issues_removed_whitespace = [issue.strip().replace("\n", " ") for issue in safety_issues_uncleaned]

        if len(safety_issues_removed_whitespace) == 0:
            print("  Could not find any safety issues using regex.")
            return None

        ## Clean up characters with llm
        clean_text = lambda text: openAICaller.query(
            """
I need some help extracting the safety issues from a section of text.

This text has been extracted from a pdf and then using regex this section was found. It contains text before the safety issue then the safety issue that starts with safety issue, follow by the some text after the safety issue. The complete safety issue will always be in the given text.

However I would like to get just as the safety issue without any of the random text (headers footers etc and white spaces) that is added by the pdf.

Please just return the cleaned version of the text. Without starting with Safety issue.
""",
            text,
            large_model=True,
            temp=0)

        safety_issues_cleaned = [clean_text(issue) for issue in safety_issues_removed_whitespace]

        return safety_issues_cleaned

    def __extract_safety_issues_with_inference(self):
        """
        Search for safety issues using inference from GPT 4 turbo.
        """

        important_text, pages_to_read = self.extract_important_text()

        if important_text == None:
            return None
        
        message = lambda text: f'''
{text}
        
=Instructions=

I want to know the safety issues which this investigation has found.

Can your response please be in yaml format.

- |
    bla bla bla
- |
    bla bla bla bla

There is no need to enclose the yaml in any tags.

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
'''
        
        response = openAICaller.query(
            """
You are going help me read a transport accident investigation report.

 I want you to please read the report and respond with the safety issues identified in the report.

Please only respond with safety issues that are quite clearly stated and/or implied.

It should be noted that the number of safety issues in a report has a minimum of 1 an 0.25 quantile of 1, median of 2, 0.75 quantile of 3 and a maximum of 13. You should try make your answers match this distribution and on average a report will have only 2 safety issues.

Remember the definitions give

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
            """,
            message(important_text),
            large_model=True,
            temp=0)
        
        if response[:7] == '"""yaml' or response[:7] == '```yaml':
            response = response[7:-3]
        
        try:
            safety_issues = yaml.safe_load(response)
        except yaml.YAMLError as exc:
            print(exc)
            print("  Assuming that there are no safety issues in the report.")
            return None
        
        return safety_issues


class ReportExtractingProcessor:

    def __init__(self, output_dir, report_dir_template, file_name_template, refresh):
        self.output_folder_reader = OutputFolderReader.OutputFolderReader()
        self.output_dir = output_dir
        self.report_dir_template = report_dir_template
        self.file_name_template = file_name_template
        self.refresh = refresh

    def _output_safety_issues(self, report_id, report_text):

        print("  Extracting safety issues from " + report_id)

        folder_dir = self.report_dir_template.replace(r'{{report_id}}', report_id)
        output_file = self.file_name_template.replace(r'{{report_id}}', report_id)
        output_path = os.path.join(self.output_dir, folder_dir, output_file)

        # Skip if the file already exists
        if os.path.exists(output_path) and not self.refresh:
            print(f"   {output_path} already exists")
            return

        safety_issues = ReportExtractor(report_text, report_id).extract_safety_issues()

        if safety_issues == None:
            print(f"  Could not extract safety issues from {report_id}")
            return
        
        print(f"   Found {len(safety_issues)} safety issues")

        with open(output_path, 'w') as f:
            yaml.safe_dump(safety_issues, f, default_flow_style=False, width=float('inf'), sort_keys=False)

    def extract_safety_issues_from_reports(self):
        self.output_folder_reader.process_reports(self._output_safety_issues)

        