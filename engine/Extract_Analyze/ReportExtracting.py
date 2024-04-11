from engine.OpenAICaller import openAICaller

import yaml
import os
import regex as re


class ReportExtractor:
    def __init__(self, report_text, report_id):
        self.report_text = report_text
        self.report_id = report_id

    def extract_important_text(self):
                 
        print(f" Generating important text for {self.report_id}")

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
    
        # Try and read the pages. If it fails try and read to the next page three times. Then give up.
        text = self.extract_text_between_page_numbers(pages_to_read[0], pages_to_read[-1])

        return text, pages_to_read

    def extract_text_between_page_numbers(self, page_number_1, page_number_2) -> str:
        # Create a regular expression pattern to match the page numbers and the text between them

        page = lambda num: f"<< Page {num} >>"
        middle_pages = "[\s\S]*"
        pattern = page(page_number_1) + middle_pages + page(page_number_2)

        matches = re.findall(pattern, self.report_text, re.MULTILINE)

        if len(matches) > 1:
            print(f"  Found multiple matches for text between pages {page_number_1} and {page_number_2}")
            return None
        
        if len(matches) == 1:
            return matches[0]

        print(f"  Could not find text between pages {page_number_1} and {page_number_2}")

        if len(re.findall(page(page_number_1), self.report_text, re.MULTILINE)) == 0:
            if page_number_1 < 2:
                print("     giving up search for text between pages")
                return None
            return self.extract_text_between_page_numbers(page_number_1-1, page_number_2)
        
        if len(re.findall(page(page_number_2), self.report_text, re.MULTILINE)) == 0:
            if page_number_2 > 100:
                print("     giving up search for text between pages")
                return None
            return self.extract_text_between_page_numbers(page_number_1, page_number_2+1)
        
        if page_number_1 > 1 and page_number_2 < 100:
            return self.extract_text_between_page_numbers(page_number_1-1, page_number_2+1)

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

        attempts_left = 5

        pages_to_read = None

        while attempts_left > 0: # Repeat until the LLMs gives a valid response
            try:
                # Get 5 responses and only includes pages that are in atleast 3 of the responses
                model_response = openAICaller.query(
                        """
    You are helping me read the content section of a report.

    I am only interested in two sections "Analysis" and "Findings".
    Can you please tell me which page Analysis starts on and which page the Findings section ends on.

    Your response is only a list of integers. No words are allowed in your response. e.g '12,45' or '10,23'. If you cant find the analysis and findings section just return 'None'
    """,
                        content_section,
                        model="gpt-4",
                        temp = 0)

                if model_response == "None":
                    return None

                pages_to_read = [int(num) for num in model_response.split(",")]

                break
            except ValueError:
                print(f"  Incorrect response from model retrying. \n  Response was: '{model_response}'")
                attempts_left -= 1

        return pages_to_read

    def extract_section(self, section_str: str):
        """
        This function extract a numbered section from the report.
        You need to give it a string like 5, 5.1, 5.1.1, etc. It can struggle with the last or second to last section in the report. In this case it utilses AI
        """
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
            print("Warning: could not find section")
            print(f"  startMatch: {startMatch} with regex {startRegex} \n  endMatch: {endMatch} with regex {endRegex}")
            print("  Attempting to extract section using page numbers")
            return self.__extract_section_using_LLM(section_str)

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
    
    def __extract_section_using_LLM(self, section):
        """
        A helper function to extract_section that will read the content section and find the page numbers then extract it from there.
        """

        content_section = self.extract_contents_section()

        pages = openAICaller.query(
            """
            You are helping me read a content section.

I will send you a content section and a section and you will return the pages with which that section will cover.

Your response is only a list of integers. No words are allowed in your response. e.g '12,45' or '10,23'. If you cant find the section number given then just return "None".
            """,
            f"""
'''
{content_section}
'''

The section number I am looking for is {section}
            """,
            model="gpt-4",
            temp = 0)
        
        if pages == "None":
            print(f"  Failed to find the section using the LLM, it responded with '{pages}'. The search was for section '{section}'")            
            return None
        
        print(" Found the section using the LLM" + pages)

        pages_to_read = [int(num) for num in pages.split(",")]

        # Make the array every page between first and last
        pages_to_read = list(range(pages_to_read[0], pages_to_read[-1] + 1))

        # Retrieve that actual text for the page numbers.
        section_text = self.extract_text_between_page_numbers(pages_to_read[0], pages_to_read[-1])

        return section_text

class SafetyIssueExtractor(ReportExtractor):
    def __init__(self, report_text, report_id, important_text):
        super().__init__(report_text, report_id)

        if important_text is None:
            raise ValueError("important_text cannot be None")
        self.important_text = important_text
        
    def extract_safety_issues(self):
        """
        Extract safety issues from a report.
        """
        # This abstraction allows the development of various extraction techniques whether it be regex or inferences.

        safety_issues = self._extract_safety_issues_with_inference()
        
        return safety_issues
        
    def _extract_safety_issues_with_inference(self):
        """
        Search for safety issues using inference from GPT 4 turbo.
        """        
        message = lambda text: f'''
{text}
        
=Instructions=

I want to know the safety issues which this investigation has found.

For each safety issue you find I need to know what is the quality of this safety issue.
Some reports will have safety issues explicitly stated with something like "safety issue - ..." or "safety issue: ...", these are "exact" safety issues. Note that the text may have extra spaces or characters in it. Furthermore findings do not count as safety issues.

However if no safety issues are stated explicitly, then you need to inferred them. These inferred safety issues are "inferred" safety issues.


Can your response please be in yaml format as shown below.

- safety_issue: |
    bla bla talking about this and that bla bla bla
  quality: exact
- safety_issue: |
    bla bla talking about this and that bla bla bla
  quality: exact


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

Please only respond with safety issues that are quite clearly stated ("inferred" safety issues) or implied ("inferred" safety issues) in the report. Each report will only contain one type of safety issue.

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
            message(self.important_text),
            model="gpt-4",
            temp=0)

        if response == None:
            print("  Could not get safety issues from the report.")
            return None

        if response[:7] == '"""yaml' or response[:7] == '```yaml':
            response = response[7:-3]
        
        try:
            safety_issues = yaml.safe_load(response)
        except yaml.YAMLError as exc:
            print(exc)
            print("  Assuming that there are no safety issues in the report.")
            return None
        
        return safety_issues


class RecommendationsExtractor(ReportExtractor):
    def __init__(self, report_text, report_id):
        super().__init__(report_text, report_id)

    def extract_recommendations(self):
        """
        Extract recommendations from a report.
        """

        recommendation_section = self._extract_recommendation_section_text()

        if recommendation_section == None:
            print("  Could not get recommendations as there was no recommendation section")
            return None

        # Parse the  recommendation section and get a list

        message = lambda text: f'''
"""        
{text}
"""

=Instructions=

This is the recommendation section of the report.  I want to have a list of all of the distinct recommendations that were made. It is important that the recommendations are copied verbatim

Can your response please be in yaml format.

- |
    bla bla bla
- |
    bla bla bla bla

There is no need to enclose the yaml in any tags.
'''
        response = openAICaller.query(
            """
You are going help me read and parse a transport accident investigation report.

You will be given a section and a question and you will need to respond in the format that is specified.
""",
            message(recommendation_section),
            model="gpt-4",
            temp=0)

        if response[:7] == '"""yaml' or response[:7] == '```yaml':
            response = response[7:-3]
        
        try:
            recommendations = yaml.safe_load(response)
        except yaml.YAMLError as exc:
            print(exc)
            print("  Assuming that there are no recommendations in the report.")
            print(f"  The response was: {response}")
            return None

        return recommendations

    def _extract_recommendation_section_text(self):
        """
        Extract the text of the recommendation section from the report.
        """
        content_section = self.extract_contents_section()
        
        if content_section == None:
            print(f'  Without content section the recommendation section cannot be found')
            return None
        
        add_whitespace = lambda text: r"\s{0,2}".join(text)

        search_regex = rf'(\d{{1,3}})\s{{0,2}}\.?\s{{0,2}}(({add_whitespace("safety")})?\s?{add_whitespace("recommendations")}?).*?(\d{{1,3}})'

        recommendation_matches = [*re.finditer(search_regex, content_section, re.IGNORECASE)]

        # Can't find the recommendation section and assuming that there are no recommendations
        if len(recommendation_matches) == 0:
            print(f'  Could not find the recommendation section')
            return None
        
        # The regex matches multiple times so will assume it is the last one as any earlier matches are probably from the executive summary
        if len(recommendation_matches) > 1:
            print(f'  Found multiple recommendation sections, assuming the last one is the correct one')
            recommendation_match = recommendation_matches[-1]
        else:
            recommendation_match = recommendation_matches[0]
        
        print(f'  Found the recommendation section it was {recommendation_match.group(1)}')
        
        recommendation_section = self.extract_section(recommendation_match.group(1))

        return  recommendation_section

        


class ReportExtractingProcessor:
    """
    This is the class that interacts with the report extracting classes. It however is the ones that actually connects to the folder structure.
    """

    def __init__(self, output_dir, reports_config, refresh = False):
        self.output_dir = output_dir
        self.reports_config = reports_config
        self.report_dir_template = reports_config.get("folder_name")
        self.refresh = refresh

    def __output_safety_issues(self, report_id, report_text):

        print(" Extracting safety issues from " + report_id)

        folder_dir = self.report_dir_template.replace(r'{{report_id}}', report_id)
        output_file = self.reports_config.get('safety_issues').replace(r'{{report_id}}', report_id)
        output_path = os.path.join(self.output_dir, folder_dir, output_file)

        # Skip if the file already exists
        if os.path.exists(output_path) and not self.refresh:
            print(f"   {output_path} already exists")
            return

        important_text = self.get_important_text(report_id)
        if important_text is None:
            print(f"  Could not extract important text from {report_id}")
            return

        safety_issues = SafetyIssueExtractor(report_text, report_id, important_text).extract_safety_issues()

        if safety_issues == None:
            print(f" Could not extract safety issues from {report_id}")
            return
        
        print(f"   Found {len(safety_issues)} safety issues")

        with open(output_path, 'w') as f:
            yaml.safe_dump(safety_issues, f, default_flow_style=False, width=float('inf'), sort_keys=False)

    def extract_safety_issues_from_reports(self, output_folder_reader = None):
        if output_folder_reader == None:
            raise Exception("  No output folder reader provided so safety issue extraction cannot happen")
        
        output_folder_reader.process_reports(self.__output_safety_issues)

    def get_important_text(self, report_id, with_pages_read = False):
        """
        This is a wrapper ground the extract_important_text function. This adds the support for reading and writing to the folder structure.
        """
        print(f"  Getting important text... for {report_id}")
        folder_dir = self.report_dir_template.replace(r'{{report_id}}', report_id)
        output_file = self.reports_config.get('important_text_file_name').replace(r'{{report_id}}', report_id)
        output_path = os.path.join(self.output_dir, folder_dir, output_file)
        
        if os.path.exists(output_path):
            with open(output_path, 'r') as f:
                yaml_obj = yaml.safe_load(f)
                return yaml_obj['text']
            
        print(f"  {output_path} does not exist, extracting important text from report text")

        # Getting the text file
        text_file_path = os.path.join(self.output_dir, folder_dir, self.reports_config.get('text_file_name').replace(r'{{report_id}}', report_id))
        if not os.path.exists(text_file_path):
            print(f"  {text_file_path} does not exist")
            return None
        with open(text_file_path, 'r') as f:
            report_text = f.read()
        
        text, pages_to_read = ReportExtractor(report_text, report_id).extract_important_text()

        print(f"  Saving the text to file with len {len(text)if text != None else 0}")
        with open(output_path, "w") as f:
            yaml.dump({
                "text": text,
                "pages_read": pages_to_read
            }, f)

        if with_pages_read:
            return text, pages_to_read
        return text


        