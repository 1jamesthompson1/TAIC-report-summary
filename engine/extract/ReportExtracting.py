from engine.utils.OpenAICaller import openAICaller

import yaml
import os
import regex as re
import pandas as pd
from tqdm import tqdm

class ReportExtractor:
    def __init__(self, report_text, report_id):
        self.report_text = report_text
        self.report_id = report_id

    def extract_important_text(self):
        # Get the pages that should be read
        contents_sections = self.extract_contents_section()
        if contents_sections == None:
            return None, None

        pages_to_read = self.extract_pages_to_read(contents_sections)

        if pages_to_read == None:
            return None, None

        # Try and read the pages. If it fails try and read to the next page three times. Then give up.
        text = self.extract_text_between_page_numbers(pages_to_read[0], pages_to_read[-1])

        return text, pages_to_read

    def extract_text_between_page_numbers(self, page_number_1, page_number_2) -> str:
        # Create a regular expression pattern to match the page numbers and the text between them

        page = lambda num: f"<< Page {num} >>"
        middle_pages = r"[\s\S]*"
        pattern = page(page_number_1) + middle_pages + page(page_number_2)

        matches = re.findall(pattern, self.report_text, re.MULTILINE | re.IGNORECASE)

        if len(matches) > 1:
            print(f"  Found multiple matches for text between pages {page_number_1} and {page_number_2}")
            return None
        
        if len(matches) == 1:
            return matches[0]

        if len(re.findall(page(page_number_1), self.report_text, re.MULTILINE)) == 0:
            if page_number_1 < 2:
                return None
            return self.extract_text_between_page_numbers(page_number_1-1, page_number_2)
        
        if len(re.findall(page(page_number_2), self.report_text, re.MULTILINE)) == 0:
            if page_number_2 > 100:
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

    def _extract_safety_issues_with_regex(self, important_text = None):
        """
        This function will use regex and search the text for any safety issues.
        It will not be used in the main engine pipeline but it useful for development purposes while we dont have a reliable inference extraction.
        """
        if important_text == None:
            raise Exception("  No important text provided to extract safety issues from")


        safety_regex = lambda x: fr's ?a ?f ?e ?t ?y ? ?i ?s ?s ?u ?e ?s? {{0,3}}{x} {{0,3}}'
        end_regex = r'([\s\S]+?)(?=(?:\d+\.(?:\d+\.)?(?:\d+)?)|(?:s ?a ?f ?e ?t ?y ? ?i ?s ?s ?u ?e ?s?))'

        uncompiled_regexes = ["(" + safety_regex(sep) + end_regex + ")" for sep in ["-", ":"]]

        safety_issue_regexes = [re.compile(regex , re.MULTILINE | re.IGNORECASE) for regex in uncompiled_regexes]

        safety_issues_from_report = []

        matches = [regex.findall(important_text) for regex in safety_issue_regexes]

        # Choose one of the matches that has the most matches
        matches = max(matches, key=lambda x: len(x))

        for full_match, safety_issue_match in matches:
            safety_issues_from_report.append(safety_issue_match)

        return safety_issues_from_report

        
    def _extract_safety_issues_with_inference(self):
        """
        Search for safety issues using inference from GPT 4 turbo.
        """        

        system_message = """
You are going help me read a transport accident investigation report.

I want you to please read the report and respond with the safety issues identified in the report.

Please only respond with safety issues that are quite clearly stated ("exact" safety issues) or implied ("inferred" safety issues) in the report. Each report will only contain one type of safety issue.

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
"""

        message = lambda text: f'''
{text}
        
=Instructions=

I want to know the safety issues which this investigation has found.

For each safety issue you find I need to know what is the quality of this safety issue.
Some reports will have safety issues explicitly stated with something like "safety issue - ..." or "safety issue: ...", these are "exact" safety issues. Note that the text may have extra spaces or characters in it. Furthermore findings do not count as safety issues.

If no safety issues are stated explicitly, then you need to inferred them. These inferred safety issues are "inferred" safety issues.


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
        
        temp = 0
        while temp < 0.1:
            response = openAICaller.query(
                system_message,
                message(self.important_text),
                model="gpt-4",
                temp=temp)

            if response == None:
                print("  Could not get safety issues from the report.")
                return None

            if response[:7] == '"""yaml' or response[:7] == '```yaml':
                response = response[7:-3]
            
            try:
                safety_issues = yaml.safe_load(response)
            except yaml.YAMLError as exc:
                print(exc)
                print('  Problem with formatting, trying again with slightly higher temp\nResponse was is \n"""\n{response}\n"""')
                temp += 0.01
                continue

            if not isinstance(safety_issues, list):
                print(f'  Response was not a yaml list. It was instead {type(safety_issues)}.\n\nWhich is \n"""\n{response}\n"""')
                print(f'  Safety issues are:\n{safety_issues}')

                temp+=0.01
                continue

            return safety_issues
        
        print("  Could not extract safety issues with inference")
        return None

        
class ReportSectionExtractor(ReportExtractor):
    def __init__(self, report_text, report_id):
        super().__init__(report_text, report_id)

    def _get_previous_section(self, section_str: str):
        """
        This function will get the previous section name from a section name.
        You need to give it a string like 5, 5.1, 5.1.1, etc.
        5.3.2 -> 5.3.1, 3.1 -> 2 etc

        """
        split_section = section_str.split(".")

        if len(split_section) == 3:
            if int(split_section[2]) == 1:
                split_section = [split_section[0], split_section[1]]
            else:    
                return split_section[0] + "." + split_section[1] + "." + str(int(split_section[2]) - 1)

        if len(split_section) == 2:
            if int(split_section[1]) == 1:
                split_section = [split_section[0]]
            else:
                return split_section[0] + "." + str(int(split_section[1]) - 1)
        
        if len(split_section) == 1:
            if int(split_section[0]) == 1:
                return split_section[0]
            return str(int(split_section[0]) - 1)

    def _get_section_start_end_regexs(self, section_str: str):
        """
        This function will get the start and end regex for a section
        You need to give it a string like 5, 5.1, 5.1.1, etc.
        It will return a tuple of (start_regex, [end_regexs])

        note that the end_regexs will have extras incrase the next section is missing.
        """
        base_regex_template = lambda section: fr"((( {section}(?! ?(m )|(metre))) {{1,3}}(?![\s\S]*^{section} ))|((^{section}) {{1,3}}))(?![\S\s()]{{1,100}}\.{{2,}})"

        split_section = section_str.split(".")
        section = split_section[0]
        endRegex_nextSection = base_regex_template(fr"{int(section)+1}\.1\.?")
        startRegex = base_regex_template(fr"{int(section)}\.1\.?")
        endRegexs = [endRegex_nextSection]
        if len(split_section) > 1:
            paragraph = split_section[1]
            # Added to prevent single unfindable section ruining search
            endRegex_nextnextSubSection = base_regex_template(fr"{section}\.{int(paragraph)+2}\.?")
            endRegexs.insert(0, endRegex_nextnextSubSection)
            endRegex_nextSubSection = base_regex_template(fr"{section}\.{int(paragraph)+1}\.?")
            endRegexs.insert(0, endRegex_nextSubSection)
            startRegex = base_regex_template(fr"{section}\.{int(paragraph)}\.?")

        if len(split_section) > 2:
            sub_paragraph = split_section[2]
            endRegex_nextnextParagraph = base_regex_template(fr"{section}\.{paragraph}\.{int(sub_paragraph)+2}\.?")
            endRegexs.insert(0, endRegex_nextnextParagraph)
            endRegex_nextParagraph = base_regex_template(fr"{section}\.{paragraph}\.{int(sub_paragraph)+1}\.?")
            endRegexs.insert(0, endRegex_nextParagraph)
            startRegex = base_regex_template(fr"{section}\.{paragraph}\.{int(sub_paragraph)}\.?")

        return (startRegex, endRegexs)
    
    def _get_section_search_bounds(self, section_str: str, endRegexs):
        ## 
        # Figure out when to start the search from.
        ##
        
        # At the start of the rpoert there can be factual information that can has numbers formatted like sections
        page_regex = r'<< Page 1 >>'
        page_regex_match = re.search(page_regex, self.report_text)
        if page_regex_match:
            first_page_pos = page_regex_match.end()
        else:
            first_page_pos = 0

        # As there can be other random numbers in the report or references to latter sections this rules out anything before hand.
        # Note that it relies on the assumption that the prevous few sections exists and are findable.
        previous_section_pos = 0
        attempt = 3
        previous_section_str = section_str
        while previous_section_pos == 0 and attempt > 0:
            previous_section_str = self._get_previous_section(previous_section_str)
            previous_section_regex, _ = self._get_section_start_end_regexs(previous_section_str)
            previous_section_match = re.search(previous_section_regex, self.report_text, re.MULTILINE, pos = first_page_pos-1)
            if previous_section_match:
                previous_section_pos = previous_section_match.end()
            else:
                attempt -= 1

        # Adding the 10 so that it still captures the first seciton of a report.
        start_pos = max(max(previous_section_pos, first_page_pos)-10, 0)

        ##
        # Figure out when to end the search
        ##
        # This wil be done by looking for the next big section as that will give an upper bound

        next_section_match = re.search(endRegexs[-1], self.report_text, re.MULTILINE, pos = start_pos)

        end_pos = next_section_match.end() + 10 if next_section_match else len(self.report_text)

        return (start_pos, end_pos)

    def extract_section(self, section_str: str, useLLM = True):
        """
        This function extract a numbered section from the report.
        You need to give it a string like 5, 5.1, 5.1.1, etc. It can struggle with the last or second to last section in the report. In this case it utilses AI
        """

        startRegex, endRegexs = self._get_section_start_end_regexs(section_str)

        start_pos, end_pos = self._get_section_search_bounds(section_str, endRegexs)

        ##
        # Search the report for start and end of the section
        ##

        startMatch = re.search(startRegex, self.report_text, re.MULTILINE | re.IGNORECASE, pos = start_pos, endpos= end_pos)

        endMatch = None

        if startMatch : 

            endRegexMatches = [
                re.search(endRegex, self.report_text, re.MULTILINE | re.IGNORECASE, pos = start_pos, endpos= end_pos)
                for endRegex in
                endRegexs
            ]

            endMatch = min(endRegexMatches, key = lambda x: x.start() if x else len(self.report_text))

        if startMatch == None or endMatch == None :
            # print("Warning: could not find section")
            # print(f"  startMatch: {startMatch} with regex {startRegex} \n  endMatch: {endMatch} with regex {endRegex}")
     
            if useLLM:
                print("  Attempting to extract section using page numbers and LLMs")
                return self.__extract_section_using_LLM(section_str)
            else:
                return None

        if endMatch.end() < startMatch.end():
            # print(f"Error: endMatch is before startMatch")
            # print(f"  startMatch: {startMatch[0]} \n  endMatch: {endMatch[0]}")
            # print(f"  Regexs: {startRegex} \n  {endRegex}")
            return None
        
        # if endMatch.end() - startMatch.end() > 16_000:
        #     # print(f"Error: section is too long")
        #     return None

        if startMatch and endMatch:
            section_text = self.report_text[startMatch.start():endMatch.start()]


            return section_text.strip()

        # print(f"Error: could not find section")
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


class RecommendationsExtractor(ReportSectionExtractor):
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

    def __init__(self, report_text_df_path: str, refresh = False):
        if not os.path.exists(report_text_df_path):
            raise ValueError(f"{report_text_df_path} does not exist")
        self.report_text_df = pd.read_pickle(report_text_df_path)

        self.refresh = refresh

        self.important_text_df = None

    def __get_safety_issues(self, important_text, report_id, report_text):

        if important_text is None:
            raise ValueError("important_text cannot be None")

        safety_issues = SafetyIssueExtractor(report_text, report_id, important_text).extract_safety_issues()

        if safety_issues == None:
            return f" Could not extract safety issues from {report_id}"

        return safety_issues

    def extract_safety_issues_from_reports(self, important_text_df_path, output_file):

        # Get previously extracted safety issues
        if os.path.exists(output_file) and not self.refresh:
            all_safety_issues_df = pd.read_pickle(output_file)
        else:
            all_safety_issues_df = pd.DataFrame(columns = ['report_id', 'safety_issues'])

        new_safety_issues = []

        if os.path.exists(important_text_df_path):
            important_text_df = pd.read_pickle(important_text_df_path)
        else:
            raise ValueError(f"{important_text_df_path} does not exist")
        
        merged_df = self.report_text_df.merge(important_text_df, on='report_id', how='outer')

        print(merged_df)

        for _, report_id, report_text, important_text, _ in (pbar := tqdm(list(merged_df.itertuples()))):
            pbar.set_description(f"Extracting safety issues from {report_id}")
            if report_id in all_safety_issues_df['report_id'].values: 
                continue

            if pd.isna(important_text):
                pbar.write(f"  No important text found for {report_id}")
                continue

            safety_issues_list = self.__get_safety_issues(important_text, report_id, report_text)
            if isinstance(safety_issues_list, str):
                pbar.write(safety_issues_list + " therefore skipping report.")
                continue
            safety_issues_df = pd.DataFrame(safety_issues_list)
            safety_issues_df['safety_issue_id'] = [report_id + "_" + str(i) for i in safety_issues_df.index]

            new_safety_issues.append({
                'report_id': report_id,
                'safety_issues': safety_issues_df
            })
            if len(new_safety_issues) > 50:
                all_safety_issues_df = pd.concat([all_safety_issues_df, pd.DataFrame(new_safety_issues)])
                all_safety_issues_df.to_pickle(output_file)
                pbar.write(f" Saving {len(new_safety_issues)} safety issues to bring it to a total of {len(all_safety_issues_df)} of safety issues.")
                new_safety_issues = []

        all_safety_issues_df = pd.concat([all_safety_issues_df, pd.DataFrame(new_safety_issues)])
        all_safety_issues_df.to_pickle(output_file)
    
    def extract_important_text_from_reports(self, output_file):
        if os.path.exists(output_file):
            important_text_df = pd.read_pickle(output_file)
        else:
            important_text_df = pd.DataFrame(columns=['report_id', 'important_text', 'pages_read'])

        for _, report_id, report_text in (pbar := tqdm(list(self.report_text_df.itertuples()))):
            pbar.set_description(f"Extracting important text from {report_id}")
            if report_id in important_text_df['report_id'].values:
                continue
            important_text, pages_read = ReportExtractor(report_text, report_id).extract_important_text()
            if important_text == None:
                pbar.write(f"  Could not extract important text from {report_id}")
                continue
            important_text_df.loc[len(important_text_df)] = [report_id, important_text, pages_read]
            important_text_df.to_pickle(output_file)

        important_text_df.to_pickle(output_file)

    def __extract_sections(num_sections, all_potential_sections, report_text, debug = False):
        get_parts_regex = r'(((\d{1,2}).\d{1,2}).\d{1,2})'
        
        extractor = ReportSectionExtractor(report_text, num_sections)

        sections = []

        for section in all_potential_sections:
            if debug: print(f"Looking at section {re.search(get_parts_regex, section[0][0]).group(3)}")

            subsection_missing_count = 0
            for sub_section in section:
                sub_section_str = re.search(get_parts_regex, sub_section[0]).group(2)
                if debug: print(f" Looking at subsection {sub_section_str}")

                paragraph_missing_count = 0

                paragraphs = []
                for paragraph in sub_section:
                    if debug: print(f"  Looking for paragraph {paragraph}")

                    paragraph_text = extractor.extract_section(paragraph, useLLM = False)

                    if paragraph_text is None and (paragraph_missing_count > 0 or paragraph[-1] == '1'):
                        break
                    elif paragraph_text is None:
                        paragraph_missing_count += 1
                        continue

                    paragraphs.append({'section': paragraph, 'section_text': paragraph_text})

                if len(paragraphs) == 0:
                    if debug: print(f" No paragraphs found ")
                    sub_section_text = extractor.extract_section(sub_section_str, useLLM = False)

                    if sub_section_text is None and subsection_missing_count > 0:
                        if debug: print(f" No subsection found")
                        break
                    elif sub_section_text is None:
                        subsection_missing_count += 1
                        continue

                    sections.append({'section': sub_section_str, 'section_text': sub_section_text})
                else:
                    sections.extend(paragraphs)

        df = pd.DataFrame(sections)

        return df

    def extract_sections_from_text(self, num_sections, output_file_path):
        sections = list(map(str, range(1,15)))

        subsections = [
            [
                section + '.' + str(subsection)
                for subsection in
                range(1,100)
            ]
            for section in 
            sections
        ]

        paragraphs = [
            [
                [
                    subsection + '.' + str(paragraph)
                    for paragraph in
                    range(1,100)
                ]
                for subsection in
                section
            ]
            for section in 
            subsections
        ]

        if os.path.exists(output_file_path):
            report_sections_df = pd.read_pickle(output_file_path)
        else:
            report_sections_df = pd.DataFrame(columns=['report_id', 'sections'])

        new_reports = []

        for _, report_id, report_text in (pbar := tqdm(list(self.report_text_df.itertuples()))):
            pbar.set_description(f"Extracting sections from {report_id}")
            if report_id in report_sections_df['report_id'].values:
                continue

            sections_df = ReportExtractingProcessor.__extract_sections(num_sections, paragraphs, report_text, debug = False)
            sections_df['report_id'] = report_id

            new_reports.append({
                'report_id': report_id,
                'sections': sections_df
            })
            if len(new_reports) > 50:
                report_sections_df = pd.concat([report_sections_df, pd.DataFrame(new_reports)], ignore_index=True)
                report_sections_df.to_pickle(output_file_path)
                pbar.write(f" Saving {len(new_reports)} new extracted reports to bring it to a total of {len(report_sections_df)} of extracted reports.")
                new_reports = []

        report_sections_df = pd.concat([report_sections_df, pd.DataFrame(new_reports)], ignore_index=True)
        report_sections_df.to_pickle(output_file_path)
