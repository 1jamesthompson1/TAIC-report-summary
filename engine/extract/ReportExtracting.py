import concurrent.futures
import os

import pandas as pd
import regex as re
import yaml
from tqdm import tqdm

from engine.utils.AICaller import AICaller


class ReportExtractor:
    def __init__(self, report_text, report_id, headers="Empty"):
        self.report_text = report_text
        self.report_id = report_id
        self.headers = (
            self.create_hierarchy(headers.assign(Page=headers["Page"].replace("", "0")))
            if isinstance(headers, pd.DataFrame)
            else headers
        )

        self.max_important_text_len = (
            128_000 * 3
        )  # This account for the fact that a token is about 4 characters and at the moment the LLMs have limits of about 128,000 tokens

    def create_hierarchy(self, df):
        hierarchy = []
        df["Level_indent"] = df["Level"].apply(lambda x: "- - " * (x - 1))
        max_title_length = df.apply(
            lambda x: len(x["Title"]) + len(x["Level_indent"]), axis=1
        ).max()
        width = max(30, max_title_length + 10)
        for _, row in df.iterrows():
            title = f"{row['Level_indent']}{row['Title']}"
            if row["Page"]:
                page_indentation = "." * (width - len(title) - 2)
                title += f" {page_indentation} {row['Page']}"
            hierarchy.append(title)
        return "\n".join(hierarchy)

    def extract_important_text(self):
        # In cases where the content section doesnt exist or is not adequate we will try to read the entire report.
        pages = list(
            re.finditer(
                r"<< Page (\d+|[xvi]+) >>",
                self.report_text,
                re.MULTILINE | re.IGNORECASE,
            )
        )
        default_response = (
            (self.report_text, f"{pages[0].group(1)},{pages[-1].group(1)}")
            if len(self.report_text) < self.max_important_text_len
            else (None, None)
        )

        # Get the pages that should be read
        contents_sections = self.extract_contents_section()
        if contents_sections is None:
            return default_response

        pages_to_read = self.extract_pages_to_read(contents_sections)

        if pages_to_read is None:
            return default_response

        text = "\n".join(
            map(
                lambda x: x if x is not None else "",
                [
                    self.extract_text_between_page_numbers(
                        page_to_read[0], page_to_read[1]
                    )
                    if (len(page_to_read) == 2)
                    else self.extract_page(page_to_read[0])
                    for page_to_read in pages_to_read
                ],
            )
        )

        # Remove duplicate page numbers in the text, assuming that roman numerals wont be duped.
        # This is because the LLM can have issues with reading the section numbers as page numbers and end up with overlapping pages to read.
        page_numbers = list(re.finditer(r"<< Page (\d+) >>", text))
        if len(page_numbers) == 0:
            return text, pages_to_read
        first_page_number = int(page_numbers[0].group(1))
        last_page_number = int(page_numbers[-1].group(1))

        for page_number in range(first_page_number, last_page_number + 1):
            if text.count(f"<< Page {page_number} >>") > 1:
                # Delete all but first version of this page
                text = re.sub(
                    rf"(?<=<< Page {page_number} >>[\s\S]+)(<< Page {page_number} >>[\s\S]+?)(<< Page \d+ >>)",
                    r"\2",
                    text,
                    flags=re.MULTILINE | re.IGNORECASE,
                )

        return text, pages_to_read

    def extract_page(self, page_to_read):
        if page_to_read in ["0", 0]:
            first_page = re.search(
                r"<< Page \d+|[xvi]+ >>", self.report_text, re.MULTILINE | re.IGNORECASE
            )
            return self.report_text[: first_page.start()]

        page = re.search(
            rf"<< Page {page_to_read} >>[\s\S]+<< Page (\d+|[xvi]+) >>",
            self.report_text,
            re.MULTILINE | re.IGNORECASE,
        )
        if page is None:
            final_page = re.search(
                rf"<< Page {page_to_read} >>[\s\S]+",
                self.report_text,
                re.MULTILINE | re.IGNORECASE,
            )
            if final_page is None:
                return None
            return final_page.group(0)
        else:
            return page.group(0)

    def extract_text_between_page_numbers(self, page_number_1, page_number_2) -> str:
        # Create a regular expression pattern to match the page numbers and the text between them
        if page_number_1 == page_number_2:
            return self.extract_page(page_number_1)
        if page_number_1 != 0:
            starting_page_match = re.search(
                rf"<< Page {page_number_1} >>",
                self.report_text,
                re.MULTILINE | re.IGNORECASE,
            )
            if starting_page_match is None:
                print(
                    f" {self.report_id} No starting page number for text between pages {page_number_1} and {page_number_2}"
                )
                return None

            starting_index = starting_page_match.start()
        else:
            starting_index = 0

        ending_page_match = re.search(
            rf"<< Page {page_number_2} >>",
            self.report_text,
            re.MULTILINE | re.IGNORECASE,
        )

        if ending_page_match is None:
            print(
                f"  {self.report_id} No ending page number for text between pages {page_number_1} and {page_number_2}"
            )
            return None

        return self.report_text[starting_index : ending_page_match.end()]

    def extract_contents_section(self) -> str:
        startRegex = r"(contents?)([ \w]{0,30}.+)([\n\w\d\sāēīōūĀĒĪŌŪ]*)(.*[ \.]{5,})"
        endRegex = (
            r"^(.*(\.{5,}|(\. ){5,}).*[\dxvi]+.{0,5}?)|((\d+\.){1,3}\d+\.?.* \d+)$"
        )

        if not (isinstance(self.headers, str) or self.headers is None):
            raise ValueError("headers cannot be left to default value")

        endOfContentSection = len(self.report_text) / 4

        # Get the entire string between the start and end regex
        startMatch = re.search(startRegex, self.report_text, re.IGNORECASE)
        if startMatch:
            if startMatch.end() > endOfContentSection:
                startMatch = None
        endMatches = list(
            re.finditer(endRegex, self.report_text, re.MULTILINE | re.IGNORECASE)
        )
        if endMatches:
            endMatches = [
                endMatch
                for endMatch in endMatches
                if endMatch.start() < endOfContentSection
            ]

        if startMatch:
            if len(endMatches) == 0:
                print(f"Found a start {self.report_id} but no end: {startMatch}")
                return self.headers
            endMatches = [
                endMatch
                for endMatch in endMatches
                if endMatch.start() - startMatch.end() < 15_000
            ]
            if len(endMatches) == 0:
                print(
                    f"Found a start {self.report_id} but no end that isn't too far away: {startMatch}"
                )
                return self.headers
            endMatch = endMatches[-1]
        elif len(endMatches) > 1:
            endMatches = [
                endMatch
                for endMatch in endMatches
                if endMatch.start() - endMatches[0].end() < 15_000
            ]

            startMatch = endMatches[0]
            endMatch = endMatches[-1]
        else:
            if len(endMatches) > 0:
                print(f"Found an end {self.report_id} but no start: {endMatches[-1]}")
            return self.headers

        raw_content_section = self.report_text[startMatch.start() : endMatch.end()]

        cleaned_content_section = AICaller.query(
            system="""
You are a helpful assistant. You will just respond with the answer no need to explain.
Can you please format this table of contents? Please include in the format the section number (if it has one) the section title and section page number. Make sure to include all of the pages the the table of contents has even it they are roman numerals.

It should go like this:
[Section number*] - [Section title] [Page number]

*Section numbers are optional. They should only be included if they are present in the original table of contents.
You should not include the figures or tables section of the table of contents. However appendices should be included.

Example output
Executive summary i
1 - Introduction 1
2 - Narrative 2
3.0 - Analysis 4
3.1 - Introduction 4
3.2 - Why did the cylinder burst 6
3.2.1 - Bad construction 6
3.2.2 - Maintenance 8
3.3 - Emergency response 10
4.0 Findings 12
   - Important 12
   - Incidental 13
5.0 Safety actions 14

""",
            user=f"""
{raw_content_section}
""",
            temp=0,
            max_tokens=4_000,
            model="gpt-4o-mini",
        )

        cleaned_content_section = cleaned_content_section.replace("```", "").strip("\n")
        return cleaned_content_section

    def extract_pages_to_read(self, content_section) -> list:
        attempts_left = 5

        pages_to_read = None

        while attempts_left > 0:  # Repeat until the LLMs gives a valid response
            model_response = None
            try:
                # Get 5 responses and only includes pages that are in atleast 3 of the responses
                model_response = AICaller.query(
                    system="""
You are helping me read the content section of a report from a transport accident investigation.
The content section is either a text extraction from the pdf or a parsing of the pdf header links. Note that the content section may be malformed.
I am looking to find the section of the reports that will help me identify safety issues. I need to the page ranges I need to read.

The sections I want you to find:
     - Analysis
     - Findings (any section that mentions findings)
     - Executive Summary / Summary / Safety summary (normally at the start of the report, but it does not always exist)
     - Safety issues (any section the explicitly mentions safety issues) 

I want to know the page ranges of the sections you found in the report. Include the start page and end page, where the end page is the page number that the next section starts on. For sections you can't find just omit them from your response. If no sections were found just return "None".

Your response should only include the page numbers of the sections. For each section found put the starting and ending page numbers separate by a comma. Then separate each section with a space.

Example responses: 
"1,2 7,17"
"1 4,8 12,16"
"i,2 10,12"
"7,13 20"
""",
                    user=content_section,
                    model="gpt-4",
                    temp=0,
                )

                cleaned_response = model_response.strip(" '\"")

                if cleaned_response == "None":
                    return None

                sections = [page.strip() for page in cleaned_response.split(" ")]
                pages_to_read = [
                    tuple(
                        int(num)
                        if num.isdigit()
                        else (num if set(num).issubset(set("vixVXI")) else int(num))
                        for num in section.split(",")
                    )
                    for section in sections
                ]

                break
            except ValueError as e:
                print(
                    f"  Incorrect response '{model_response}' from model retrying, error: {e}'"
                )
                attempts_left -= 1

        if pages_to_read is None:
            return None

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

    def _extract_safety_issues_with_regex(self, important_text=None):
        """
        This function will use regex and search the text for any safety issues.
        It will not be used in the main engine pipeline but it useful for development purposes while we dont have a reliable inference extraction.
        """
        if important_text is None:
            raise Exception(
                "  No important text provided to extract safety issues from"
            )

        def safety_regex(x):
            return f"s ?a ?f ?e ?t ?y ? ?i ?s ?s ?u ?e ?s? {{0,3}}{x} {{0,3}}"

        end_regex = r"([\s\S]+?)(?=(?:\d+\.(?:\d+\.)?(?:\d+)?)|(?:s ?a ?f ?e ?t ?y ? ?i ?s ?s ?u ?e ?s?))"

        uncompiled_regexes = [
            "(" + safety_regex(sep) + end_regex + ")" for sep in ["-", ":"]
        ]

        safety_issue_regexes = [
            re.compile(regex, re.MULTILINE | re.IGNORECASE)
            for regex in uncompiled_regexes
        ]

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

Remember the definitions given

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

        def message(text):
            agency = self.report_id.split("_")[0]
            match agency:
                case "TSB":
                    instruction_core = """
I want to know the safety issues which this investigation has found. If the safety issues are not explicitly stated you will need to infer them. You need to respond with what safety issues this report has identified. Note that sometimes will not have any relevant safety issues. In this case you can respond with an empty list.

If no safety issues are stated explicitly, then you need to inferred them. These inferred safety issues are "inferred" safety issues."""
                case "TAIC":
                    instruction_core = """
I want to know the safety issues which this investigation has found.

For each safety issue you find I need to know what is the quality of this safety issue.
Some reports will have safety issues explicitly stated with something like "safety issue - ..." or "safety issue: ...", these are "exact" safety issues. Note that the text may have extra spaces or characters in it. Furthermore findings do not count as safety issues.

If no safety issues are stated explicitly, then you need to inferred them. These inferred safety issues are "inferred" safety issues.
"""
                case _:
                    raise ValueError(
                        f"{agency} is not a supported agency for safety issue extraction"
                    )

            return f"""
{text}

=Instructions=

{instruction_core}

Can your response please be in yaml format as shown below.

- safety_issue: |
    bla bla talking about this and that bla bla bla
  quality: exact
- safety_issue: |
    bla bla talking about this and that bla bla bla
  quality: exact

or it could be 

- safety_issue: |
    bla bla talking about this and that bla bla bla
  quality: inferred
- safety_issue: |
    bla bla talking about this and that bla bla bla
  quality: inferred


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
"""

        temp = 0
        while temp < 0.1:
            response = AICaller.query(
                system_message,
                message(self.important_text),
                model="gpt-4",
                temp=temp,
                max_tokens=9096,
            )

            if response is None:
                print("  Could not get safety issues from the report.")
                return None

            if response[:7] == '"""yaml' or response[:7] == "```yaml":
                response = response[7:-3]

            try:
                safety_issues = yaml.safe_load(response)
                safety_issues = [
                    {
                        "safety_issue": safety_issue["safety_issue"].strip(),
                        "quality": safety_issue["quality"],
                    }
                    for safety_issue in safety_issues
                ]
            except yaml.YAMLError as exc:
                print(exc)
                print(
                    '  Problem with formatting, trying again with slightly higher temp\nResponse was is \n"""\n{response}\n"""'
                )
                temp += 0.01
                continue

            if not isinstance(safety_issues, list):
                print(
                    f'  Response was not a yaml list. It was instead {type(safety_issues)}.\n\nWhich is \n"""\n{response}\n"""'
                )
                print(f"  Safety issues are:\n{safety_issues}")

                temp += 0.01
                continue

            return safety_issues

        print("  Could not extract safety issues with inference")
        return None


class ReportSectionExtractor(ReportExtractor):
    def __init__(self, report_text, report_id, headers="Empty"):
        super().__init__(report_text, report_id, headers)

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
                return (
                    split_section[0]
                    + "."
                    + split_section[1]
                    + "."
                    + str(int(split_section[2]) - 1)
                )

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

        def base_regex_template(section):
            return f"((( {section}(?! ?(m )|(metre))) {{1,3}}(?![\\s\\S]*^{section} ))|((^{section}) {{1,3}}))(?![\\S\\s()]{{1,100}}\\.{{2,}})"

        split_section = section_str.split(".")
        section = split_section[0]
        endRegex_nextSection = base_regex_template(rf"{int(section)+1}\.1\.?")
        startRegex = base_regex_template(rf"{int(section)}\.1\.?")
        endRegexs = [endRegex_nextSection]
        if len(split_section) > 1:
            paragraph = split_section[1]
            # Added to prevent single unfindable section ruining search
            endRegex_nextnextSubSection = base_regex_template(
                rf"{section}\.{int(paragraph)+2}\.?"
            )
            endRegexs.insert(0, endRegex_nextnextSubSection)
            endRegex_nextSubSection = base_regex_template(
                rf"{section}\.{int(paragraph)+1}\.?"
            )
            endRegexs.insert(0, endRegex_nextSubSection)
            startRegex = base_regex_template(rf"{section}\.{int(paragraph)}\.?")

        if len(split_section) > 2:
            sub_paragraph = split_section[2]
            endRegex_nextnextParagraph = base_regex_template(
                rf"{section}\.{paragraph}\.{int(sub_paragraph)+2}\.?"
            )
            endRegexs.insert(0, endRegex_nextnextParagraph)
            endRegex_nextParagraph = base_regex_template(
                rf"{section}\.{paragraph}\.{int(sub_paragraph)+1}\.?"
            )
            endRegexs.insert(0, endRegex_nextParagraph)
            startRegex = base_regex_template(
                rf"{section}\.{paragraph}\.{int(sub_paragraph)}\.?"
            )

        return (startRegex, endRegexs)

    def _get_section_search_bounds(self, section_str: str, endRegexs):
        ##
        # Figure out when to start the search from.
        ##

        # At the start of the rpoert there can be factual information that can has numbers formatted like sections
        page_regex = r"<< Page 1 >>"
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
            previous_section_regex, _ = self._get_section_start_end_regexs(
                previous_section_str
            )
            previous_section_match = re.search(
                previous_section_regex,
                self.report_text,
                re.MULTILINE,
                pos=first_page_pos - 1,
            )
            if previous_section_match:
                previous_section_pos = previous_section_match.end()
            else:
                attempt -= 1

        # Adding the 10 so that it still captures the first seciton of a report.
        start_pos = max(max(previous_section_pos, first_page_pos) - 10, 0)

        ##
        # Figure out when to end the search
        ##
        # This wil be done by looking for the next big section as that will give an upper bound

        next_section_match = re.search(
            endRegexs[-1], self.report_text, re.MULTILINE, pos=start_pos
        )

        end_pos = (
            next_section_match.end() + 10
            if next_section_match
            else len(self.report_text)
        )

        return (start_pos, end_pos)

    def extract_section(self, section_str: str, useLLM=True):
        """
        This function extract a numbered section from the report.
        You need to give it a string like 5, 5.1, 5.1.1, etc. It can struggle with the last or second to last section in the report. In this case it utilses AI
        """

        startRegex, endRegexs = self._get_section_start_end_regexs(section_str)

        start_pos, end_pos = self._get_section_search_bounds(section_str, endRegexs)

        ##
        # Search the report for start and end of the section
        ##

        startMatch = re.search(
            startRegex,
            self.report_text,
            re.MULTILINE | re.IGNORECASE,
            pos=start_pos,
            endpos=end_pos,
        )

        endMatch = None

        if startMatch:
            endRegexMatches = [
                re.search(
                    endRegex,
                    self.report_text,
                    re.MULTILINE | re.IGNORECASE,
                    pos=start_pos,
                    endpos=end_pos,
                )
                for endRegex in endRegexs
            ]

            endMatch = min(
                endRegexMatches, key=lambda x: x.start() if x else len(self.report_text)
            )

        if startMatch is None or endMatch is None:
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
            section_text = self.report_text[startMatch.start() : endMatch.start()]

            return section_text.strip()

        # print(f"Error: could not find section")
        return None

    def __extract_section_using_LLM(self, section):
        """
        A helper function to extract_section that will read the content section and find the page numbers then extract it from there.
        """

        content_section = self.extract_contents_section()

        pages = AICaller.query(
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
            temp=0,
        )

        if pages == "None":
            print(
                f"  Failed to find the section using the LLM, it responded with '{pages}'. The search was for section '{section}'"
            )
            return None

        print(" Found the section using the LLM" + pages)

        pages_to_read = [int(num) for num in pages.split(",")]

        # Make the array every page between first and last
        pages_to_read = list(range(pages_to_read[0], pages_to_read[-1] + 1))

        # Retrieve that actual text for the page numbers.
        section_text = self.extract_text_between_page_numbers(
            pages_to_read[0], pages_to_read[-1]
        )

        return section_text

    def split_into_paragraphs(self):
        raw_splits = [
            paragraph.strip()
            for paragraph in re.split(r"\n *\n", self.report_text)
            if len(paragraph.strip()) > 0
        ]

        splits_df = pd.DataFrame(raw_splits, columns=["section_text"])

        splits_df["page"] = splits_df["section_text"].map(
            lambda x: re.match(r"<< Page (\d+|[xvi]+) >>", x).group(1)
            if re.match(r"<< Page (\d+|[xvi]+) >>", x)
            else None
        )
        splits_df.ffill(inplace=True)
        splits_df.replace({pd.NA: "0", None: "0"}, inplace=True)
        splits_df["paragraph_num"] = splits_df.groupby(["page"]).cumcount()
        splits_df["section"] = (
            "p" + splits_df["page"] + "." + splits_df["paragraph_num"].astype(str)
        )

        splits_df = splits_df[
            splits_df["section_text"].map(
                lambda x: len(re.sub(r"<< Page (\d+|[xvi]+) >>", "", x).strip())
            )
            > 8
        ]

        return splits_df[["section", "section_text"]]


class RecommendationsExtractor(ReportSectionExtractor):
    def __init__(self, report_text, report_id, headers):
        super().__init__(report_text, report_id, headers)

    def extract_recommendations(self):
        """
        Extract recommendations from a report.
        """

        recommendation_section = self._extract_recommendation_section_text()

        if recommendation_section is None:
            print(
                "  Could not get recommendations as there was no recommendation section"
            )
            return None

        extracted_recommendations = self._extract_recommendations_from_text(
            recommendation_section
        )

        return extracted_recommendations

    def _extract_recommendations_from_text(self, text):
        """
        This will look for the recommendations that are present in the given text
        """

        agency = self.report_id.split("_")[0]
        if agency == "ATSB":
            agency_text = "ATSB (Australia Transport Safety Bureau)"
        else:
            raise NotImplementedError(
                f"{agency} is not currently supported yet for recommendation extraction."
            )

        response = AICaller.query(
            f"""
You are going help me read and parse a transport accident investigation report.
This is the section of a report that may or may not contain recommendations. I want to have a list of all of the distinct recommendations that were made. It is important that the recommendations are copied verbatim.

Recommendations are made to those who can make the changes needed to address safety issues identified during an inquiry.  I only want recommendations that were made by the {agency_text}.

If no appropriate recommendations were made then return "None".

Can your response please be in yaml format.

- recommendation: |
    bla bla stating the recommendation that was made.
  recommendation_id: 
  recipient: organization who it was directed at.
  recommendation_context: |
    Extra context around why the recommendation was made. Potentially teh safety issue that prompted the recommendation.
  made: date the recommendation was made. Leave empty if not known.
- recommendation: |
    bla bla stating the recommendation that was made.
  recommendation_id: 
  recipient: organization who it was directed at.
  recommendation_context: |
    Extra context around why the recommendation was made. Potentially teh safety issue that prompted the recommendation.
  made: date the recommendation was made. Leave empty if not known.

There is no need to enclose the yaml in any tags.
""",
            text,
            model="gpt-4",
            temp=0,
        )

        response = response.strip().strip(" `").replace("yaml", "")

        if response == "None":
            print("  No recommendations were found")
            return None

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
        Extract the text of the recommendation section from the report. This is usually in the safety action section.
        """
        content_section = self.extract_contents_section()

        if content_section is None:
            print(
                "  Without content section the recommendation section cannot be found"
            )
            return None

        recommendation_pages_to_read = self._get_recommendation_pages(content_section)

        if recommendation_pages_to_read is None:
            print("  Could not find recommendation pages")
            return None

        recommendation_text = self.extract_text_between_page_numbers(
            recommendation_pages_to_read[0], recommendation_pages_to_read[-1]
        )

        return recommendation_text

    def _get_recommendation_pages(self, content_section):
        """
        This will get the pages that need to be read to get the recommendations section
        """
        model_response = AICaller.query(
            system="""
You are helping me read the content sections of a report.

Can you please find the starting and end sections of the recommendations or safety actions section. The end of it is the same as the start of the next section. Note that generally the page number will be on the right.
If neither of these sections exist just return "None". A single page should just be [25,25]

Your response should just be 2 numbers for example: 23,26.
""",
            user=content_section,
            model="gpt-4",
            temp=0,
        )

        if model_response.strip() == "None":
            return None

        parsed_model_response = model_response.strip().split(",")

        if len(parsed_model_response) != 2:
            print(f"  Error: Could not parse the pages to read: {model_response}")
            return None

        try:
            return [int(x) for x in parsed_model_response]
        except ValueError:
            print(f"  Error: Could not parse the pages to read: {model_response}")
            return None


class ReportExtractingProcessor:
    """
    This is the class that interacts with the report extracting classes. It however is the ones that actually connects to the folder structure.
    """

    def __init__(self, report_text_df_path: str, refresh=False):
        if not os.path.exists(report_text_df_path):
            raise ValueError(f"{report_text_df_path} does not exist")
        self.report_text_df = pd.read_pickle(report_text_df_path)

        self.refresh = refresh

        self.important_text_df = None

    def __get_safety_issues(self, important_text, report_id, report_text):
        if important_text is None:
            raise ValueError("important_text cannot be None")

        safety_issues = SafetyIssueExtractor(
            report_text, report_id, important_text
        ).extract_safety_issues()

        if safety_issues is None:
            return f" Could not extract safety issues from {report_id}"

        return safety_issues

    def extract_safety_issues_from_reports(
        self,
        important_text_df_path,
        report_titles_df_path,
        atsb_safety_issues_df_path,
        output_file,
    ):
        ## -- Safety issue datasets -- ##
        # Get previously extracted safety issues
        if os.path.exists(output_file) and not self.refresh:
            all_safety_issues_df = pd.read_pickle(output_file)
        else:
            all_safety_issues_df = pd.DataFrame(columns=["report_id", "safety_issues"])

        # Get atsb safety_issue_dataset
        if os.path.exists(atsb_safety_issues_df_path):
            atsb_safety_issues_df = pd.read_pickle(atsb_safety_issues_df_path)
        else:
            raise ValueError(f"{atsb_safety_issues_df_path} does not exist")

        all_safety_issues_df = pd.concat(
            [all_safety_issues_df, atsb_safety_issues_df]
        ).drop_duplicates("report_id", keep="first")

        ## -- metadata datasets -- ##
        # Get the important text
        if os.path.exists(important_text_df_path):
            important_text_df = pd.read_pickle(important_text_df_path)[
                ["report_id", "important_text", "pages_read"]
            ]
        else:
            raise ValueError(f"{important_text_df_path} does not exist")

        # Get the report titles
        if os.path.exists(report_titles_df_path):
            report_titles_df = pd.read_pickle(report_titles_df_path)
        else:
            raise ValueError(f"{report_titles_df_path} does not exist")

        ## -- Merging datasets together -- ##
        merged_df = (
            self.report_text_df.merge(important_text_df, on="report_id", how="outer")
            .merge(report_titles_df, on="report_id", how="outer")
            .reset_index(drop=True)
        )

        print(
            f"There are {len(merged_df)} total reports. There are {len(merged_df[merged_df['important_text'].isna()])} reports without important text and {len(all_safety_issues_df)} reports with safety issues. "
        )
        print(
            f"Removing {len(merged_df[merged_df['investigation_type'] == 'short'])} short reports."
        )
        merged_df = merged_df[merged_df["investigation_type"] != "short"]

        print(
            f"There are {len(merged_df[merged_df['report_id'].isin(all_safety_issues_df['report_id'])])} reports that already have safety issues"
        )
        new_safety_issues = []
        for (
            _,
            report_id,
            report_text,
            important_text,
            investigation_type,
            pages_read,
        ) in (
            pbar := tqdm(
                list(
                    merged_df[
                        [
                            "report_id",
                            "text",
                            "important_text",
                            "investigation_type",
                            "pages_read",
                        ]
                    ].itertuples()
                )
            )
        ):
            agency = report_id.split("_")[0]
            year = int(report_id.split("_")[2])
            important_text_len = len(important_text)
            # Confirm that this report should be included and have its safety issues extracted
            match agency:
                case "ATSB":
                    continue  # For now we wont include any atsb reports
                    ## TODO: Figure out a better way to include pre 2008 atsb reports.
                    if year >= 2008 or (
                        investigation_type == "unknown"
                        and (
                            important_text_len < 40_000 and isinstance(pages_read, str)
                        )
                    ):
                        continue
                case "TSB":
                    if investigation_type == "unknown" and (
                        important_text_len < 40_000 and isinstance(pages_read, str)
                    ):
                        continue
                case "TAIC":
                    pass
                case _:
                    raise ValueError(f"Unknown agency: {agency} for report {report_id}")

            pbar.set_description(f"Extracting safety issues from {report_id}")
            if report_id in all_safety_issues_df["report_id"].tolist():
                tqdm.write("Report already has safety issues")
                continue

            if pd.isna(important_text):
                pbar.write(f"  No important text found for {report_id}")
                continue

            safety_issues_list = self.__get_safety_issues(
                important_text, report_id, report_text
            )
            if isinstance(safety_issues_list, str):
                pbar.write(safety_issues_list + " therefore skipping report.")
                continue
            safety_issues_df = pd.DataFrame(safety_issues_list)
            safety_issues_df["safety_issue_id"] = [
                report_id + "_" + str(i) for i in safety_issues_df.index
            ]

            new_safety_issues.append(
                {"report_id": report_id, "safety_issues": safety_issues_df}
            )
            if len(new_safety_issues) > 50:
                all_safety_issues_df = pd.concat(
                    [all_safety_issues_df, pd.DataFrame(new_safety_issues)]
                ).reset_index(drop=True)
                all_safety_issues_df.to_pickle(output_file)
                pbar.write(
                    f" Saving {len(new_safety_issues)} safety issues to bring it to a total of {len(all_safety_issues_df)} of safety issues."
                )
                new_safety_issues = []

        all_safety_issues_df = pd.concat(
            [all_safety_issues_df, pd.DataFrame(new_safety_issues)]
        ).reset_index(drop=True)
        all_safety_issues_df.to_pickle(output_file)

    def extract_important_text_from_reports(self, output_file):
        if os.path.exists(output_file):
            important_text_df = pd.read_pickle(output_file)
        else:
            important_text_df = pd.DataFrame(
                columns=["report_id", "important_text", "pages_read"]
            )

        def process_report(report_id, report_text, important_text_df, header):
            if report_id in important_text_df["report_id"].values:
                return None
            important_text, pages_read = ReportExtractor(
                report_text, report_id, header
            ).extract_important_text()
            if important_text is None:
                return None  # Indicates failure
            return report_id, important_text, pages_read

        def save_result(result, important_text_df, output_file):
            report_id, important_text, pages_read = result
            if important_text is None:
                print(f"  Could not extract important text from {report_id}")
            else:
                important_text_df.loc[len(important_text_df)] = [
                    report_id,
                    important_text,
                    pages_read,
                ]
                important_text_df.to_pickle(output_file)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for _, report_id, report_text, _, header in (
                pbar := tqdm(list(self.report_text_df.itertuples()))
            ):
                pbar.set_description(f"Extracting important text from {report_id}")
                futures.append(
                    executor.submit(
                        process_report,
                        report_id,
                        report_text,
                        important_text_df,
                        header,
                    )
                )

            for future in tqdm(
                concurrent.futures.as_completed(futures), total=len(futures)
            ):
                result = future.result()
                if result is not None:
                    save_result(result, important_text_df, output_file)

        important_text_df.to_pickle(output_file)

    def __extract_sections(
        num_sections, all_potential_sections, report_text, debug=False, headers=None
    ):
        get_parts_regex = r"(((\d{1,2}).\d{1,2}).\d{1,2})"

        extractor = ReportSectionExtractor(report_text, num_sections, headers)

        sections = []

        for section in all_potential_sections:
            if debug:
                print(
                    f"Looking at section {re.search(get_parts_regex, section[0][0]).group(3)}"
                )

            subsection_missing_count = 0
            for sub_section in section:
                sub_section_str = re.search(get_parts_regex, sub_section[0]).group(2)
                if debug:
                    print(f" Looking at subsection {sub_section_str}")

                paragraph_missing_count = 0

                paragraphs = []
                for paragraph in sub_section:
                    if debug:
                        print(f"  Looking for paragraph {paragraph}")

                    paragraph_text = extractor.extract_section(paragraph, useLLM=False)

                    if paragraph_text is None and (
                        paragraph_missing_count > 0 or paragraph[-1] == "1"
                    ):
                        break
                    elif paragraph_text is None:
                        paragraph_missing_count += 1
                        continue

                    paragraphs.append(
                        {"section": paragraph, "section_text": paragraph_text}
                    )

                if len(paragraphs) == 0:
                    if debug:
                        print(" No paragraphs found ")
                    sub_section_text = extractor.extract_section(
                        sub_section_str, useLLM=False
                    )

                    if sub_section_text is None and subsection_missing_count > 0:
                        if debug:
                            print(" No subsection found")
                        break
                    elif sub_section_text is None:
                        subsection_missing_count += 1
                        continue

                    sections.append(
                        {"section": sub_section_str, "section_text": sub_section_text}
                    )
                else:
                    sections.extend(paragraphs)

        df = pd.DataFrame(sections, columns=["section", "section_text"])

        # Check if this worked. Otherwise extract with paragraph splitting.

        if len(df) > 4 and df["section_text"].map(len).mean() < 2_000:
            return df

        else:
            return extractor.split_into_paragraphs()

    def extract_sections_from_text(self, num_sections, output_file_path):
        sections = list(map(str, range(1, 15)))

        subsections = [
            [section + "." + str(subsection) for subsection in range(1, 100)]
            for section in sections
        ]

        paragraphs = [
            [
                [subsection + "." + str(paragraph) for paragraph in range(1, 100)]
                for subsection in section
            ]
            for section in subsections
        ]

        if os.path.exists(output_file_path):
            report_sections_df = pd.read_pickle(output_file_path)
        else:
            report_sections_df = pd.DataFrame(columns=["report_id", "sections"])

        new_reports = []

        for _, report_id, report_text, _, headers in (
            pbar := tqdm(list(self.report_text_df.itertuples()))
        ):
            pbar.set_description(f"Extracting sections from {report_id}")
            if report_id in report_sections_df["report_id"].values:
                continue

            sections_df = ReportExtractingProcessor.__extract_sections(
                num_sections, paragraphs, report_text, debug=False, headers=headers
            )
            sections_df["report_id"] = report_id

            new_reports.append({"report_id": report_id, "sections": sections_df})
            if len(new_reports) > 50:
                report_sections_df = pd.concat(
                    [report_sections_df, pd.DataFrame(new_reports)], ignore_index=True
                )
                report_sections_df.to_pickle(output_file_path)
                pbar.write(
                    f" Saving {len(new_reports)} new extracted reports to bring it to a total of {len(report_sections_df)} of extracted reports."
                )
                new_reports = []

        report_sections_df = pd.concat(
            [report_sections_df, pd.DataFrame(new_reports)], ignore_index=True
        )
        report_sections_df.to_pickle(output_file_path)

    def extract_recommendations(
        self, output_path, tsb_recommendations_path, taic_recommendations_path
    ):
        if os.path.exists(output_path) and not self.refresh:
            recommendations_df = pd.read_pickle(output_path)
        else:
            recommendations_df = pd.DataFrame(columns=["report_id", "recommendations"])
        if os.path.exists(tsb_recommendations_path):
            tsb_recommendations_df = pd.read_pickle(tsb_recommendations_path)
        else:
            tsb_recommendations_df = pd.DataFrame(
                columns=["report_id", "recommendations"]
            )

        if os.path.exists(taic_recommendations_path):
            taic_recommendations_df = pd.read_pickle(taic_recommendations_path)
        else:
            taic_recommendations_df = pd.DataFrame(
                columns=["report_id", "recommendations"]
            )

        recommendations_df = pd.concat(
            [recommendations_df, tsb_recommendations_df, taic_recommendations_df],
            ignore_index=True,
        ).drop_duplicates("report_id")

        atsb_reports = self.report_text_df[
            self.report_text_df["report_id"].map(lambda x: x.split("_")[0]) == "ATSB"
        ]
        new_reports = recommendations_df.merge(
            atsb_reports, how="right", on="report_id"
        )
        new_reports = new_reports[new_reports["recommendations"].isna()]

        print(recommendations_df)
        for _, report_id, report_text, headers in (
            pbar := tqdm(new_reports[["report_id", "text", "headers"]].itertuples())
        ):
            pbar.set_description(f"Extracting sections from {report_id}")
            if report_id in recommendations_df["report_id"].values:
                continue

            recommendations = RecommendationsExtractor(
                report_text, report_id, headers
            ).extract_recommendations()

            recommendations_df.loc[len(recommendations_df)] = [
                report_id,
                recommendations,
            ]

        recommendations_df.to_pickle(output_path)
