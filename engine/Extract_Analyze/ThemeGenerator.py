import os
import yaml

from ..OpenAICaller import openAICaller
from . import OutputFolderReader
from .ReportExtracting import ReportExtractor
from . import Themes, ReferenceChecking

class ThemeGenerator:
    def __init__(self, output_folder, report_dir_template, report_theme_template):
        self.output_folder = output_folder
        self.open_ai_caller = openAICaller
        self.report_dir_template = report_dir_template
        self.report_theme_template = report_theme_template
        self.all_themes = ""
        self.output_folder_reader = OutputFolderReader.OutputFolderReader()

    def _get_theme_file_path(self, report_id):
        return os.path.join(self.output_folder,
                            self.report_dir_template.replace(r'{{report_id}}', report_id),
                            self.report_theme_template.replace(r'{{report_id}}', report_id))

    def generate_themes(self):
        print("Generating themes from reports with config:")
        print(f"  Output folder: {self.output_folder}")
        print(f"  Report directory template: {self.report_dir_template}")
        print(f"  Report theme template: {self.report_theme_template}")


        self.output_folder_reader.process_reports(self._get_theme)

        print(" Themes generated for each report")

        print(" Creating global themes")
        self.output_folder_reader.read_all_themes(self._read_themes)
        print("  All themes read")
            
        print("  Summarizing themes...")
        summarized_themes = self.open_ai_caller.query(
            "I am trying to find the themes present across a collection of about 50 marine accident investigation reports.\n\nI will give you 3-6 themes/causes summaries for each report. \n\nPlease read all of the summaries and provide 5-10 themes/causes that best cover all of the individual themes present.\n\nYour output should have a title and description paragraph (<= 50 words) for each general theme/cause discovered.\n\nNote that I want the output of this process to be consistent and repeatable. This means that I want your response to be as deterministic as possible."
            ,
            self.all_themes,
            large_model=True,
            temp = 0,
            n=4
        )

        summarized_themes_str = ""
        for i, theme in enumerate(summarized_themes, start = 1):
                summarized_themes_str += (f"Summary {i} of all the themes\n{theme}\n\n")

        print("  Getting average summary...")

        average_summary = self.open_ai_caller.query(
            "I am trying to find the themes present across a collection of about 50 marine accident investigation reports.\n\nI have three summaries of all of the themes.\n\nI wanted you to take the average of all of the summaries.\n\nYour output should have a title and description of each theme/cause.\n\nNote that I want this to be reproducible and deterministic as possible.",
            summarized_themes_str,
            large_model=True,
            temp = 0,
        )

        formatted_themes = self.open_ai_caller.query(
            "I will give you descriptions of themes and I want to you format them into yaml.\n\nJust output the yaml structure with no extra text.\n\nThe yaml layout should follow the structure seen below where the title and description is replaced. With as many theme elements as needed.\n\nthemes:\n   - title:  theme name one\n    description: \"Description of the first theme\"\n\n  - title:  theme name two\n    description: \"Description of the second theme\"\n\n  - title:  theme name three\n    description: \"Description of the third theme\"\n\n  - title:  theme name four\n    description: \"Description of the fourth theme\"\n\n  - title:  theme name five\n    description: \"Description of the fifth theme\"\n\n  - title:  theme name six\n    description: \"Description of the sixth theme\"\n ",
            average_summary,
            temp = 0)
        
        Themes.ThemeWriter().write_themes(formatted_themes)

        print(" Themes summaried and written to file")        


    def _get_theme(self, report_id, report_text):

        print(f" Generating themes for report {report_id}")

        important_text = ReportExtractor(report_text, report_id).extract_important_text()[0]

        if important_text is None:
            return
        
        system_message = """
You will be provided with a document delimited by triple quotes and a question. Your task is to answer the question using only the provided document and to cite the passage(s) of the document used to answer the question. There may be multiple citations needed. If the document does not contain the information needed to answer this question then simply write: "Insufficient information." If an answer to the question is provided, it must include quotes with citation.

You must follow these formats exactly.
For direct quotes there can only ever be one section mentioned:
"quote in here" (section.paragraph.subparagraph)
For indirect quotes there may be one section, multiple or a range: 
sentence in here (section.paragraph.subparagraph)
sentence in here (section.paragraph.subparagraph, section.paragraph.subparagraph, etc)
sentence in here (section.paragraph.subparagraph-section.paragraph.subparagraph)


Example quotes would be:
"it was a wednesday afternoon when the boat struck" (5.4)
It was both the lack of fresh paint and the old radar dish that caused this accident (4.5.2, 5.4.4)

Quotes should be weaved into your answer. 
"""
        user_message = f"""
'''
{important_text}
'''

Question:
Please provide me 3 - 6 safety themes that are most related to this accident.
For each theme provide a paragraph explaining what the theme is and reasoning as to why it is relevant to this accident. Provide evidence for your reasoning with inline quotes. More than 1 quote may be needed and direct quotes are preferable.

Please output your answer in yaml. There should be no opening or closing code block just straight yaml. The yaml format should have a name and explanation field (which uses a literal scalar block) for each safety theme.

----
Here are some definition

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

        report_themes_str = self.open_ai_caller.query(
            system_message,
            user_message,
            large_model=True,
            temp = 0
        )

        # with open(self._get_theme_file_path(report_id), "r") as f:
        #     report_themes_str = f.read()

        if report_themes_str is None:
            return

        try :
            report_themes = yaml.safe_load(report_themes_str)
        except yaml.YAMLError as exc:
            print(exc)
            print("  Error parsing yaml for themes")
            return self._get_theme(report_id, report_text)
        
        print(f"  Themes for {report_id} generated now validating references")

        referenceChecker = ReferenceChecking.ReferenceValidator(report_text)

        validated_themes_counter = 0
        updated_themes_counter = 0

        for theme in report_themes:
            result = referenceChecker.validate_references(theme['explanation'])

            if result is None:
                return self._get_theme(report_id, report_text)

            processed_text, num_references, num_updated_references = result
            updated_themes_counter += num_updated_references
            if isinstance(processed_text, str):
                theme['explanation'] = processed_text


            validated_themes_counter += num_references
            
        print(f"    {validated_themes_counter} references validated across {len(report_themes)} themes with {updated_themes_counter} themes updated")

        print(f"  References for {report_id} validated now writing to file")

        with open(self._get_theme_file_path(report_id), "w") as f:
            yaml.dump(report_themes, f, default_flow_style=False, width=float('inf'), sort_keys=False)
        

    def _read_themes(self, report_id, report_themes):
        theme = yaml.safe_load(report_themes)

        # convert theme object with name and explanation to a string
        theme_str = '\n\n'.join(f"{element['name']}\n{element['explanation']}" for element in theme)

        self.all_themes += (f"Themes for {report_id}: \n{theme_str}\n")

