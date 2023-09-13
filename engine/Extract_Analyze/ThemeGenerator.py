import os

from ..OpenAICaller import openAICaller
from . import OutputFolderReader
from .Summarizer import ReportExtractor
from . import Themes

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
        print("Generating themes from reports...")

        self.output_folder_reader.process_reports(self._get_theme)

        print("Themes generated from reports")

        print("Reading all themes and summarizing...")
        self.output_folder_reader.read_all_themes(self._read_themes)
        print("All themes read")
            
        print("Summarizing themes...")
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
            gpt4 = True,
            temp = 0,
        )

        formatted_themes = self.open_ai_caller.query(
            "I will give you descriptions of themes and I want to you format them into yaml.\n\nJust output the yaml structure with no extra text.\n\nThe yaml layout should follow the structure seen below where the title and description is replaced. With as many theme elements as needed.\n\nthemes:\n   - title:  theme name one\n    description: \"Description of the first theme\"\n\n  - title:  theme name two\n    description: \"Description of the second theme\"\n\n  - title:  theme name three\n    description: \"Description of the third theme\"\n\n  - title:  theme name four\n    description: \"Description of the fourth theme\"\n\n  - title:  theme name five\n    description: \"Description of the fifth theme\"\n\n  - title:  theme name six\n    description: \"Description of the sixth theme\"\n ",
            average_summary,
            temp = 0)
        
        Themes.ThemeWriter().write_themes(formatted_themes)

        print("Themes summaried and written to file")        


    def _get_theme(self, report_id, report_text):

        print(f"Generating themes for report {report_id}")

        important_text = ReportExtractor(report_text, report_id).extract_important_text()[0]

        if important_text is None:
            return

        report_themes = self.open_ai_caller.query(
            "I am trying to find the themes present across a collection of about 50 marine accident investigation reports.\n\nFor this, I need your help by reading this report and telling me the 3-6 themes/causes that are present in the report.\n\nYour response should have a short paragraph (<= 30 words) for each theme/cause. With an empty line in between each paragraph.\n\nNote that I want the output of this process to be consistent and repeatable. This means that I want your response to be as deterministic as possible.",
            important_text,
            large_model=True,
            temp = 0
        )

        if report_themes is None:
            return

        with open(self._get_theme_file_path(report_id), "w") as f:
            f.write(report_themes)

        print(f"Themes for {report_id} generated")

    def _read_themes(self, report_id, report_themes):
        with open(self._get_theme_file_path(report_id), "r") as f:
            report_themes = f.read()

        self.all_themes += (f"Themes for {report_id}: \n{report_themes}\n")

