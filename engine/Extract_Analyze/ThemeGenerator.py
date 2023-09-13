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

        summarised_themes = self.open_ai_caller.query(
            "Here is a list of themes received from a collection of marine accident investigation reports.\n\nThese were retrieved by reading each report and listing 3-6 causes from each report.\n\nPlease read and figure out what are the most common and important causes.\n\nEach cause should have a title and description.\n\nThere can be 5-10 main causes."
            ,
            self.all_themes,
            large_model=True,
            temp = 0
        )

        formated_themes = self.open_ai_caller.query(
            "I will give you descriptions of themes and I want to you format them into yaml.\n\nJust output the yaml structure with no extra text.\n\nThe yaml layout should follow the structure seen below where the title and description is replaced. With as many theme elements as needed.\n\nthemes:\n   - title:  theme name one\n    description: \"Description of the first theme\"\n\n  - title:  theme name two\n    description: \"Description of the second theme\"\n\n  - title:  theme name three\n    description: \"Description of the third theme\"\n\n  - title:  theme name four\n    description: \"Description of the fourth theme\"\n\n  - title:  theme name five\n    description: \"Description of the fifth theme\"\n\n  - title:  theme name six\n    description: \"Description of the sixth theme\"\n ",
            summarised_themes,
            temp = 0)
        
        Themes.ThemeWriter().write_themes(formated_themes)

        print("Themes summaried and written to file")        


    def _get_theme(self, report_id, report_text):

        print(f"Generating themes for report {report_id}")

        important_text = ReportExtractor(report_text, report_id).extract_important_text()[0]

        if important_text is None:
            return

        report_themes = self.open_ai_caller.query(
            "I want to learn the causes of accidents across a collection of 50 or so accident investigation reports.\n\nTo help I would to know the causes and themes of this accident. Please give me 3- 6\n\nYour output should have a short paragraph (<50 words) per cause with an empty line separating each cause.\n",
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

