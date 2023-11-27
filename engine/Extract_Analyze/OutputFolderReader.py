import os
from .. import Config
import regex as re

class OutputFolderReader:
    def __init__(self, output_folder = None):
        self.output_config = Config.ConfigReader().get_config()['engine']['output']
        if output_folder is None:
            self.output_folder = self.output_config.get("folder_name")
        else:
            self.output_folder = output_folder

    def _get_report_ids(self):
        directory_names = os.listdir(self.output_folder)

        def extract_report_id(dir_name):
            if match := re.search(r'\d{4}_\d{3}', dir_name):
                return match.group()
            else:
                return None

        report_ids = map( extract_report_id,directory_names)

        return list(filter( lambda ele: ele != None, report_ids))
    
    def _read_file_from_each_report_dir(self, file_name_template, processing_function):
        report_dir_template = self.output_config.get("reports").get("folder_name")
        for report_id in self._get_report_ids():

            report_dir = os.path.join(self.output_folder, report_dir_template.replace(r'{{report_id}}', report_id))
            if not os.path.isdir(report_dir):
                print(f"  Could not find directory for {report_id}, skipping report.")
                continue

            text_path = os.path.join(report_dir, file_name_template.replace(
            r'{{report_id}}', report_id))
            if not os.path.exists(text_path):
                print(f"  Could not find file for {report_id}, skipping report.")
                continue
            
            with open(text_path, 'r', encoding='utf-8', errors='replace') as f:
                report_text = f.read()

            if len(report_text) < 5:
                print(f"  Text file for {report_id} is too short, skipping report.")
                continue
            
            processing_function(report_id, report_text)

    def read_all_themes(self, processing_function):
        self._read_file_from_each_report_dir(self.output_config.get("reports").get("themes_file_name"), processing_function)

    def read_all_summaries(self, processing_function):
        self._read_file_from_each_report_dir(self.output_config.get("reports").get("weightings_file_name"), processing_function)

    def process_reports(self, processing_function):
        self._read_file_from_each_report_dir(self.output_config.get("reports").get("text_file_name"), processing_function)
