import os

class OutputFolderReader:
    def __init__(self, output_folder):
        self.output_folder = output_folder

    def _get_report_ids(self):
        return os.listdir(self.output_folder)
    
    def _read_file_from_each_report_dir(self, file_name, extension, processing_function):
        for report_id in self._get_report_ids():

            report_dir = os.path.join(self.output_folder, report_id)
            if not os.path.isdir(report_dir):
                print(f"  Could not find directory for {report_id}, skipping report.")
                continue

            text_path = os.path.join(report_dir, f'{report_id + file_name}.{extension}')
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
        self._read_file_from_each_report_dir('_themes', 'txt', processing_function)

    def process_reports(self, processing_function):
        self._read_file_from_each_report_dir('', 'txt', processing_function)
