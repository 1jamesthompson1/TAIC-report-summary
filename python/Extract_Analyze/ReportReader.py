import os

class ReportReader:
    def __init__(self, output_folder):
        self.output_folder = output_folder

    def process_reports(self, processing_function):
        for report_id in os.listdir(self.output_folder):

            report_dir = os.path.join(self.output_folder, report_id)
            if not os.path.isdir(report_dir):
                continue

            text_path = os.path.join(report_dir, f'{report_id}.txt')
            if not os.path.exists(text_path):
                print(f"Could not find text file for {report_id}, skipping report.")
                continue
            
            with open(text_path, 'r', encoding='utf-8', errors='replace') as f:
                report_text = f.read()

            if len(report_text) < 100:
                print(f"Text file for {report_id} is too short, skipping report.")
                continue
            
            processing_function(report_id, report_text)
