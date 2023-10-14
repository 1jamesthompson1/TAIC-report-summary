from ..Extract_Analyze.OutputFolderReader import OutputFolderReader
from .. import Config

class Comparer:
    def __init__(self):
        self.validation_set = dict()

    def get_validation_set(self, validation_type):
        if self.validation_set == dict():
            validation_set_folder_name = Config.ConfigReader().get_config()['engine']['validation']['folder_name']
            reader = OutputFolderReader(validation_set_folder_name)
            print (f"Reading validation set from {validation_set_folder_name} folder.")
            match(validation_type):
                case('themes'):
                    reader.read_all_themes(self.add_report_ID)
                case('summaries'):
                    reader.read_all_summaries(self.add_report_ID)
                case ():
                    raise ValueError(f"Unknown validation type {validation_type}")

        return self.validation_set
    
    
    def add_report_ID(self, report_id, report_data):
        raise NotImplementedError("add_report_ID not implemented")