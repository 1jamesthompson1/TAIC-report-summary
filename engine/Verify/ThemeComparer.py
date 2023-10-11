from ..Extract_Analyze.OutputFolderReader import OutputFolderReader
from .. import Config

class ThemeComparer:
    def __init__(self):
        self.validation_set = dict()
        self.get_validation_set()

        self.compared_themes = dict()

    def get_validation_set(self):
        if self.validation_set == dict():
            validation_set_folder_name = Config.ConfigReader().get_config()['engine']['validation']['folder_name']
            reader = OutputFolderReader(validation_set_folder_name)
            print (f"Reading validation set from {validation_set_folder_name} folder.")
            reader.read_all_themes(self.add_report_ID)

        return self.validation_set
        


    def add_report_ID(self, report_id, report_theme):
        print(f"  Adding {report_id} to validation set.")
        self.validation_set[report_id] = report_theme

    def compare_themes(self):
        print("Comparing themes...")
    
        OutputFolderReader().read_all_themes(self.compare_two_themes)
        
        

    def compare_two_themes(self, report_id, report_theme):
        if report_id in self.validation_set.keys():
            print(f"  Found {report_id} in validation set.")
        else:
            return
        
        validation_theme = self.validation_set[report_id]

        
        
        
        

