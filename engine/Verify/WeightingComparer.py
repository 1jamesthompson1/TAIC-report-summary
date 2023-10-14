from ..Extract_Analyze.OutputFolderReader import OutputFolderReader
from ..Extract_Analyze.Themes import ThemeReader


from .Comparer import Comparer

class WeightingComparer(Comparer):
    def __init__(self):
        super().__init__()
        self.get_validation_set('summaries')
        self.compared_weightings = dict()

    def  decode_summaries(self, report_summary):
        elements = report_summary.split(",")
        num_themes = ThemeReader().get_num_themes()
        pages_read = set(elements[1].strip('[]').split("  "))
        weightings = [float(x) for x in elements[2:(num_themes+2)]]

        return {"weights": weightings, "pages_read": pages_read}


    def add_report_ID(self, report_id, report_summary):
        self.validation_set[report_id] = self.decode_summaries(report_summary)

    def compare_weightings(self):
        print("Comparing weightings...")
    
        OutputFolderReader().read_all_summaries(self.compare_two_summaries)

        print("Finished comparing weightings.")
        
        num_reports = len(self.compared_weightings)
        print('==Validation summary==')
        print(f"  {num_reports} reports compared.")
        # print(f"  {[report for report in self.compared_weightings]}")
        print(f"  Average weighting manhattan distance percentage: {sum([self.compared_weightings[report]['weightings'] for report in self.compared_weightings])/num_reports}%")
        print(f"  Average pages read jaccard similarity: {sum([self.compared_weightings[report]['pages_read'] for report in self.compared_weightings])/num_reports}")
    

    def compare_two_summaries(self, report_id, report_summary):
        if (report_id in self.validation_set.keys()):
            print(f"  Found {report_id} in validation set.")
            engine_summary = self.decode_summaries(report_summary)
        else:
            return
        
        # Compare the pages read

        validation_pages_read = self.validation_set[report_id]["pages_read"]
        engine_pages_read = engine_summary["pages_read"]

        pages_read_jaccard_similarity = len(validation_pages_read.intersection(engine_pages_read)) / len(validation_pages_read.union(engine_pages_read))
        
        # Compare the weightings
        
        validation_weightings = self.validation_set[report_id]["weights"]

        engine_weightings = engine_summary["weights"]

        if len(validation_weightings) != len(engine_weightings):
            print(f"  Validation weightings and engine weightings have different lengths. Skipping {report_id}")
            return
        
        manhattan_weightings_similarity = sum([abs(validation_weightings[i] - engine_weightings[i]) for i in range(len(validation_weightings))])

        self.compared_weightings[report_id] = {"pages_read": pages_read_jaccard_similarity, "weightings": manhattan_weightings_similarity}



