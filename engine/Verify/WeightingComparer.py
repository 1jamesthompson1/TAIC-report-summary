from ..Extract_Analyze.OutputFolderReader import OutputFolderReader
from ..Extract_Analyze.Themes import ThemeReader
from .Comparer import Comparer

import yaml
import csv

class WeightingComparer(Comparer):
    def __init__(self):
        super().__init__()
        self.get_validation_set('summaries')
        self.compared_weightings = dict()

    def  decode_summaries(self, report_summary):
        csv_reader = csv.reader([report_summary])

        elements = next(csv_reader)[:-2]

        return {"weights": [float(weight) if weight !="<NA>" else None for weight in elements[2::3]],
                "explanation": elements[3::3],
                "pages_read": set(elements[1].strip('[]').split("  "))}

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
        print(f"  Average weighting manhattan distance: {sum([self.compared_weightings[report]['weightings'] for report in self.compared_weightings])/num_reports}")
        print(f"  Average pages read jaccard similarity: {sum([self.compared_weightings[report]['pages_read'] for report in self.compared_weightings])/num_reports}")
    

    def compare_two_summaries(self, report_id, report_summary):
        if (report_id in self.validation_set.keys()):
            engine_summary = self.decode_summaries(report_summary)
        else:
            return
        
        # Compare the pages read

        validation_pages_read = self.validation_set[report_id]["pages_read"]
        engine_pages_read = engine_summary["pages_read"]

        pages_read_jaccard_similarity = len(validation_pages_read.intersection(engine_pages_read)) / len(validation_pages_read.union(engine_pages_read))
        
        # Compare the weightings
                
        validation_weightings = self.validation_set[report_id]["weights"]
        validation_explanation = self.validation_set[report_id]["explanation"]
        engine_weightings = engine_summary["weights"]
        engine_explanation = engine_summary["explanation"]

        if len(validation_weightings) != len(engine_weightings):
            print(f"  Validation weightings and engine weightings have different lengths. Skipping {report_id}")
            return

        ## Make sure that None are in the same location
        none_in_both = [i for i, (v, e) in enumerate(zip(validation_weightings, engine_weightings)) if v is None and e is None]
        none_in_one = [i for i, (v, e) in enumerate(zip(validation_weightings, engine_weightings)) if (v is None) != (e is None)]

        if none_in_one:
            print(f"  Validation weightings and engine weightings have a None in a different location {none_in_one}. Skipping {report_id}")
            return

        print(f"  Validation weightings and engine weightings have a None in the same location {none_in_both}. Removing from comparison.")

        validation_weightings = [v for i, v in enumerate(validation_weightings) if i not in none_in_both]
        engine_weightings = [e for i, e in enumerate(engine_weightings) if i not in none_in_both]
        validation_explanation = [v for i, v in enumerate(validation_explanation) if i not in none_in_both]
        engine_explanation = [e for i, e in enumerate(engine_explanation) if i not in none_in_both]
        
        manhattan_weightings_similarity = sum([abs(validation_weightings[i] - engine_weightings[i]) for i in range(len(validation_weightings))])/ThemeReader().get_num_themes()

        self.compared_weightings[report_id] = {"pages_read": pages_read_jaccard_similarity, "weightings": manhattan_weightings_similarity}

        # TODO
        # Compare the explanations

