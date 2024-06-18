import os

import pandas as pd

from engine.analyze.RecommendationSafetyIssueLinking import (
    RecommendationSafetyIssueLinker,
)


class TestRecommendationSafetyIssueLinking:
    def get_test_dataframe(self):
        return pd.read_pickle("tests/data/extracted_reports.pkl")

    def test_single_report_linking(self):
        test_dataframe = self.get_test_dataframe()

        links = RecommendationSafetyIssueLinker()._evaluate_all_possible_links(
            test_dataframe["recommendations"].iloc[0],
            test_dataframe["safety_issues"].iloc[0],
        )

        assert isinstance(links, pd.DataFrame)

    def test_multiple_reports_linking(self):
        RecommendationSafetyIssueLinker().evaluate_links_for_report(
            "tests/data/extracted_reports.pkl", "tests/data/links_temp.pkl"
        )

        assert os.path.exists("tests/data/links_temp.pkl")

        dataframe = pd.read_pickle("tests/data/links_temp.pkl")

        assert isinstance(dataframe, pd.DataFrame)
        assert dataframe.columns.tolist() == ["report_id", "recommendation_links"]
        assert isinstance(dataframe.loc[0, "recommendation_links"], pd.DataFrame)

        os.remove("tests/data/links_temp.pkl")
