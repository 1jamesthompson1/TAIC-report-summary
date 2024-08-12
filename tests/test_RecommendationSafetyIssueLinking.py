import os

import pandas as pd
import pytest

from engine.analyze.RecommendationSafetyIssueLinking import (
    RecommendationSafetyIssueLinker,
)


def get_test_dataframe_path():
    return os.path.join(
        pytest.output_config["folder_name"],
        pytest.output_config["extracted_reports_df_file_name"],
    )


def test_single_report_linking():
    test_dataframe = pd.read_pickle(get_test_dataframe_path())

    links = RecommendationSafetyIssueLinker()._evaluate_all_possible_links(
        test_dataframe["recommendations"].iloc[0],
        test_dataframe["safety_issues"].iloc[0],
    )

    assert isinstance(links, pd.DataFrame)


def test_multiple_reports_linking(tmpdir):
    output_path = tmpdir.join("links_temp.pkl")

    RecommendationSafetyIssueLinker().evaluate_links_for_report(
        get_test_dataframe_path(), output_path
    )

    assert os.path.exists(output_path)

    dataframe = pd.read_pickle(output_path)

    assert isinstance(dataframe, pd.DataFrame)
    assert dataframe.columns.tolist() == ["report_id", "recommendation_links"]
    assert isinstance(dataframe.loc[0, "recommendation_links"], pd.DataFrame)
