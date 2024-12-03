import os

import pandas as pd
import pytest

import engine.analyze.RecommendationResponseClassification as RecommendationResponseClassification


@pytest.mark.parametrize(
    "response, expected",
    [
        pytest.param(
            "The CAA has issued a new reminder to all aircraft operators and pilots about the importance of seatbelts.",
            "accepted and implemented",
            id="Accepted and Implemented",
        ),
        pytest.param(
            "The CAA will work on adding a new reminder to aircraft operators in the next quarterly industry newsletter",
            "accepted",
            id="Accepted",
        ),
        pytest.param(
            "The CAA will think about a reminder and whether it fits in with the current industry standards",
            "under consideration",
            id="Under consideration",
        ),
        pytest.param(
            "The CAA believes that reminders are not needed as they are very ineffective.",
            "rejected",
            id="Rejected",
        ),
    ],
)
def test_classify_response(response, expected):
    recommendation_response_classifier = (
        RecommendationResponseClassification.RecommendationResponseClassifier()
    )
    recommendation = "The Commission recommends that the CAA issues a new reminder to all aircraft operators and pilots of the importance of ensuring that aircraft occupants fasten and properly adjust their seatbelts at all times."
    recommendation_num = 1

    response_category = recommendation_response_classifier.classify_response(
        response, recommendation, recommendation_num
    )

    assert response_category == expected


def test_classification_process(tmpdir):
    for path_name in [
        os.path.join(
            pytest.output_config["folder_name"],
            pytest.output_config["extracted_reports_df_file_name"],
        ),
    ]:
        output = tmpdir.join("response_classification_temp.pkl")
        perform_test(path_name, output)


def perform_test(input, output):
    RecommendationResponseClassification.RecommendationResponseClassificationProcessor().process(
        input, output
    )

    assert os.path.exists(output)

    dataframe = pd.read_pickle(output)
    os.remove(output)

    assert isinstance(dataframe, pd.DataFrame)
    assert "response_category" in dataframe.columns
    assert "response_category_quality" in dataframe.columns

    print(
        dataframe[
            ["recommendation_id", "response_category", "response_category_quality"]
        ]
    )

    assert dataframe["response_category_quality"].isnull().sum() == 0
