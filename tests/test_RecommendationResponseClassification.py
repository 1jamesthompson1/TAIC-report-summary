import engine.analyze.RecommendationResponseClassification as RecommendationResponseClassification

import pandas as pd


import os
import pytest

@pytest.mark.parametrize("response, expected", [
    pytest.param("The CAA has issued a new reminder to all aircraft operators and pilots about the importance of seatbelts.", "Accepted and Implemented", id="Accepted and Implemented"), 
    pytest.param("The CAA will work on adding a new reminder to aircraft operators in the next quarterly industry newsletter", "Accepted", id="Accepted"),
    pytest.param("The CAA will think about a reminder and whether it fits in with the current industry standards", "Under consideration", id="Under consideration"),
    pytest.param("The CAA believes that reminders are not needed as they are very ineffective.", "Rejected", id="Rejected")
])
def test_classify_response(response, expected):
    recommendation_response_classifier = RecommendationResponseClassification.RecommendationResponseClassifier()
    recommendation = "The Commission recommends that the CAA issues a new reminder to all aircraft operators and pilots of the importance of ensuring that aircraft occupants fasten and properly adjust their seatbelts at all times."
    recommendation_num = 1

    response_category = recommendation_response_classifier.classify_response(response, recommendation, recommendation_num)

    assert response_category == expected


@pytest.mark.parametrize("input", [
    pytest.param(
        "tests/data/extracted_reports.pkl",
        id="from scratch"),
    pytest.param(
        "tests/data/response_classification_partly_classified.pkl",
        id="some already classified"),
])
def test_classification_process(input):
    output = "tests/data/response_classification_temp.pkl"
    RecommendationResponseClassification.RecommendationResponseClassificationProcessor().process(input, output, [2000, 2020])

    assert os.path.exists(output)

    dataframe = pd.read_pickle(output)
    os.remove(output)

    assert isinstance(dataframe, pd.DataFrame)
    assert 'response_category' in dataframe.columns
    assert 'response_category_quality' in dataframe.columns

    print(dataframe[['recommendation_id', 'response_category', 'response_category_quality']])

    assert dataframe['response_category_quality'].isnull().sum() == 0