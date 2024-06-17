from engine.utils.OpenAICaller import openAICaller

import pandas as pd
from tqdm import tqdm

tqdm.pandas()
import numpy as np

import os


class RecommendationResponseClassifier:
    """
    This class will take a response to a recommendation and provide a string classification of it. There are going to be 4 classifications ranging from Rejected to Accepted and Implemented.
    """

    def __init__(self):
        self.response_categories = [
            {
                "category": "Accepted and Implemented",
                "definition": "The recommendation was accepted (wholly) and has been implemented",
            },
            {
                "category": "Accepted",
                "definition": "The recommendation was accepted (wholly) and is being, or will be implemented",
            },
            {
                "category": "Under consideration",
                "definition": "The recipient has acknowledged that the recommendation is received and will consider it.",
            },
            {
                "category": "Rejected",
                "definition": "The recommendation will not be implemented",
            },
        ]

        pass

    def classify_response(self, response, recommendation, recommendation_num):
        categories = "\n".join(
            [
                f"{element['category']} - {element['definition']}"
                for element in self.response_categories
            ]
        )

        system_prompt = f"""
    You are helping me put responses into categories.

    These responses are to recommendations that were made in a transport accident investigation report. These recommendations are issued directly to a particular party.

    There are three categories:

    {categories}

    However if there are responses that don't fit into any of the categories then you can put them as N/A. These may be responses that request further information or want recommendation to be sent elsewhere.

    Your response should just be the name of the category with nothing else.
    """
        user_prompt = f"""
    Which category is this response in?

    "
    {response}
    "

    in regards to recommendation {recommendation_num}
    """

        openai_response = openAICaller.query(
            system_prompt, user_prompt, model="gpt-4", temp=0
        )

        if openai_response in [
            category["category"] for category in self.response_categories
        ] + ["N/A"]:
            return openai_response.lower()
        else:
            print(f"Did not match any of the categories - {openai_response}")
            return "Classification Error"


class RecommendationResponseClassificationProcessor:
    """
    This class uses `RecommendationResponseClassifier` to classify the responses of the recommendations.
    """

    def __init__(self):
        self.recommendation_response_classifier = RecommendationResponseClassifier()

        self.required_columns = [
            "recommendation_id",
            "recommendation",
            "reply_text",
            "response_category",
        ]

    def process(self, input_path: str, output_path: str, year_ranges):
        """
        This will read the DataFrame of recommendations from the data folder and then add a response category column and a response category quality column then save the DataFrame in the output folder.
        """
        print(
            "============================================================================================================================="
        )
        print(
            "                                         ...Classifying recommendations..."
        )
        print(
            "============================================================================================================================="
        )

        if not os.path.exists(input_path):
            raise ValueError(f"{input_path} does not exist")

        # Load the data
        recommendations_df = pd.read_pickle(input_path)

        recommendations_df = pd.concat(
            list(recommendations_df["recommendations"].dropna()), ignore_index=True
        )

        # Check to make sure it has all of the required columns
        if not all(
            [column in recommendations_df.columns for column in self.required_columns]
        ):
            missing_columns = [
                column
                for column in self.required_columns
                if column not in recommendations_df.columns
            ]
            raise RuntimeError(
                f"Required column/s {missing_columns} not found in DataFrame given\nPath of DataFrame file: {input_path}\nDataFrame columns: {list(recommendations_df.columns)}"
            )

        start_date = f"{year_ranges[0]}-01-01"
        end_date = f"{year_ranges[1]}-12-31"

        # Filter out so that it is only within the years specified
        recommendations_df.query(
            "made >= @start_date & made <= @end_date", inplace=True
        )

        columns = [
            "report_id",
            "recommendation_id",
            "recipient",
            "made",
            "recommendation",
            "recommendation_text",
            "extra_recommendation_context",
            "reply_text",
        ]

        # Check to see the for previously classified responses
        if os.path.exists(output_path):
            output_df = pd.read_pickle(output_path)
            merged_df = pd.merge(recommendations_df, output_df, on=columns, how="outer")
            merged_df = merged_df.drop_duplicates(subset=columns)
            merged_df.drop(columns=["response_category_x"], inplace=True)
            merged_df.rename(
                columns={"response_category_y": "response_category"}, inplace=True
            )
        else:
            merged_df = recommendations_df

        recommendations_df = self._process(merged_df)

        recommendations_df["response_category"] = recommendations_df[
            "response_category"
        ].apply(lambda x: x.lower() if isinstance(x, str) else "Classification Error")

        recommendations_df.to_pickle(output_path)

    def _process(self, recommendations: pd.DataFrame) -> pd.DataFrame:
        """
        Take a long form DataFrame of recommendations and provide a response_category column. There will be a response_category quality column
        """
        # Splits into two DataFrames based on whether response_category is already filled out.
        unclassified_responses = recommendations[
            recommendations["response_category"].isnull()
        ]
        classified_responses = recommendations[
            ~recommendations["response_category"].isnull()
        ]

        # Add response_category quality column if it doesnt exist
        if "response_category_quality" not in classified_responses.columns:
            classified_responses["response_category_quality"] = None

        classified_responses["response_category_quality"] = classified_responses[
            "response_category_quality"
        ].apply(lambda x: "exact" if x is None else x)

        print(
            f" Out of all {len(recommendations)} recommendations, {len(unclassified_responses)} need to be classified"
        )

        if len(unclassified_responses) == 0:
            return recommendations

        # For all empty response_category infer the response category

        unclassified_responses["response_category"] = (
            unclassified_responses.progress_apply(
                lambda x: self.recommendation_response_classifier.classify_response(
                    x["reply_text"], x["recommendation"], x["recommendation_id"]
                ),
                axis=1,
            )
        )

        unclassified_responses["response_category_quality"] = "inferred"

        return pd.concat(
            [classified_responses, unclassified_responses], ignore_index=True
        )
