import os
import tempfile

import pandas as pd
import pytest

import engine.analyze.Embedding as Embedding


@pytest.mark.parametrize(
    "dataframe_column_name, document_column_name",
    [
        pytest.param("safety_issues", "safety_issue", id="safety_issue_embeddings"),
        pytest.param(
            "recommendations", "recommendation", id="recommendation_embeddings"
        ),
        pytest.param("sections", "section", id="section_embeddings"),
        pytest.param("text", "text", id="text_embeddings"),
    ],
)
def test_basic_embedding(dataframe_column_name, document_column_name):
    extracted_df = pd.read_pickle(
        os.path.join(
            pytest.output_config["folder_name"],
            pytest.output_config["extracted_reports_df_file_name"],
        )
    )

    # Create temp file
    temp_input = tempfile.NamedTemporaryFile(suffix=".pkl", delete=True)
    temp_output = tempfile.NamedTemporaryFile(suffix=".pkl", delete=True)

    if isinstance(
        extracted_df.dropna(subset=[dataframe_column_name], ignore_index=True).loc[
            0, dataframe_column_name
        ],
        pd.DataFrame,
    ):
        pd.concat(
            list(extracted_df[dataframe_column_name].dropna()), ignore_index=True
        ).to_pickle(temp_input.name)
    else:
        extracted_df[["report_id", dataframe_column_name]].dropna().to_pickle(
            temp_input.name
        )

    Embedding.Embedder().embed_dataframe(
        temp_input.name, document_column_name, temp_output
    )

    assert os.path.exists(temp_output.name)

    embedded_dataframe = pd.read_pickle(temp_output.name)
    print(embedded_dataframe)

    assert isinstance(embedded_dataframe, pd.DataFrame)

    assert isinstance(embedded_dataframe[document_column_name + "_embedding"][1], list)
