import os
import tempfile

import pandas as pd
import pytest

import engine.analyze.Embedding as Embedding


@pytest.mark.parametrize(
    "dataframe_column_name, document_column_name, current_df, start_num",
    [
        pytest.param(
            "safety_issues", "safety_issue", None, None, id="safety_issue_embeddings"
        ),
        pytest.param(
            "recommendations",
            "recommendation",
            None,
            None,
            id="recommendation_embeddings",
        ),
        pytest.param(
            "sections",
            "section",
            "report_sections_embeddings_1.pkl",
            1,
            id="section_embeddings",
        ),
        pytest.param("text", "text", None, None, id="text_embeddings"),
    ],
)
def test_basic_embedding(
    dataframe_column_name, document_column_name, current_df, start_num
):
    extracted_df = pd.read_pickle(
        os.path.join(
            pytest.output_config["folder_name"],
            pytest.output_config["extracted_reports_df_file_name"],
        )
    )

    # Create temp file
    temp_input = tempfile.NamedTemporaryFile(suffix=".pkl", delete=True)
    temp_output = tempfile.NamedTemporaryFile(suffix=".pkl", delete=True)
    input_df = None
    if isinstance(
        extracted_df.dropna(subset=[dataframe_column_name], ignore_index=True).loc[
            0, dataframe_column_name
        ],
        pd.DataFrame,
    ):
        input_df = pd.concat(
            list(extracted_df[dataframe_column_name].dropna()), ignore_index=True
        )
    else:
        input_df = extracted_df[["report_id", dataframe_column_name]].dropna()

    if isinstance(current_df, str):
        current_df = pd.read_pickle(
            os.path.join(
                pytest.output_config["folder_name"],
                pytest.output_config["embeddings"]["folder_name"],
                current_df,
            )
        )
        input_df.index += current_df.shape[0]
    else:
        start_num = 0

    input_df.to_pickle(temp_input.name)

    Embedding.Embedder().embed_dataframe(
        temp_input.name, document_column_name, temp_output.name, current_df, start_num
    )

    output_path = temp_output.name
    assert os.path.exists(output_path)

    embedded_dataframe = pd.read_pickle(output_path)

    assert isinstance(embedded_dataframe, pd.DataFrame)

    assert isinstance(embedded_dataframe[document_column_name + "_embedding"][1], list)
