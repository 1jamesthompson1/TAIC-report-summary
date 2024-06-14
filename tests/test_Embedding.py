import engine.analyze.Embedding as Embedding

import pandas as pd
import pytest

import os
import tempfile

@pytest.mark.parametrize("dataframe_column_name, document_column_name", [
    pytest.param("safety_issues", "safety_issue", id="safety_issue_embeddings"),
    pytest.param("recommendations", "recommendation", id="recommendation_embeddings"),
    pytest.param('sections', 'section', id="section_embeddings"),
    pytest.param('important_text', 'important_text', id="important_text"),
])
def test_basic_embedding(dataframe_column_name, document_column_name):
    extracted_df = pd.read_pickle("tests/data/extracted_reports.pkl")
    extracted_df['report_id'] = extracted_df.index
    extracted_df = extracted_df.reset_index(drop=True)

    # Create temp file
    temp_input = tempfile.NamedTemporaryFile(suffix='.pkl', delete=True)
    temp_output = tempfile.NamedTemporaryFile(suffix='.pkl', delete=True)

    if isinstance(extracted_df.loc[0, dataframe_column_name], pd.DataFrame):
        pd.concat(list(extracted_df[dataframe_column_name].dropna()), ignore_index=True).to_pickle(temp_input.name)
    else:
        extracted_df[['report_id', dataframe_column_name]].dropna().to_pickle(temp_input.name)

    Embedding.Embedder().embed_dataframe(temp_input.name, document_column_name, temp_output)

    assert os.path.exists(temp_output.name)

    embedded_dataframe = pd.read_pickle(temp_output.name)
    print(embedded_dataframe)

    assert isinstance(embedded_dataframe, pd.DataFrame)

    assert isinstance(embedded_dataframe[document_column_name + '_embedding'][0], list)