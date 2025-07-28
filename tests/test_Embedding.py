import os
import tempfile

import pytest

import engine.analyze.Embedding as Embedding


def test_basic_embedding():
    extracted_df_path = os.path.join(
        pytest.output_config["folder_name"],
        pytest.output_config["extracted_reports_df_file_name"],
    )

    # Initialize VectorDB instance
    test_db_uri = os.getenv("TEST_VECTORDB_URI")
    assert test_db_uri, "TEST_VECTORDB_URI environment variable must be set."

    vector_db = Embedding.VectorDB(
        local_embedded_ids_path=tempfile.mkdtemp(),
        db_uri=test_db_uri,
    )

    vector_db.process_extracted_reports(
        extracted_df_path,
        [
            (
                "safety_issues",
                "safety_issue",
            ),
            (
                "recommendations",
                "recommendation",
            ),
            (
                "sections",
                "section",
            ),
            (
                "summary",
                "summary",
            ),
        ],
    )

    # Verify table exists
    assert vector_db.table_name in vector_db.db.table_names()

    # Verify that the table has rows
    assert vector_db.table.count_rows() > 0, "The table should have rows."

    # Drop all tables
    vector_db.db.drop_all_tables()

    # Ensure tables are dropped
    assert not vector_db.db.table_names()
