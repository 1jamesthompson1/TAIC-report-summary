import os
from datetime import datetime

import lancedb
import pytest
import pytz

from engine.utils.EngineOutputStorage import (
    EngineOutputDownloader,
    EngineOutputUploader,
)


def test_upload_outputs(tmpdir):
    vector_db_uri = tmpdir.strpath
    embeddings = list(pytest.output_config["embeddings"].values())[1:]
    embedding_paths = [
        os.path.join(
            pytest.output_config["folder_name"],
            pytest.output_config["embeddings"]["folder_name"],
            file,
        )
        for file in embeddings
    ]

    uploader = EngineOutputUploader(
        os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        os.environ["AZURE_STORAGE_ACCOUNT_KEY"],
        pytest.output_config["storage"]["container"],
        pytest.output_config["folder_name"],
        vector_db_uri,
        *embedding_paths,
    )

    uploader.upload_latest_output()

    blobs = uploader.engine_output_container.list_blob_names(
        name_starts_with=datetime.now(pytz.timezone("Pacific/Auckland")).strftime(
            "%Y-%m-%d_%H"
        )
    )

    assert len(list(blobs)) > 0

    db = lancedb.connect(vector_db_uri)
    assert "all_document_types" in db.table_names()


def test_download_outputs(tmpdir):
    downloader = EngineOutputDownloader(
        os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        os.environ["AZURE_STORAGE_ACCOUNT_KEY"],
        pytest.output_config["storage"]["container"],
        tmpdir,
    )

    downloader.download_latest_output()

    downloaded_files = [len(f[2]) for f in os.walk(tmpdir.strpath)]
    assert sum(downloaded_files) == 8
