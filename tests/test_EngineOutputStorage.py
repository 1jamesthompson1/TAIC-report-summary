import os
from datetime import datetime

import pytest
import pytz

from engine.utils.EngineOutputStorage import (
    EngineOutputDownloader,
    EngineOutputUploader,
)


def test_upload_outputs():
    uploader = EngineOutputUploader(
        os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        os.environ["AZURE_STORAGE_ACCOUNT_KEY"],
        pytest.output_config["storage"]["container"],
        pytest.output_config["folder_name"],
    )

    uploader.upload_latest_output()

    blobs = uploader.engine_output_container.list_blob_names(
        name_starts_with=datetime.now(pytz.timezone("Pacific/Auckland")).strftime(
            "%Y-%m-%d_%H"
        )
    )

    assert len(list(blobs)) > 0


def test_download_outputs(tmpdir):
    downloader = EngineOutputDownloader(
        os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        os.environ["AZURE_STORAGE_ACCOUNT_KEY"],
        pytest.output_config["storage"]["container"],
        tmpdir,
    )

    downloader.download_latest_output()

    assert len(os.listdir(tmpdir.strpath)) == 4
