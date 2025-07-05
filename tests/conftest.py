import os

import dotenv
import pytest

from engine.utils import Config
from engine.utils.AzureStorage import PDFStorageManager


@pytest.fixture(scope="session", autouse=True)
def load_test_config():
    config = Config.ConfigReader(os.path.join("tests", "test_config.yaml")).get_config()
    pytest.config = config

    pytest.output_config = config["engine"]["output"]

    dotenv.load_dotenv()


@pytest.fixture(scope="function")
def test_pdf_storage_manager():
    """
    Create a PDF storage manager for tests.

    This fixture is available to all tests in the suite and provides access
    to the test Azure storage container for PDF operations.

    Usage in test files:
    ```python
    def test_my_pdf_function(test_pdf_storage_manager):
        # Use the PDF storage manager
        pdf_list = test_pdf_storage_manager.list_pdfs()
        assert len(pdf_list) >= 0

        # Upload a test PDF
        test_pdf_storage_manager.upload_pdf("test_report", b"fake pdf data")
    ```

    The fixture automatically connects to the test container specified in test_config.yaml.
    """
    return PDFStorageManager(
        os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        os.environ["AZURE_STORAGE_ACCOUNT_KEY"],
        pytest.output_config["pdf_container_name"],
    )


@pytest.fixture(scope="function")
def stable_pdf_storage_manager():
    """
    Create a PDF storage manager for stable test PDFs.

    This fixture connects to a separate container with a consistent set of test PDFs
    that are NOT automatically cleaned up. This is useful for tests that need
    reliable, consistent PDF data.

    Usage:
    ```python
    def test_pdf_parsing(stable_pdf_storage_manager):
        # This container has a known set of test PDFs
        pdf_list = stable_pdf_storage_manager.list_pdfs()
        # pdf_list will always contain the same test reports
    ```

    Note: This container is separate from the regular test container and is not
    subject to automatic cleanup.
    """
    return PDFStorageManager(
        os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        os.environ["AZURE_STORAGE_ACCOUNT_KEY"],
        pytest.output_config["stable_pdf_container_name"],
    )


@pytest.fixture(scope="function", autouse=True)
def cleanup_test_containers():
    """
    Universal cleanup fixture for Azure test containers.

    This fixture automatically cleans up Azure storage containers after each test
    to prevent accumulation of test data and associated storage costs.
    Available to all tests in the suite.

    NOTE: This does NOT clean up the stable PDF container, which is meant to
    contain consistent test data.
    """
    # This runs before each test
    yield

    # This runs after each test completes
    try:
        # Clean up regular PDF container (but NOT the stable one)
        pdf_storage_manager = PDFStorageManager(
            os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
            os.environ["AZURE_STORAGE_ACCOUNT_KEY"],
            pytest.output_config["pdf_container_name"],
        )

        # Get list of all blobs and delete them silently
        all_blobs = pdf_storage_manager.list_blobs()
        for blob_name in all_blobs:
            try:
                pdf_storage_manager.delete_blob(blob_name)
            except Exception:
                # Silently ignore individual deletion failures
                pass

    except Exception:
        # Don't fail the test run if cleanup fails
        pass
