import os

import dotenv
import pytest

from engine.utils import Config


@pytest.fixture(scope="session", autouse=True)
def load_test_config():
    config = Config.ConfigReader(os.path.join("tests", "test_config.yaml")).get_config()
    pytest.config = config

    pytest.output_config = config["engine"]["output"]

    dotenv.load_dotenv()
