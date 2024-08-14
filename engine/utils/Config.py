import os

import yaml


class ConfigFile:
    def __init__(self, file_path="config.yaml"):
        self._file_path = file_path

        if not self._file_exists():
            raise ValueError(f"{file_path} does not exist")

    def _file_exists(self) -> bool:
        return os.path.exists(self._file_path)


class ConfigReader(ConfigFile):
    def __init__(self, file_path="config.yaml"):
        super().__init__(file_path)
        self._config = self._read_config()

    def _read_config(self) -> dict:
        with open(self._file_path, "r") as f:
            data = yaml.safe_load(f)
        return data

    def get_config(self) -> dict:
        return self._config


class ConfigWriter(ConfigFile):
    def __init__(self):
        super().__init__()

    def write_config(self, config) -> None:
        with open(self._file_path, "w") as f:
            yaml.dump(config, f)


configReader = ConfigReader()
configWriter = ConfigWriter()
