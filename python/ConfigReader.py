import yaml

class ConfigReader:
    def __init__(self, file_path = 'config.yaml'):
        self._config = self._read_config(file_path)

    def _read_config(self, file_path) -> dict:
        with open(file_path, 'r') as f:
            data = yaml.safe_load(f)
        return data

    def get_config(self) -> dict:
        return self._config
    

configReader = ConfigReader()