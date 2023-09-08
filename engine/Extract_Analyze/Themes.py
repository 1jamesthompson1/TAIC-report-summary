import yaml
import os
from .. import Config
# Example usage of class
# theme_reader = ThemeReader()
# print(theme_reader.get_theme_str())
# print(theme_reader.get_theme_description_str())

class ThemeFile:
    def __init__(self):
        engine_config = Config.ConfigReader().get_config()['engine']['output']

        self._file_path = os.path.join(engine_config.get("folder_name"), engine_config.get("themes_file_name"))

class ThemeReader(ThemeFile):
    def __init__(self):
        super().__init__()
        self._themes = self._get_themes()

    def get_num_themes(self) -> int:
        return len(self._themes)

    def get_theme_str(self) -> str:
        themes_str = "\"" + "\",\"".join([theme["title"] for theme in self._themes]) + "\""
        return themes_str

    def get_theme_description_str(self) -> str:
        themes_description_str = ""
        for theme in self._themes:
            themes_description_str += f"{theme['title']}:\n {theme['description']}\n\n"
        return themes_description_str
    
    def get_theme_titles(self) -> list:
        return [theme["title"] for theme in self._themes]

    def _get_themes(self) -> dict:
        if not os.path.exists(self._file_path):
            print(f"Cant read themes as {self._file_path} doesnt exist")
            return None
        themes = yaml.safe_load(open(self._file_path, 'r'))['themes']
        return themes
    
class ThemeWriter(ThemeFile):
    def __init__(self):
        super().__init__()
    
    def write_themes(self, new_themes):
        with open(self._file_path, 'w') as f:
            f.write(new_themes)
