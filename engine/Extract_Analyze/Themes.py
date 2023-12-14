import yaml
import os
from .. import Config, Modes
# Example usage of class
# theme_reader = ThemeReader()
# print(theme_reader.get_theme_str())
# print(theme_reader.get_theme_description_str())

class ThemeFile:
    def __init__(self, output_path: str = None, use_predefined: bool = False):
        engine_config = Config.ConfigReader().get_config()['engine']['output']
        if output_path is None:
            self._file_path = os.path.join(engine_config.get("folder_name"), engine_config.get("predefined_themes_file_name") if use_predefined else engine_config.get("themes_file_name"))
        else:
            self._file_path = os.path.join(output_path, engine_config.get("predefined_themes_file_name") if use_predefined else engine_config.get("themes_file_name"))

class ThemeReader(ThemeFile):
    def __init__(self, output_path: str = None, use_predefined: bool = False, modes = Modes.all_modes):
        super().__init__(output_path, use_predefined)
        self._modes = modes
        self._themes = self._get_themes()

    def get_num_themes(self, modes = None) -> int:
        if modes is None:
            modes = self._modes
        return len(ThemeReader._filter_themes(self._themes, modes))

    def get_theme_str(self) -> str:
        themes_str = "\"" + "\",\"".join([theme["title"].strip("\n") for theme in self._themes]) + "\""
        return themes_str

    def get_theme_description_str(self, modes = None) -> str:
        if modes is None:
            modes = self._modes
        return_themes = ThemeReader._filter_themes(self._themes, modes)

        themes_description_str = ""
        for theme in return_themes:
            themes_description_str += f"{theme['title']}:\n {theme['description']}\n\n"
        return themes_description_str
    
    def get_theme_titles(self, modes = None) -> list:
        if modes is None:
            modes = self._modes
        return [theme["title"] for theme in ThemeReader._filter_themes(self._themes, modes)]

    def _get_themes(self) -> dict:
        if not os.path.exists(self._file_path):
            print(f"Cant read themes as {self._file_path} doesnt exist")
            return None
        themes_file = yaml.safe_load(open(self._file_path, 'r'))
        print(themes_file)
        themes = themes_file['themes']

        # Filter out themes that are not in the modes
        themes = ThemeReader._filter_themes(themes, self._modes)
        return themes
    
    def _filter_themes(themes, modes):
        if not isinstance(modes, list):
            modes = [modes]
        return list(filter(lambda theme: any(Modes.Mode[mode] in modes for mode in theme['modes']), themes))
class ThemeWriter(ThemeFile):
    def __init__(self):
        super().__init__()
    
    def write_themes(self, new_themes):
        with open(self._file_path, 'w') as f:
            yaml.dump(new_themes, f, default_flow_style=False, width=float('inf'), sort_keys=False)
