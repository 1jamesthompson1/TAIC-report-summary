import yaml

# Example usage of class
# theme_reader = ThemeReader()
# print(theme_reader.get_theme_str())
# print(theme_reader.get_theme_description_str())

class ThemeReader:
    def __init__(self, file_path = 'config.yaml'):
        self._themes = self._get_themes(file_path)

    def get_num_themes(self) -> int:
        return len(self._themes)

    def get_theme_str(self) -> str:
        themes_str = "\"" + "\",\"".join(list(self._themes.keys())) + "\""
        return themes_str

    def get_theme_description_str(self) -> str:
        themes_description_str = ""
        for theme in self._themes:
            themes_description_str += f"{theme}:\n {self._themes.get(theme)['description']}\n\n"
        return themes_description_str

    def _get_themes(self, file_path) -> dict:
        with open(file_path, 'r') as f:
            data = yaml.safe_load(f)
        themes = data['themes']
        return themes