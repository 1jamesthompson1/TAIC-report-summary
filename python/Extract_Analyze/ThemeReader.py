import ConfigReader

# Example usage of class
# theme_reader = ThemeReader()
# print(theme_reader.get_theme_str())
# print(theme_reader.get_theme_description_str())

class ThemeReader:
    def __init__(self):
        self._themes = self._get_themes()

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

    def _get_themes(self) -> dict:
        themes = ConfigReader.configReader.get_config()['themes']
        return themes