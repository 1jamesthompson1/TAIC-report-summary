from jinja2 import Environment, PackageLoader
import weasyprint
from io import BytesIO
from datetime import datetime

class ReportGenerator:
    def __init__(self, search_result, search):
        self.search_result = search_result
        self.search = search
    def generate(self):
        # Set up Jinja2 environment
        env = Environment(loader=PackageLoader('viewer'))
        template = env.get_template('search_result_report.html')

        # Render the template with your data
        data = {"date": datetime.today().strftime('%d-%m-%Y'), "search_info": self.generate_string_search()}
        html_out = template.render(data)

        print(html_out)

        # Convert the HTML to PDF
        pdf = weasyprint.HTML(string=html_out).write_pdf()

        # Create a BytesIO object from the PDF
        buffer = BytesIO(pdf)
        buffer.seek(0)
        return buffer
    
    def generate_string_search(self):
        # unpack
        search_query, settings, theme_slider_values_dict, theme_group_slider_values_dict, modes_list, year_range = self.search

        if search_query == "":
            search_string = "No search query<br>"
        else:
            search_string = f"Search query: {search_query}<br>"

        search_string += f"Settings:<br>"
        for setting, value in settings.items():
            search_string += f"\t{setting}: {value}<br>"

        # Only include theme slider values if they are not 0-100
        if not all([value == (0, 100) for value in theme_slider_values_dict.values()]):
            search_string += f"Theme slider values:<br>"
            for theme, value in theme_slider_values_dict.items():
                search_string += f"\t{theme}: {value}<br>"

        # Only include theme group slider values if they are not 0-100
        if not all([value == (0, 100) for value in theme_group_slider_values_dict.values()]):
            search_string += f"Theme group slider values:<br>"
            for theme_group, value in theme_group_slider_values_dict.items():
                search_string += f"\t{theme_group}: {value}<br>"

        search_string += f"Modes: {[mode.as_string(mode) for mode in modes_list]}<br>"
        search_string += f"Year range: {year_range}<br>"
        
        return search_string