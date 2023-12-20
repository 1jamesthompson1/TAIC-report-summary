from jinja2 import Environment, PackageLoader, BaseLoader
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

        # Convert the HTML to PDF
        pdf = weasyprint.HTML(string=html_out).write_pdf()

        # Create a BytesIO object from the PDF
        buffer = BytesIO(pdf)
        buffer.seek(0)
        return buffer
    
    def generate_string_search(self):
        # template 

        template = Environment(loader=BaseLoader()).from_string("""
<div>
    <h2>Search Information</h2>
    <div style="display: flex;">
    <div style="flex: 1;">
        <p>Search query: {{search_query}}</p>
        <h3>Search settings:</h3>
        {% for setting in settings %}
            <p>{{ setting.name }}: {{ setting.value }}</p>
        {% endfor %}
        <h3>Filters:</h3>
        {% for filter in filters %}
            <p>{{ filter.name }}: {{filter.value}}</p>
        {% endfor %}
    </div>
    <div style="flex: 1;">
        <h3>Theme sliders:</h3>
        <p>Note that only the sliders that have been changed from their default values are shown.</p>
        <h4>Group theme sliders:</h4>
        {% for slider in theme_group_sliders %}
            <p>{{ slider.name }}: {{ slider.value }}</p>
        {% endfor %}
    
        
        <h4>Individual theme sliders:</h4>
        {% for slider in theme_individual_sliders %}
            <p>{{ slider.name }}: {{ slider.value }}</p>
        {% endfor %}
    </div>
</div>
""")

        # unpack search
        search_query, settings, theme_slider_values_dict, theme_group_slider_values_dict, modes_list, year_range = self.search

        search_query = search_query if search_query else "No search query used"


        # Filters
        filters = [
            {'name': "Allowed modes", 'value': [mode.as_string(mode) for mode in modes_list]},
            {'name': "Year range", 'value': year_range}
        ]
        
        # Settings
        settings = [
            {'name': n, 'value': v} for n,v in settings.items()
        ]

        # Theme sliders process
        dict_to_filtered_list = lambda dict: list(
            filter(
                lambda obj: obj['value'] != (0,100),
                [{'name': n, 'value': v} for n,v in dict.items()]
            )
        )

        # Theme
        theme_individual_sliders = dict_to_filtered_list(theme_slider_values_dict)
        theme_group_sliders = dict_to_filtered_list(theme_group_slider_values_dict)

        print(settings)
        
        return template.render(settings=settings, 
                               search_query=search_query,
                               filters=filters,
                               theme_individual_sliders=theme_individual_sliders,
                               theme_group_sliders=theme_group_sliders)
        