from jinja2 import Environment, PackageLoader, BaseLoader
import weasyprint
from io import BytesIO
from datetime import datetime

from . import ResultsAnalysis

from engine import Modes

class ReportGenerator:
    def __init__(self, search_result, search):
        self.search_result = search_result
        self.results_analysis = ResultsAnalysis.ResultsAnalyzer(search_result)
        self.search = search
    def generate(self):
        # Set up Jinja2 environment
        env = Environment(loader=PackageLoader('viewer'))
        template = env.get_template('search_result_report.html')

        if self.search_result is None:
            data = {"date": datetime.today().strftime('%d-%m-%Y'),
                "search_info": self.generate_search_parameter_string(),
                "general_information": "<p>No reports were returned from the serach therefore this report will be empty</p>",
                }
        else:
            self.results_analysis.run_analysis()
        # Render the template with your data
            data = {"date": datetime.today().strftime('%d-%m-%Y'),
                    "search_info": self.generate_search_parameter_string(),
                    "general_information": self.generate_general_information(),
                    "safety_themes_weightings": self.generate_safety_themes_summary(),
                    "safety_issues": self.generate_safety_issues_summary()
                    }
        html_out = template.render(data)

        # Convert the HTML to PDF
        pdf = weasyprint.HTML(string=html_out).write_pdf()

        # Create a BytesIO object from the PDF
        buffer = BytesIO(pdf)
        buffer.seek(0)
        return buffer
    
    def generate_search_parameter_string(self):
        # template 

        template = Environment(loader=BaseLoader()).from_string("""
<div style="display: flex;">
<div style="flex: 2;">
    <p>Search query: {{search_query}}</p>
    <h4>Search settings:</h4>
    {% for setting in settings %}
        <p>{{ setting.name }}: {{ setting.value }}</p>
    {% endfor %}
    <h4>Filters:</h4>
    {% for filter in filters %}
        <p>{{ filter.name }}: {{filter.value}}</p>
    {% endfor %}
</div>
<div style="flex: 3;">
    <h4>Theme sliders:</h4>
    <p style="font-style:italic">Note that only the sliders that have been changed from their default ranges of (0,100) are shown.</p>
    <h5>Group theme sliders:</h5>
    {% for slider in theme_group_sliders %}
        <p>{{ slider.name }}: {{ slider.value }}</p>
    {% endfor %}

    
    <h5>Individual theme sliders:</h5>
    {% for slider in theme_individual_sliders %}
        <p>{{ slider.name }}: {{ slider.value }}</p>
    {% endfor %}
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
        
    def generate_safety_issues_summary(self):
        # template 

        template = Environment(loader=BaseLoader()).from_string("""
""")

        
        return template.render()
    

    def generate_safety_themes_summary(self):

        template = Environment(loader=BaseLoader()).from_string("""
""")
        
        return template.render()
    
    def generate_general_information(self):
        
        template = Environment(loader=BaseLoader()).from_string("""
<div style="display: block;"> 
{% for fact in facts %}
    <p>{{ fact.name }}: {{ fact.value }}</p>
{% endfor %}
</div>
""")    
        print(len(self.search_result))
        
        found_modes = set([Modes.Mode.as_string(Modes.get_report_mode_from_id(report_id)) for report_id in self.search_result['ReportID']])

        facts = [
            {'name': "Number of reports", 'value': len(self.search_result)},
            {'name': "Found modes", 'value': found_modes},
            {'name': "Number of unqiue safety issues", 'value': len(self.results_analysis.safety_issues)},
            {'name': "Top three most important safety themes", 'value': "TODO"},
        ]
        
        return template.render(facts = facts)
        