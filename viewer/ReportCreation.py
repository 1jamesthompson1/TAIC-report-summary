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
                    "safety_themes": self.generate_safety_themes_summary(),
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
        
        return template.render(settings=settings, 
                               search_query=search_query,
                               filters=filters,
                               theme_individual_sliders=theme_individual_sliders,
                               theme_group_sliders=theme_group_sliders)
        
    def generate_safety_themes_summary(self):
        # template 

        template = Environment(loader=BaseLoader()).from_string("""
Each report was compared against {{ num_themes }} to find which safety theme contributed most to an accident.
                                                                
<h4>Summary of all safety themes and average weighting for each report:</h4>                                       
<style>
.num-summary th {
    font-size: 0.6em;
}
.num-summary td {
    padding: 0.5em;
}
</style>
<div id="num-summary">{{ num_summary}}</div>
                                                                
<h4>Top 5 most common safety themes:</h4>
{{ common_themes}}
                                                                
<h4>Top 5 most impactful safety themes:</h4>
{{ impactful_themes }}
""")
        # Get 5 number summary of each theme column
        num_summary = self.results_analysis.theme_weightings.describe().loc[['min', '25%', '50%', '75%', 'max']].round(2).T.to_html()

        # Common themes
        common_themes = self.results_analysis.theme_weightings.map(lambda x: 1 if x > 1 else 0).sum().sort_values(ascending=False).head(5)
        common_themes_df = common_themes.reset_index()
        common_themes_df.columns = ['Theme', 'Occurences in accidents']
        common_themes_html = common_themes_df.to_html(index=False)

        # Impactful themes
        impactful_themes = self.results_analysis.theme_weightings.mean().sort_values(ascending=False).head(5)
        impactful_themes_df = impactful_themes.reset_index()
        impactful_themes_df.columns = ['Theme', 'Average weighting']
        impactful_themes_html = impactful_themes_df.to_html(index=False)

        

        return template.render(num_summary=num_summary,
                               common_themes=common_themes_html,
                               impactful_themes=impactful_themes_html)
    

    def generate_safety_issues_summary(self):

        template = Environment(loader=BaseLoader()).from_string("""
<em>NOTE: The safety issue extraction is not perfect (see <a href="https://github.com/1jamesthompson1/TAIC-report-summary/issues/101">issue #101</a>) and so the results below may not be accurate.</em>
                                                                
<h4>Top {{ number_of_common_safey_issues }} most common safety issues:</h4>
<em>NOTE: only showing safety issues that are present in more than one report. Up to 10 safety issues will be shown</em>
{% for issue in common_safety_issues %}
    <div style="display: flex; justify-content: space-between;">
        <div style="width: 50%;">
            <p>- {{ issue.description }}</p>
        </div>
        <div style="width: 50%;">
            <p>Present in these reports:</p>
            <ul>
            {% for report in issue.reports %}
                <li>{{ report }}</li>
            {% endfor %}
            </ul>
        </div>
    </div>
{% endfor %}
                                                                
<h4>Short AI generated summary of safety issues present:</h4>
<p>{{ safety_issues_summary }}</p>
""")
        
        sorted_safety_issues = sorted(self.results_analysis.safety_issues, key=lambda x: len(x['reports']), reverse=True)

        common_safety_issues = list(filter(
            lambda x: len(x['reports']) > 1,
            sorted_safety_issues)
        )[:10]


    
        
        return template.render(
            number_of_common_safey_issues = len(common_safety_issues),
            common_safety_issues=common_safety_issues,
            safety_issues_summary=self.results_analysis.safety_issues_summary.replace("\n", "<br>"))
    
    def generate_general_information(self):
        
        template = Environment(loader=BaseLoader()).from_string("""
<div style="display: block;"> 
{% for fact in facts %}
    <p>{{ fact.name }}: {{ fact.value }}</p>
{% endfor %}
</div>
""")    
        
        found_modes = set([Modes.Mode.as_string(Modes.get_report_mode_from_id(report_id)) for report_id in self.search_result['ReportID']])

        # safety themes
        present_themes = self.results_analysis.theme_weightings.map(lambda x: 1 if x > 1 else 0).sum().loc[lambda x: x > 0]

        facts = [
            {'name': "Number of reports", 'value': len(self.search_result)},
            {'name': "Found modes", 'value': found_modes},
            {'name': "Number of unqiue safety issues", 'value': len(self.results_analysis.safety_issues)},
            {'name': "Number of Safety themes present", 'value': len(present_themes)},
        ]
        
        return template.render(facts = facts)
        