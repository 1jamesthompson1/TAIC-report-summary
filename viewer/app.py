from flask import Flask, render_template, request, jsonify
import os
import argparse
from . import search  # Assuming this is your custom module for searching

import engine.Extract_Analyze.Themes as Themes
import engine.Modes as Modes

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

def get_search():

    search_query = request.form.get('searchQuery')
    settings = {
        'simple_search': request.form.get('simpleSearch') == "on",
        'search_report_text': request.form.get('searchReport') == "on",
        'search_theme_text': request.form.get('searchSummary') == "on",
        'include_incomplete_reports': request.form.get('includeIncompleteReports') == "on",
    }

    slider_values = list(map(lambda tuple: (tuple[0][6:], tuple[1]), filter(lambda tuple: tuple[0].startswith('theme-'), request.form.items())))

    slider_values_dict = dict()
    for theme, value in slider_values:
        theme_stripped = theme[:-4]
        if_min = theme.endswith('-min')
        if if_min:
            slider_values_dict[theme_stripped] = (int(value), slider_values_dict.get(theme_stripped, (None, None))[1])
        else:
            slider_values_dict[theme_stripped] = (slider_values_dict.get(theme_stripped, (None, None))[0], int(value))

    # Modes
    modes_list = list()

    if request.form.get('includeModeAviation') == "on":
        modes_list.append(Modes.Mode.a)
    if request.form.get('includeModeRail') == "on":
        modes_list.append(Modes.Mode.r)
    if request.form.get('includeModeMarine') == "on":
        modes_list.append(Modes.Mode.m)
    
    print(modes_list)

    return search_query, settings, slider_values_dict, modes_list

@app.route('/search', methods=['POST'])
def search_reports():
    search_query, settings, theme_ranges, theme_modes = get_search()
    
    searcher = search.Searcher()
    results = searcher.search(search_query, settings, theme_ranges, theme_modes)

    if results is None:
        return jsonify({'html_table': "<p class='text-center'>No results found</p>"})
    
    results['NoMatches'] = results.apply(lambda row: f'<a href="#" class="no-matches-link" data-report-id="{row["ReportID"]}">{row["NoMatches"]}</a>', axis=1)

    results['ThemeSummary'] = results.apply(lambda row: f'<a href="#" class="theme-summary-link" data-report-id="{row["ReportID"]}">{row["ThemeSummary"]}</a>', axis=1)

    for theme in searcher.themes:
        results[theme] = results.apply(lambda row: f'<a href="#" class="weighting-link" data-report-id="{row["ReportID"]}" data-theme="{theme}">{row[theme]}</a>', axis=1)


    html_table = results.to_html(classes='table table-bordered table-hover align-middle', table_id="dataTable", justify = "center", index=False, escape=False)
    
    return jsonify({'html_table': html_table})

@app.route('/get_report_text', methods=['GET'])
def get_report_text():
    search_query, settings = get_search()
    report_id = request.args.get('report_id')
    search_query = request.args.get('search_query')

    searcher = search.Searcher()
    highlighted_report_text = searcher.get_highlighted_report_text(report_id, search_query, settings)

    return jsonify({'title': report_id, 'main': highlighted_report_text})

@app.route('/get_weighting_explanation', methods=['GET'])
def get_weighting_explanation():
    report_id = request.args.get('report_id')
    theme = request.args.get('theme')

    explanation = search.Searcher().get_weighting_explanation(report_id, theme)

    return jsonify({'title': f"{theme} for {report_id}", 'main': explanation})

@app.route('/get_theme_text', methods=['GET'])
def get_theme_text():
    report_id = request.args.get('report_id')

    theme_text = search.Searcher().get_theme_text(report_id)

    return jsonify({'title': f"Theme summary for {report_id}", 'main': theme_text})

@app.route('/get_theme_titles', methods=['GET'])
def get_theme_titles():
    titles = Themes.ThemeReader(search.Searcher().input_dir).get_theme_titles()

    return jsonify({'theme_titles': titles})

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    args = parser.parse_args()

    port = int(os.environ.get("PORT", 5000))
    app.run(port=port, host="0.0.0.0", debug=args.debug)

if __name__ == '__main__':
    run()
