from flask import Flask, render_template, request, jsonify
from urllib.parse import parse_qs
import json
import os
import argparse
from . import search  # Assuming this is your custom module for searching

import engine.Extract_Analyze.Themes as Themes
import engine.Modes as Modes

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

def get_search(form):
    search_query = form.get('searchQuery')
    settings = {
        'use_synonyms': form.get('useSynonyms') == "on",
        'search_report_text': form.get('searchReport') == "on",
        'search_theme_text': form.get('searchSummary') == "on",
        'search_weighting_reasoning': form.get('searchWeighting') == "on",
        'include_incomplete_reports': form.get('includeIncompleteReports') == "on",
    }

    # Theme ranges
    theme_slider_values = list(map(lambda tuple: (tuple[0][6:], tuple[1]), filter(lambda tuple: tuple[0].startswith('theme-'), form.items())))

    theme_slider_values_dict = dict()
    for theme, value in theme_slider_values:
        theme_stripped = theme[:-4]
        if_min = theme.endswith('-min')
        if if_min:
            theme_slider_values_dict[theme_stripped] = (int(value), theme_slider_values_dict.get(theme_stripped, (None, None))[1])
        else:
            theme_slider_values_dict[theme_stripped] = (theme_slider_values_dict.get(theme_stripped, (None, None))[0], int(value))

    # Theme group ranges
    theme_group_slider_values = list(map(lambda tuple: (tuple[0][12:], tuple[1]), filter(lambda tuple: tuple[0].startswith('theme-group-'), form.items())))

    theme_group_slider_values_dict = dict()

    for theme_group, value in theme_group_slider_values:
        theme_group_stripped = theme_group[:-4]
        if_min = theme_group.endswith('-min')
        if if_min:
            theme_group_slider_values_dict[theme_group_stripped] = (
                int(value),
                theme_group_slider_values_dict.get(theme_group_stripped, (None, None))[1])
        else:
            theme_group_slider_values_dict[theme_group_stripped] = (
                theme_group_slider_values_dict.get(theme_group_stripped, (None, None))[0],
                int(value))
            
    print(theme_group_slider_values_dict)

    # Modes
    modes_list = list()

    if form.get('includeModeAviation') == "on":
        modes_list.append(Modes.Mode.a)
    if form.get('includeModeRail') == "on":
        modes_list.append(Modes.Mode.r)
    if form.get('includeModeMarine') == "on":
        modes_list.append(Modes.Mode.m)

    # Year
    year_range = int(form.get('yearSlider-min')), int(form.get('yearSlider-max'))

    return search_query, settings, theme_slider_values_dict, theme_group_slider_values_dict, modes_list, year_range

@app.route('/search', methods=['POST'])
def search_reports():    
    searcher = search.Searcher()
    results = searcher.search(*get_search(request.form))

    if results is None:
        return jsonify({'html_table': "<p class='text-center'>No results found</p>"})
    
    results['NoMatches'] = results.apply(lambda row: f'<a href="#" class="no-matches-link" data-report-id="{row["ReportID"]}">{row["NoMatches"]}</a>', axis=1)

    results['ThemeSummary'] = results.apply(lambda row: f'<a href="#" class="theme-summary-link" data-report-id="{row["ReportID"]}">{row["ThemeSummary"]}</a>', axis=1)

    results['SafetyIssues'] = results.apply(lambda row: f'<a href="#" class="safety-issues-link" data-report-id="{row["ReportID"]}">{row["SafetyIssues"]}</a>', axis=1)

    for theme in searcher.themes + ["Other"]:
        results[theme] = results.apply(lambda row: f'<a href="#" class="weighting-link" data-report-id="{row["ReportID"]}" data-theme="{theme}">{row[theme]}</a>', axis=1)


    html_table = results.to_html(classes='table table-bordered table-hover align-middle', table_id="dataTable", justify = "center", index=False, escape=False)
    
    return jsonify({'html_table': html_table})

@app.route('/get_report_text', methods=['GET'])
def get_report_text():
    form_serial = request.args.get('form')

    form_data = parse_qs(form_serial)
    form_data = {k: v[0] for k, v in form_data.items()}

    search_query, settings, _, _, _ = get_search(form_data)
    report_id = request.args.get('report_id')

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

@app.route('/get_safety_issues', methods=['GET'])
def get_safety_issues():
    report_id = request.args.get('report_id')

    safety_issues = search.Searcher().get_safety_issues(report_id)

    return jsonify({'title': f"Safety issues for {report_id}", 'main': safety_issues})

@app.route('/get_theme_groups', methods=['GET'])
def get_theme_groups():
    titles = Themes.ThemeReader(search.Searcher().input_dir).get_groups()

    return jsonify({'themeGroups': titles})

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    args = parser.parse_args()

    port = int(os.environ.get("PORT", 5001))
    app.run(port=port, host="0.0.0.0", debug=args.debug)

if __name__ == '__main__':
    run()
