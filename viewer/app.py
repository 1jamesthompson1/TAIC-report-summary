from flask import Flask, render_template, request, jsonify, send_file
from urllib.parse import parse_qs
import json
import os
import argparse
from . import Search, ReportCreation
from threading import Thread
from werkzeug.wsgi import FileWrapper
import tempfile
import pandas as pd

import base64

import engine.Extract_Analyze.Themes as Themes
import engine.Modes as Modes

app = Flask(__name__)

def parseFormSerial(form_serial):
    form_data = parse_qs(form_serial)
    form_data = {k: v[0] for k, v in form_data.items()}

    return form_data

@app.route('/')
def index():
    return render_template('index.html')

def get_search(form):
    if form is None or len(form) == 0:
        print("No form data")

    search_query = form.get('searchQuery')
    settings = {
        'use_synonyms': form.get('useSynonyms') == "on",
        'search_report_text': form.get('searchReport') == "on",
        'search_theme_text': form.get('searchSummary') == "on",
        'search_weighting_reasoning': form.get('searchWeighting') == "on",
        'include_incomplete_reports': form.get('includeIncompleteReports') == "on",
    }

    # Theme ranges
    theme_slider_values = list(map(
        lambda tuple: (tuple[0][6:], tuple[1]),
        filter(lambda tuple: tuple[0].startswith('theme-') and not tuple[0].startswith('theme-group'), form.items())))

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

def format_search_results(results):
    searcher = Search.Searcher()

    if results is None:
        return jsonify({'html_table': "<p class='text-center'>No results found</p>"})
    
    # Remove extra columns that are not needed
    results = results.filter(regex='^(?!Complete)')

    results['NoMatches'] = results.apply(lambda row: f'<a href="#" class="no-matches-link" data-report-id="{row["ReportID"]}">{row["NoMatches"]}</a>', axis=1)

    results['ThemeSummary'] = results.apply(lambda row: f'<a href="#" class="theme-summary-link" data-report-id="{row["ReportID"]}">{row["ThemeSummary"]}</a>', axis=1)

    results['SafetyIssues'] = results.apply(lambda row: f'<a href="#" class="safety-issues-link" data-report-id="{row["ReportID"]}">{row["SafetyIssues"]}</a>', axis=1)

    results['Recommendations'] = results.apply(lambda row: f'<a href="#" class="recommendations-link" data-report-id="{row["ReportID"]}">{row["Recommendations"]}</a>', axis=1)

    results['linksVisual'] = results.apply(lambda row: f'<a href="#" class="links-visual-link" data-report-id="{row["ReportID"]}">Visualization of recommendation and safety issue links</a>' if row['linksVisual'] else 'No links to show', axis=1)

    for theme in searcher.themes + ["Other"]:
        results[theme] = results.apply(lambda row: f'<a href="#" class="weighting-link" data-report-id="{row["ReportID"]}" data-theme="{theme}">{row[theme]}</a>', axis=1)


    html_table = results.to_html(classes='table table-bordered table-hover align-middle', table_id="dataTable", justify = "center", index=False, escape=False)
    
    return jsonify({'html_table': html_table})

@app.route('/search', methods=['POST'])
def search_reports():    
    results = Search.Searcher().search(*get_search(request.form))

    return format_search_results(results)

@app.route('/get_report_text', methods=['GET'])
def get_report_text():
    form_data = parseFormSerial(request.args.get('form'))

    search_query, settings, _, _, _, _ = get_search(form_data)
    report_id = request.args.get('report_id')

    searcher = Search.Searcher()
    highlighted_report_text = searcher.get_highlighted_report_text(report_id, search_query, settings)

    return jsonify({'title': report_id, 'main': highlighted_report_text})

@app.route('/get_weighting_explanation', methods=['GET'])
def get_weighting_explanation():
    report_id = request.args.get('report_id')
    theme = request.args.get('theme')

    explanation = Search.Searcher().get_weighting_explanation(report_id, theme)

    return jsonify({'title': f"{theme} for {report_id}", 'main': explanation})

@app.route('/get_theme_text', methods=['GET'])
def get_theme_text():
    report_id = request.args.get('report_id')

    theme_text = Search.Searcher().get_theme_text(report_id)

    return jsonify({'title': f"Theme summary for {report_id}", 'main': theme_text})

@app.route('/get_safety_issues', methods=['GET'])
def get_safety_issues():
    report_id = request.args.get('report_id')

    safety_issues = Search.Searcher().get_safety_issues(report_id)

    safety_issues += "<br><br><em>These safety issues are identified using a LLM model this means that they could not be 100% accurate.</em>"

    return jsonify({'title': f"Safety issues for {report_id}", 'main': safety_issues})

@app.route('/get_recommendations', methods=['GET'])
def get_recommendations():
    report_id = request.args.get('report_id')

    recommendations = Search.Searcher().get_recommendations(report_id)

    main_text = "<br><br>".join(recommendations) if len(recommendations) > 0 else "No recommendations found"

    return jsonify({'title': f"Recommendations for {report_id}", 'main': main_text})

@app.route('/get_links_visual', methods=['GET'])
def get_links_visual():
    report_id = request.args.get('report_id')

    link = Search.Searcher().get_links_visual_path(report_id)

    # read image and encode

    with open(link, "rb") as image:
        encoded = base64.b64encode(image.read()).decode()

    return jsonify({'title': f"Links visual for {report_id}", 'main': f"<br><br><img src='data:image/png;base64,{encoded}'></img>"})



@app.route('/get_theme_groups', methods=['GET'])
def get_theme_groups():
    titles = Themes.ThemeReader(Search.Searcher().input_dir).get_groups()

    return jsonify({'themeGroups': titles})

result = None

@app.route('/get_results_summary_report', methods=['POST'])
def get_results_summary_report():
    global result
    result = None
    thread = Thread(target=get_results_summary_report_task, args=(request.form,))
    thread.start()
    return jsonify({'message': 'Task started'}), 202

def get_results_summary_report_task(form_data):
    global result
    search_data = get_search(form_data)

    search_results = Search.Searcher().search(*search_data)
    generated_report = ReportCreation.ReportGenerator(
        search_results,
        search_data
        ).generate()

    result = generated_report

@app.route('/get_result', methods=['GET'])
def get_result():
    global result
    if result is None:
        # The result is not ready yet
        return jsonify({'message': 'Result not ready'}), 202
    else:
        # The result is ready, send it as a file
        return send_file(ReportCreation.ReportGenerator.generate_pdf(result), download_name='report.pdf')

@app.route('/get_results_as_csv', methods=['POST'])
def get_results_as_csv():
    search_data = get_search(request.form)

    search_results = Search.Searcher().search(*search_data)

        # Create a temporary file
    temp = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)

    # Write the CSV data to the file
    search_results.to_csv(temp.name, index=False)

    # Send the file
    return send_file(temp.name, as_attachment=True, download_name='search_results.csv')

@app.route('/get_SI_recs_links_as_csv', methods=['POST'])
def get_SI_recs_links_as_csv():

    search_data = get_search(request.form)

    search_results = Search.Searcher().search(*search_data)

    recommendations = search_results['Completelinks'].tolist()

    print(recommendations)

    all_recommendations = pd.concat(filter(lambda x: x is not None, recommendations), axis=0)

    # Create a temporary file
    temp = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)

    # Write the CSV data to the file
    all_recommendations.to_csv(temp.name, index=False)

    # Send the file
    return send_file(temp.name, as_attachment=True, download_name='safety_issue_recommendation_links.csv')

@app.route('/get_results_safety_issues_as_csv', methods=['POST'])
def get_results_safety_issues_as_csv():
     search_data = get_search(request.form)

    search_results = Search.Searcher().search(*search_data)
  
    search_results_safety_issues = search_results[['ReportID', 'CompleteSafetyIssues']]
    
    search_results_safety_issues = search_results_safety_issues.explode('CompleteSafetyIssues')

    search_results_safety_issues.dropna(subset=['CompleteSafetyIssues'], inplace=True)

    search_results_safety_issues['SafetyIssue'] = search_results_safety_issues['CompleteSafetyIssues'].apply(lambda x: x['safety_issue'])
    search_results_safety_issues['SafetyIssueIndicatedQuality'] = search_results_safety_issues['CompleteSafetyIssues'].apply(lambda x: x['quality'])

    search_results_safety_issues = search_results_safety_issues[['ReportID', 'SafetyIssue', 'SafetyIssueIndicatedQuality']]

    search_results_safety_issues.to_csv(temp.name, index=False)

    # Send the file
    return send_file(temp.name, as_attachment=True, download_name='safety_issues.csv')

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    args = parser.parse_args()

    port = int(os.environ.get("PORT", 5001))
    app.run(port=port, host="0.0.0.0", debug=args.debug)

if __name__ == '__main__':
    run()
