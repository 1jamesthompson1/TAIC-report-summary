from flask import Flask, render_template, request, jsonify
import os
import argparse
from . import search  # Assuming this is your custom module for searching

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
    return search_query, settings

@app.route('/search', methods=['POST'])
def search_reports():
    search_query, settings = get_search()
    
    searcher = search.Searcher()
    results = searcher.search(search_query, settings)

    if results is None:
        return jsonify({'html_table': "<p class='text-center'>No results found</p>"})
    
    results['NoMatches'] = results.apply(lambda row: f'<a href="#" class="no-matches-link" data-report-id="{row["ReportID"]}">{row["NoMatches"]}</a>', axis=1)

    # Update all of the weightings to be links
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


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    args = parser.parse_args()

    port = int(os.environ.get("PORT", 5000))
    app.run(port=port, host="0.0.0.0", debug=args.debug)

if __name__ == '__main__':
    run()
