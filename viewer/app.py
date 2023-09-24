from flask import Flask, render_template, request, jsonify
import os
import search  # Assuming this is your custom module for searching

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search_reports():
    search_query = request.form.get('searchQuery')
    settings = {
        'simple_search': request.form.get('simpleSearch') == "on",
        'search_report_text': request.form.get('searchReport') == "on",
        'search_theme_text': request.form.get('searchSummary') == "on",
        'include_incomplete_reports': request.form.get('includeIncompleteReports') == "on",
    }
    
    searcher = search.Searcher()
    results = searcher.search(search_query, settings)

    if results is None:
        return jsonify({'html_table': "<p class='text-center'>No results found</p>"})

    html_table = results.to_html(classes='table table-bordered table-hover align-middle', justify = "center", index=False, escape=False)
    
    return jsonify({'html_table': html_table})

port = int(os.environ.get("PORT", 5000))

if __name__ == '__main__':
    app.run(port=port, host="0.0.0.0")
