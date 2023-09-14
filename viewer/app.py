from flask import Flask, render_template, request, jsonify

import search  # Assuming this is your custom module for searching

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search_reports():
    search_query = request.form.get('searchQuery')
    
    searcher = search.Searcher()
    results = searcher.search(search_query)

    html_table = results.to_html(classes='table table-bordered table-hover', index=False, escape=False)
    
    return jsonify({'html_table': html_table})

if __name__ == '__main__':
    app.run(debug=True)
