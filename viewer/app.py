import argparse
import base64
import os
import tempfile

import pandas as pd
from flask import Flask, g, jsonify, render_template, request, send_file

from . import Searching

app = Flask(__name__)


def get_searcher():
    if "searcher" not in g:
        g.searcher = Searching.SearchEngine(os.environ["db_URI"])

    return g.searcher


@app.route("/")
def index():
    return render_template("index.html")


def get_search(form) -> Searching.Search:
    return Searching.Search.from_form(form)


def format_search_results(results: Searching.SearchResult):
    context_df = results.getContextCleaned()
    html_table = context_df.to_html(
        classes="table table-bordered table-hover align-middle",
        table_id="dataTable",
        justify="center",
        index=False,
        escape=False,
    )

    mode_pie_chart = results.getModePieChart().to_json()

    year_hist = results.getYearHistogram().to_json()

    # most_common_event_types = results.getMostCommonEventTypes()
    # most_common_event_types.columns = ["Event Type", "Count"]
    # most_common_event_types_html_table = most_common_event_types.to_html(index=False, justify="center")

    most_common_event_types = results.getMostCommonEventTypes().to_json()

    return jsonify(
        {
            "html_table": html_table,
            "results_summary_info": {
                "mode_pie_chart": mode_pie_chart,
                "year_histogram": year_hist,
                "most_common_event_types": most_common_event_types,
            },
            "summary": results.getSummary(),
        }
    )


@app.route("/search", methods=["POST"])
def search_reports():
    results = get_searcher().search(get_search(request.form))

    return format_search_results(results)


@app.route("/get_links_visual", methods=["GET"])
def get_links_visual():
    report_id = request.args.get("report_id")

    link = Searching.SearchEngine().get_links_visual_path(report_id)

    # read image and encode

    with open(link, "rb") as image:
        encoded = base64.b64encode(image.read()).decode()

    return jsonify(
        {
            "title": f"Links visual for {report_id}",
            "main": f"<br><br><img src='data:image/png;base64,{encoded}'></img>",
        }
    )


def send_csv_file(df: pd.DataFrame, name: str):
    temp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)

    # Write the CSV data to the file
    df.to_csv(temp.name, index=False)

    # Send the file
    return send_file(temp.name, as_attachment=True, download_name=name)


@app.route("/get_results_as_csv", methods=["POST"])
def get_results_as_csv():
    search_results = get_searcher().search(get_search(request.form), with_rag=False)

    return send_csv_file(search_results.getContextCleaned(), "search_results.csv")


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    args = parser.parse_args()

    os.environ["db_URI"] = "./viewer/vector_db"

    port = int(os.environ.get("PORT", 5001))
    app.run(port=port, host="0.0.0.0", debug=args.debug)


if __name__ == "__main__":
    run()
