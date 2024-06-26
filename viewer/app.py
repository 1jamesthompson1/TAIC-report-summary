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


def format_report_id_as_weblink(report_id):
    """
    Formats a report id like it has to be on the taic.org.nz website and hubstream links
    2011_002 -> AO-2011-002
    2018_206 -> MO-2018-206
    2020_120 -> RO-2020-020
    """
    letters = ["a", "r", "m"]

    return f"{letters[int(report_id[5])]}o-{report_id[0:4]}-{report_id[5:8]}"


def format_search_results(results: Searching.SearchResult):
    context_df = results.getContextCleaned()

    context_df["recommendations"] = context_df.apply(
        lambda row: f"<a href='#' data-safety-issue-id='{row['safety_issue_id']}' class='safety-issue-recommendations-link'>{row['recommendations']}</a>",
        axis=1,
    )

    # Add link columns

    context_df["Hubstream"] = context_df["report_id"].apply(
        lambda x: f'<a href="https://taic.hubstreamonline.com/#/search/Investigation/{format_report_id_as_weblink(x)}" target="_blank">Open in Hubstream</a>'
    )
    context_df["report_id"] = context_df["report_id"].apply(
        lambda x: f'<a href="https://taic.org.nz/inquiry/{format_report_id_as_weblink(x)}" target="_blank">{x}</a>'
    )

    html_table = context_df.to_html(
        classes="table table-bordered table-hover align-middle",
        table_id="dataTable",
        justify="center",
        index=False,
        escape=False,
    )

    mode_pie_chart = results.getModePieChart().to_json()

    year_hist = results.getYearHistogram().to_json()

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


@app.route("/get_safety_issue_recommendations", methods=["GET"])
def get_safety_issue_recommendations():
    safety_issue_id = request.args.get("safety_issue_id")

    recommendations = get_searcher().get_recommendations_for_safety_issue(
        safety_issue_id
    )

    columns = ["recommendation_id", "recommendation", "recipient"]

    recommendations_df = pd.DataFrame(recommendations, columns=columns)

    recommendations_df = recommendations_df[columns]
    recommendations_df.columns = ["Recommendation ID", "Recommendation", "Recipient"]

    return jsonify(
        {
            "title": f"Recommendations for {safety_issue_id}",
            "main": f"<br>{recommendations_df.to_html(index=False, justify='center') if recommendations_df.shape[0] > 0 else 'No recommendations found'}<br><br><em>These recommendations are linked to the safety issue using AI so won't be 100% accurate.</em>",
        }
    )


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
