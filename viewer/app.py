import argparse
import os
import tempfile
import uuid
from threading import Thread

import pandas as pd
from flask import Flask, jsonify, render_template, request, send_file

from . import Searching

app = Flask(__name__)

tasks_status = {}
tasks_results = {}


@app.route("/task-status/<task_id>", methods=["GET"])
def task_status(task_id):
    status = tasks_status.get(task_id, "not found")
    result = tasks_results.get(task_id, {})
    return jsonify({"task_id": task_id, "status": status, "result": result})


def get_searcher():
    return Searching.SearchEngine(os.environ["db_URI"])


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

    document_type_pie_chart = results.getDocumentTypePieChart().to_json()

    mode_pie_chart = results.getModePieChart().to_json()

    year_hist = results.getYearHistogram().to_json()

    most_common_event_types = results.getMostCommonEventTypes().to_json()

    return {
        "html_table": html_table,
        "results_summary_info": {
            "document_type_pie_chart": document_type_pie_chart,
            "mode_pie_chart": mode_pie_chart,
            "year_histogram": year_hist,
            "most_common_event_types": most_common_event_types,
            "duration": results.getSearchDuration(),
            "num_results": context_df.shape[0],
        },
        "summary": results.getSummary(),
    }


@app.route("/search", methods=["POST"])
def search():
    form_data = request.form
    task_id = str(uuid.uuid4())
    tasks_status[task_id] = "in progress"
    task_thread = Thread(target=search_reports, args=(task_id, form_data))
    task_thread.start()
    return jsonify({"task_id": task_id}), 202


def search_reports(task_id, form_data):
    try:
        results = get_searcher().search(get_search(form_data))
    except Exception as e:
        tasks_results[task_id] = repr(e)
        tasks_status[task_id] = "failed"
        return

    with app.app_context():
        tasks_results[task_id] = format_search_results(results)
        tasks_status[task_id] = "completed"


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

    if "db_URI" not in os.environ:
        os.environ["db_URI"] = "./viewer/vector_db"

    port = int(os.environ.get("PORT", 5001))
    app.run(port=port, host="0.0.0.0", debug=args.debug)


if __name__ == "__main__":
    run()
