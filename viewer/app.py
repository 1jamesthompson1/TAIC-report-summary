import argparse
import os
import tempfile
import uuid
from io import StringIO
from threading import Thread

import dotenv
import identity.web
import pandas as pd
from azure.data.tables import TableServiceClient
from flask import (
    Flask,
    copy_current_request_context,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from werkzeug.middleware.proxy_fix import ProxyFix

from flask_session import Session
from viewer import Searching, app_config

dotenv.load_dotenv(override=True)

__version__ = "1.0.0-beta"

app = Flask(__name__)
app.config.from_object(app_config)
assert app.config["REDIRECT_PATH"] != "/", "REDIRECT_PATH must not be /"
Session(app)


app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

app.jinja_env.globals.update(Auth=identity.web.Auth)  # Useful in template for B2C
auth = identity.web.Auth(
    session=session,
    authority=app.config["AUTHORITY"],
    client_id=app.config["CLIENT_ID"],
    client_credential=app.config["CLIENT_SECRET"],
)

connection_string = f"AccountName={os.getenv('AZURE_STORAGE_ACCOUNT_NAME')};AccountKey={os.getenv('AZURE_STORAGE_ACCOUNT_KEY')};EndpointSuffix=core.windows.net"
client = TableServiceClient.from_connection_string(conn_str=connection_string)
searchlogs = client.create_table_if_not_exists(table_name="searchlogs")
resultslogs = client.create_table_if_not_exists(table_name="resultslogs")
errorlogs = client.create_table_if_not_exists(table_name="errorlogs")


searcher = Searching.SearchEngine(os.environ["db_URI"])

data_last_updated_date = searcher.all_document_types_table.list_versions()[-1][
    "timestamp"
].strftime("%Y-%m-%d")


def log_search(search):
    if searchlogs:
        search_log = {
            "PartitionKey": auth.get_user()["name"],
            "RowKey": search.uuid.hex,
            "query": search.getQuery(),
            "start_time": search.creation_time,
            **search.getSettings().to_dict(),
        }
        try:
            searchlogs.create_entity(entity=search_log)
        except Exception as e:
            print(e)
    else:
        print("Error table does not exist")


def log_search_results(results):
    if resultslogs:
        results_log = {
            "PartitionKey": auth.get_user()["name"],
            "RowKey": results.search.uuid.hex,
            "duration": results.duration,
            "summary": results.getSummary(),
            "search_results": results.getContextCleaned()
            .head(100)
            .drop(columns=["document"])
            .to_json(),
            "num_results": results.getContext().shape[0],
        }
        try:
            resultslogs.create_entity(entity=results_log)
        except Exception as e:
            print(e)
    else:
        print("Error table does not exist")


def log_search_error(e, search):
    if errorlogs:
        error_log = {
            "PartitionKey": auth.get_user()["name"],
            "RowKey": search.uuid.hex,
            "error": repr(e),
        }
        try:
            errorlogs.create_entity(entity=error_log)
        except Exception as e:
            print(e)
    else:
        print("Error table does not exist")


@app.route("/login")
def login():
    return render_template(
        "login.html",
        version=__version__,
        **auth.log_in(
            scopes=app_config.SCOPE,  # Have user consent to scopes during log-in
            redirect_uri=url_for(
                "auth_response", _external=True
            ),  # Optional. If present, this absolute URL must match your app's redirect_uri registered in Microsoft Entra admin center
            prompt="select_account",  # Optional.
        ),
        data_last_updated_date=data_last_updated_date,
    )


@app.route(app_config.REDIRECT_PATH)
def auth_response():
    result = auth.complete_log_in(request.args)
    if "error" in result:
        return render_template(
            "auth_error.html",
            result=result,
            version=__version__,
            data_last_updated_date=data_last_updated_date,
        )
    return redirect(url_for("index"))


@app.route("/logout")
def logout():
    return redirect(auth.log_out(url_for("index", _external=True)))


@app.route("/")
def index():
    if not auth.get_user():
        return redirect(url_for("login"))
    return render_template(
        "index.html",
        user=auth.get_user(),
        version=__version__,
        data_last_updated_date=data_last_updated_date,
    )


@app.route("/feedback")
def feedback():
    if not auth.get_user():
        return redirect(url_for("login"))
    return render_template(
        "feedback_form.html",
        user=auth.get_user(),
        version=__version__,
        feedback_form_loaded=True,
        data_last_updated_date=data_last_updated_date,
    )


tasks_status = {}
tasks_results = {}


@app.route("/task-status/<task_id>", methods=["GET"])
def task_status(task_id):
    status = tasks_status.get(task_id, "not found")
    result = tasks_results.get(task_id, {})
    if status == "completed":
        session["search_results"] = result
    return jsonify({"task_id": task_id, "status": status, "result": result})


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


def getUpdatedRelevanceSearch(search, new_relevance):
    search.settings.relevanceCutoff = new_relevance
    return search.to_url_params()


def format_search_results(results: Searching.SearchResult):
    context_df = results.getContextCleaned()

    context_df["Hubstream"] = context_df["report_id"].apply(
        lambda x: f'<a href="https://taic.hubstreamonline.com/#/search/Investigation/{format_report_id_as_weblink(x)}" target="_blank">Open in Hubstream</a>'
    )
    context_df["report_id"] = context_df["report_id"].apply(
        lambda x: f'<a href="https://taic.org.nz/inquiry/{format_report_id_as_weblink(x)}" target="_blank">{x}</a>'
    )

    context_df["relevance"] = context_df.apply(
        lambda x: f"""<a href="/?{getUpdatedRelevanceSearch(results.search, x['relevance'])}">{x['relevance']}</a>""",
        axis=1,
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
        "settings": results.search.getSettings().to_dict(),
        "start_time": results.search.getStartTime(),
        "query": results.search.getQuery(),
    }


@app.route("/search", methods=["POST"])
def search():
    if not auth.get_user():
        return redirect(url_for("login"))

    form_data = request.form

    task_id = str(uuid.uuid4())
    tasks_status[task_id] = "in progress"
    task_thread = Thread(
        target=copy_current_request_context(search_reports), args=(task_id, form_data)
    )
    task_thread.start()
    return jsonify({"task_id": task_id}), 202


def search_reports(task_id, form_data):
    try:
        search = get_search(form_data)
        log_search(search)
        results = searcher.search(search)
        tasks_results[task_id] = format_search_results(results)
        log_search_results(results)
        tasks_status[task_id] = "completed"

    except Exception as e:
        log_search_error(e, search)
        tasks_results[task_id] = repr(e)
        tasks_status[task_id] = "failed"
        return


def send_csv_file(df: pd.DataFrame, name: str):
    temp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)

    # Write the CSV data to the file
    df.to_csv(temp.name, index=False)

    # Send the file
    return send_file(temp.name, as_attachment=True, download_name=name)


@app.route("/get_results_as_csv", methods=["POST"])
def get_results_as_csv():
    if not auth.get_user():
        return redirect(url_for("login"))
    if session.get("search_results") is None:
        return jsonify({"error": "No results found"}), 404

    search_results = session.get("search_results")

    # Create a temporary file
    temp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    temp.close()  # Close the file so it can be opened by openpyxl

    # Create a new Workbook
    wb = Workbook()

    # Write summary information to the first sheet
    summary_info = search_results["results_summary_info"]
    summary_sheet = wb.active
    summary_sheet.title = "Summary"

    summary_sheet["A1"] = "Search Query:"
    summary_sheet["D1"] = "Redo search:"
    summary_sheet[
        "E1"
    ].hyperlink = f"""https://taic-document-searcher-cfdkgxgnc3bxgbeg.australiaeast-01.azurewebsites.net/?{Searching.Search(search_results["query"], settings=Searching.SearchSettings.from_dict(search_results["settings"])).to_url_params()}"""
    summary_sheet["A2"] = search_results["query"]

    summary_sheet["A4"] = "Start Time:"
    summary_sheet["A5"] = search_results["start_time"]

    summary_sheet["A7"] = "Settings:"
    summary_sheet["A8"] = repr(search_results["settings"])

    summary_sheet["A10"] = "Search Duration:"
    summary_sheet["A11"] = summary_info["duration"]

    summary_sheet["A13"] = "Number of Results:"
    summary_sheet["A14"] = summary_info["num_results"]

    summary_sheet["A16"] = "Summary:"
    summary_sheet["A17"] = search_results["summary"]

    # Create a second sheet and write the search results using pandas
    results_df = pd.read_html(StringIO(search_results["html_table"]))[0]

    results_sheet = wb.create_sheet(title="Search Results")

    for r_idx, row in enumerate(
        dataframe_to_rows(results_df, index=False, header=True), 1
    ):
        for c_idx, value in enumerate(row, 1):
            results_sheet.cell(row=r_idx, column=c_idx, value=value)

    # Save the workbook to the temporary file
    wb.save(temp.name)

    # Send the file as a response
    search_query = search_results["query"]
    search_start_time = search_results["start_time"]
    download_name = f"{search_query}-{search_start_time}.xlsx"

    return send_file(temp.name, as_attachment=True, download_name=download_name)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    args = parser.parse_args()

    app.run(port=5000, host="localhost", debug=args.debug)


if __name__ == "__main__":
    app.run()
