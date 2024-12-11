import importlib
import json
import time
from io import StringIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# I am reimporting the app so that I can patch the login_required function
# This seems to be the easiest way for me to make the tests work and allow me to test it even if it is a bit of a hack.
import viewer.app as app


@pytest.fixture(scope="function")
def client():
    """
    This returns a client that has a mock Auth instance that acts like it is authenticated.
    """
    print("Setting up mock auth")
    with patch("identity.flask.Auth") as mock_auth:
        auth_instance = MagicMock()

        counter = 0

        def mock_login_required(f):
            nonlocal counter
            endpoint_name = f"{f.__name__}_wrapped_{counter}"
            counter += 1
            print(f"Mock login_required called for {f.__name__} as {endpoint_name}")

            def wrapped(*args, **kwargs):
                print(f"Wrapped function called for {f.__name__}")
                return f(*args, context={"user": "test_user"}, **kwargs)

            wrapped.__name__ = endpoint_name
            return wrapped

        auth_instance.login_required = mock_login_required
        mock_auth.return_value = auth_instance
        importlib.reload(app)
        with app.app.test_client() as c:
            yield c

    print("Tearing down mock auth")


def perform_search_and_wait(c, form_data):
    rv = c.post("/search", data=form_data)

    print(rv.data)

    assert rv.status == "202 ACCEPTED"

    start_time = time.time()

    while True:
        task_status = c.get("/task-status/" + json.loads(rv.data)["task_id"])
        parsed = json.loads(task_status.data)
        if parsed["status"] == "completed" or parsed["status"] == "failed":
            return parsed

        if time.time() - start_time > 180:
            raise TimeoutError("Search timed out")
        time.sleep(2)


def test_index():
    importlib.reload(app)
    with app.app.test_client() as c:
        rv = c.get("/", follow_redirects=True)
        print(rv.data)
        assert rv.status == "200 OK"
        assert b"<title>TAIC Document Searcher</title>" in rv.data


def test_no_login_search():
    importlib.reload(app)
    with app.app.test_client() as c:
        rv = c.post("/search")
        print(rv.data)
        assert rv.status_code == 200
        assert b"Sign In" in rv.data


def test_form_submit(client):
    rv = perform_search_and_wait(
        client,
        {
            "searchQuery": "",
            "includeModeAviation": "on",
            "includeModeRail": "on",
            "includeModeMarine": "on",
            "yearSlider-min": "2000",
            "yearSlider-max": "2025",
            "relevanceCutoff": "0.0",
            "includeSafetyIssues": "on",
            "includeRecommendations": "on",
            "includeReportSection": "on",
            "includeReportText": "on",
            "includeTAIC": "on",
            "includeATSB": "on",
            "includeTSB": "on",
        },
    )
    df = pd.read_html(StringIO(rv["result"]["html_table"]))[0]
    assert df.shape[0] == 2834


def test_form_submit_filtered(client):
    rv = perform_search_and_wait(
        client,
        {
            "searchQuery": "",
            "includeModeRail": "on",
            "includeModeMarine": "on",
            "yearSlider-min": "2010",
            "yearSlider-max": "2024",
            "relevanceCutoff": "0.7",
            "includeSafetyIssues": "on",
            "includeRecommendations": "on",
            "includeReportSection": "on",
            "includeATSB": "on",
            "includeTSB": "on",
        },
    )
    df = pd.read_html(StringIO(rv["result"]["html_table"]))[0]
    assert len(df) > 0
    assert df["year"].isin(range(2010, 2025)).all()
    assert df["mode"].isin(["Rail", "Marine"]).all()


def test_form_submit_no_results(client):
    rv = perform_search_and_wait(
        client,
        {
            "searchQuery": "pilot",
            "includeModeAviation": "on",
            "yearSlider-min": "1900",
            "yearSlider-max": "1924",
            "relevanceCutoff": "0.5",
            "includeSafetyIssues": "on",
            "includeTAIC": "on",
        },
    )
    print(rv)
    df = pd.read_html(StringIO(rv["result"]["html_table"]))[0]
    assert df.shape[0] == 0


def test_form_with_query(client):
    rv = perform_search_and_wait(
        client,
        {
            "searchQuery": "pilots of aircraft",
            "includeModeAviation": "on",
            "yearSlider-min": "2000",
            "yearSlider-max": "2024",
            "relevanceCutoff": "0.1",
            "includeSafetyIssues": "on",
            "includeRecommendations": "on",
            "includeReportSection": "on",
            "includeTAIC": "on",
            "includeATSB": "on",
        },
    )
    df = pd.read_html(StringIO(rv["result"]["html_table"]))[0]
    assert df.shape[0] > 0
    assert rv["result"]["summary"]


def test_form_with_fts_with_results(client):
    rv = perform_search_and_wait(
        client,
        {
            "searchQuery": '"work needed"',
            "includeModeAviation": "on",
            "includeModeMarine": "on",
            "includeModeRail": "on",
            "yearSlider-min": "2000",
            "yearSlider-max": "2024",
            "relevanceCutoff": "0.6",
            "includeSafetyIssues": "on",
            "includeRecommendations": "on",
            "includeTAIC": "on",
        },
    )

    df = pd.read_html(StringIO(rv["result"]["html_table"]))[0]
    assert df.shape[0] > 0


def test_form_with_fts_no_results(client):
    rv = perform_search_and_wait(
        client,
        {
            "searchQuery": '""More work needed""',
            "includeModeAviation": "on",
            "yearSlider-min": "2000",
            "yearSlider-max": "2024",
            "relevanceCutoff": "0.6",
            "includeSafetyIssues": "on",
            "includeTSB": "on",
        },
    )

    df = pd.read_html(StringIO(rv["result"]["html_table"]))[0]
    assert df.shape[0] == 0
