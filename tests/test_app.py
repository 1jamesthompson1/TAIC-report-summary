import json
import os
from io import StringIO

import pandas as pd
import pytest

import viewer.app as app

os.environ["db_URI"] = "./tests/data/vector_db"


@pytest.fixture
def client():
    class MockAuth:
        @staticmethod
        def get_user():
            return {"id": "test_user", "name": "Test User"}

    app.auth = MockAuth()

    with app.app.test_client() as c:
        yield c


def perform_search_and_wait(c, form_data):
    rv = c.post("/search", data=form_data)

    assert rv.status == "202 ACCEPTED"

    while True:
        task_status = c.get("/task-status/" + json.loads(rv.data)["task_id"])
        parsed = json.loads(task_status.data)
        if parsed["status"] == "completed" or parsed["status"] == "failed":
            return parsed


def test_index():
    with app.app.test_client() as c:
        rv = c.get("/", follow_redirects=True)
        assert rv.status == "200 OK"
        assert b"<title>TAIC Document Searcher</title>" in rv.data


def test_no_login_search():
    with app.app.test_client() as c:
        rv = c.post("/search")
        assert rv.status_code == 302


def test_form_submit(client):
    rv = perform_search_and_wait(
        client,
        {
            "searchQuery": "",
            "includeModeAviation": "on",
            "includeModeRail": "on",
            "includeModeMarine": "on",
            "yearSlider-min": "2000",
            "yearSlider-max": "2024",
            "relevanceCutoff": "0.5",
            "includeSafetyIssues": "on",
            "includeRecommendations": "on",
            "includeReportSection": "on",
        },
    )
    df = pd.read_html(StringIO(rv["results"]["html_table"]))[0]
    assert df.shape[0] == 5926


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
        },
    )
    df = pd.read_html(StringIO(rv["result"]["html_table"]))[0]
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
        },
    )
    print(rv)
    df = pd.read_html(StringIO(rv["result"]["html_table"]))[0]
    assert df.shape[0] == 0


def test_form_with_query(client):
    rv = perform_search_and_wait(
        client,
        {
            "searchQuery": "pilot",
            "includeModeAviation": "on",
            "yearSlider-min": "2000",
            "yearSlider-max": "2024",
            "relevanceCutoff": "0.6",
            "includeSafetyIssues": "on",
        },
    )
    df = pd.read_html(StringIO(rv["result"]["html_table"]))[0]
    assert df.shape[0] > 0
    assert rv["result"]["summary"]
