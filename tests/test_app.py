import json
import os
from io import StringIO

import pandas as pd

import viewer.app as app

os.environ["db_URI"] = "./tests/data/vector_db"


def test_index():
    with app.app.test_client() as c:
        rv = c.get("/")
        assert rv.status == "200 OK"
        assert b"<title>TAIC safety issues Searcher</title>" in rv.data


def test_form_submit():
    with app.app.test_client() as c:
        rv = c.post(
            "/search",
            data={
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
            follow_redirects=True,
        )
        assert rv.status == "200 OK"
        df = pd.read_html(StringIO(json.loads(rv.data)["html_table"]))[0]
    assert df.shape[0] == 5822


def test_form_submit_filtered():
    with app.app.test_client() as c:
        rv = c.post(
            "/search",
            data={
                "searchQuery": "",
                "includeModeRail": "on",
                "includeModeMarine": "on",
                "yearSlider-min": "2010",
                "yearSlider-max": "2024",
                "relevanceCutoff": "0.5",
                "includeSafetyIssues": "on",
                "includeRecommendations": "on",
                "includeReportSection": "on",
            },
            follow_redirects=True,
        )

        assert rv.status == "200 OK"

        df = pd.read_html(StringIO(json.loads(rv.data)["html_table"]))[0]

        assert df["year"].isin(range(2010, 2025)).all()
        assert df["mode"].isin(["Rail", "Marine"]).all()


def test_form_submit_no_results():
    with app.app.test_client() as c:
        rv = c.post(
            "/search",
            data={
                "searchQuery": "pilot",
                "includeModeAviation": "on",
                "yearSlider-min": "1900",
                "yearSlider-max": "1924",
                "relevanceCutoff": "0.5",
                "includeSafetyIssues": "on",
            },
            follow_redirects=True,
        )
        assert rv.status == "200 OK"
        df = pd.read_html(StringIO(json.loads(rv.data)["html_table"]))[0]

        assert df.shape[0] == 0


def test_form_with_query():
    with app.app.test_client() as c:
        rv = c.post(
            "/search",
            data={
                "searchQuery": "pilot",
                "includeModeAviation": "on",
                "yearSlider-min": "2000",
                "yearSlider-max": "2024",
                "relevanceCutoff": "0.5",
                "includeSafetyIssues": "on",
            },
            follow_redirects=True,
        )
        assert rv.status == "200 OK"
        pd.read_html(StringIO(json.loads(rv.data)["html_table"]))[0]

        assert json.loads(rv.data)["summary"]
