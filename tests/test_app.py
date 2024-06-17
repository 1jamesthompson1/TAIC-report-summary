import viewer.app as app

import pandas as pd
import json


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
            },
            follow_redirects=True,
        )
        assert rv.status == "200 OK"
        df = pd.read_html(json.loads(rv.data)["html_table"])[0]
        assert df.shape[0] == 1841


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
            },
            follow_redirects=True,
        )

        assert rv.status == "200 OK"

        df = pd.read_html(json.loads(rv.data)["html_table"])[0]

        assert df["year"].isin(range(2010, 2025)).all()
        assert df["mode"].isin([1, 2]).all()


def test_form_with_query():
    with app.app.test_client() as c:
        rv = c.post(
            "/search",
            data={
                "searchQuery": "pilot",
                "includeModeAviation": "on",
                "yearSlider-min": "2000",
                "yearSlider-max": "2024",
            },
            follow_redirects=True,
        )
        assert rv.status == "200 OK"
        pd.read_html(json.loads(rv.data)["html_table"])[0]

        assert json.loads(rv.data)["summary"]
