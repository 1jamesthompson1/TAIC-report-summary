import os
import urllib.parse

import pandas as pd
import pytest

import engine.utils.Modes as Modes
import viewer.Searching as Searching


class TestSearchSettings:
    def test_basic_creation(self):
        settings = Searching.SearchSettings(
            [Modes.Mode.a, Modes.Mode.r],
            (2000, 2020),
            ["safety_issue", "recommendation"],
            0.1,
        )

        assert settings.getModes() == [Modes.Mode.a, Modes.Mode.r]
        assert settings.getYearRange() == (2000, 2020)

    def test_failed_creation(self):
        with pytest.raises(TypeError):
            Searching.SearchSettings(
                [Modes.Mode.a, Modes.Mode.r],
                ["safety_issue", "recommendation"],
                (2001, "2020"),
            )

        with pytest.raises(TypeError):
            Searching.SearchSettings(["a", "r"], (2001, 2020))

    def test_to_dict(self):
        settings = Searching.SearchSettings(
            [Modes.Mode.a, Modes.Mode.r],
            (2000, 2020),
            ["safety_issue", "recommendation"],
            0.1,
        )

        assert settings.to_dict() == {
            "setting_modes": "[0, 1]",
            "setting_max_year": 2020,
            "setting_min_year": 2000,
            "setting_document_types": "['safety_issue', 'recommendation']",
            "setting_relevanceCutoff": 0.1,
        }

    def test_from_dict(self):
        settings = Searching.SearchSettings.from_dict(
            {
                "setting_modes": "[0, 1]",
                "setting_max_year": 2020,
                "setting_min_year": 2000,
                "setting_document_types": "['safety_issue', 'recommendation']",
                "setting_relevanceCutoff": 0.1,
            }
        )

        assert settings.getModes() == [Modes.Mode.a, Modes.Mode.r]
        assert settings.getYearRange() == (2000, 2020)
        assert settings.getDocumentTypes() == ["safety_issue", "recommendation"]
        assert settings.getRelevanceCutoff() == 0.1


class TestSearch:
    def test_basic_creation(self):
        search = Searching.Search(
            "hello",
            Searching.SearchSettings(
                [Modes.Mode.a, Modes.Mode.r],
                (2000, 2020),
                ["safety_issue", "recommendation"],
                0.1,
            ),
        )

        assert search.getQuery() == "hello"
        assert search.getSettings().getModes() == [Modes.Mode.a, Modes.Mode.r]
        assert search.getSettings().getYearRange() == (2000, 2020)

    def test_from_form_creation(self):
        form = {
            "searchQuery": "hello",
            "includeModeAviation": "on",
            "includeModeRail": "on",
            "yearSlider-min": "2000",
            "yearSlider-max": "2020",
            "relevanceCutoff": "0.1",
            "includeSafetyIssues": "on",
            "includeRecommendations": "on",
        }

        search = Searching.Search.from_form(form)

        assert search.getQuery() == "hello"
        assert search.getSettings().getModes() == [Modes.Mode.a, Modes.Mode.r]
        assert search.getSettings().getYearRange() == (2000, 2020)
        assert search.getSettings().getRelevanceCutoff() == 0.1
        assert search.getSettings().getDocumentTypes() == [
            "safety_issue",
            "recommendation",
        ]

    def test_failed_form_creation(self):
        form = {"query": "hello"}

        with pytest.raises(ValueError):
            Searching.Search.from_form(form)

    def test_to_url_params(self):
        search = Searching.Search(
            "hello",
            Searching.SearchSettings(
                [Modes.Mode.a, Modes.Mode.r],
                (2000, 2020),
                ["safety_issue", "recommendation"],
                0.1,
            ),
        )

        assert isinstance(search.to_url_params(), str)

        parsed_params = urllib.parse.parse_qs(search.to_url_params())

        assert parsed_params["searchQuery"][0] == "hello"
        assert parsed_params["includeModeAviation"][0] == "on"
        assert parsed_params["includeModeRail"][0] == "on"
        assert parsed_params["includeModeMarine"][0] == "off"
        assert parsed_params["yearSlider-min"][0] == "2000"
        assert parsed_params["yearSlider-max"][0] == "2020"
        assert parsed_params["relevanceCutoff"][0] == "0.1"
        assert parsed_params["includeSafetyIssues"][0] == "on"
        assert parsed_params["includeRecommendations"][0] == "on"
        assert parsed_params["includeReportSection"][0] == "off"
        assert parsed_params["includeImportantText"][0] == "off"


@pytest.fixture(scope="session")
def searcher():
    return Searching.SearchEngine(os.environ["db_URI"])


class TestSearcher:
    def test_search(self, searcher):
        search = Searching.Search(
            "hello",
            Searching.SearchSettings(
                [Modes.Mode.a, Modes.Mode.r],
                (2000, 2020),
                ["safety_issue", "recommendation"],
                0.1,
            ),
        )
        result = searcher.search(search, with_rag=False)

        assert result
        assert result.getSummary() is None
        assert isinstance(result.getSearchDuration(), str)
        assert isinstance(result.getContext(), pd.DataFrame)

    def test_search_with_summary(self, searcher):
        search = Searching.Search(
            "pilot incapacity",
            Searching.SearchSettings(
                Modes.all_modes, (2000, 2020), ["safety_issue", "recommendation"], 0.8
            ),
        )
        result = searcher.search(search, with_rag=True)

        assert result
        assert isinstance(result.getSummary(), str)
        assert isinstance(result.getContext(), pd.DataFrame)

    def test_filtered_search(self, searcher):
        search = Searching.Search(
            "pilot",
            Searching.SearchSettings(
                [Modes.Mode.a, Modes.Mode.r],
                (2010, 2015),
                ["safety_issue", "recommendation"],
                0.7,
            ),
        )
        result = searcher.search(search, with_rag=False)

        assert result
        assert result.getSummary() is None
        assert isinstance(result.getContext(), pd.DataFrame)

        assert (
            result.getContext()["year"].isin([2010, 2011, 2012, 2013, 2014, 2015]).all()
        )

        assert result.getContext()["mode"].isin([0, 1]).all()

    def test_filtered_search_single_mode(self, searcher):
        search = Searching.Search(
            "pilot",
            Searching.SearchSettings(
                [Modes.Mode.a], (2010, 2015), ["safety_issue", "recommendation"], 0.5
            ),
        )
        result = searcher.search(search, with_rag=False)

        assert result
        assert result.getSummary() is None
        assert isinstance(result.getContext(), pd.DataFrame)

        assert (
            result.getContext()["year"].isin([2010, 2011, 2012, 2013, 2014, 2015]).all()
        )

        assert result.getContext()["mode"].isin([0]).all()

    def test_filtered_search_no_query(self, searcher):
        search = Searching.Search(
            "",
            Searching.SearchSettings(
                [Modes.Mode.m, Modes.Mode.r],
                (2002, 2005),
                ["safety_issue", "recommendation"],
                0.6,
            ),
        )
        assert search.getSearchType() == "none"
        result = searcher.search(search, with_rag=False)

        assert result
        assert result.getSummary() is None
        assert isinstance(result.getContext(), pd.DataFrame)

        assert result.getContext()["year"].isin([2002, 2003, 2004, 2005]).all()

        assert result.getContext()["mode"].isin([2, 1]).all()

    def test_search_without_results(self, searcher):
        search = Searching.Search(
            "hello",
            Searching.SearchSettings(
                [Modes.Mode.a, Modes.Mode.r],
                (1897, 1899),
                ["safety_issue", "recommendation"],
                0.6,
            ),
        )
        assert search.getSearchType() == "vector"
        result = searcher.search(search, with_rag=False)

        assert result
        assert result.getSummary() is None
        assert result.getContext().empty

    def test_fts_search(self, searcher):
        search = Searching.Search(
            '"work"',
            Searching.SearchSettings(
                Modes.all_modes,
                (2000, 2020),
                ["safety_issue", "recommendation"],
                0.6,
            ),
        )
        assert search.getSearchType() == "fts"
        result = searcher.search(search, with_rag=False)

        assert result
        assert result.getSummary() is None

        assert result.getContext().shape[0] == 22

    def test_fts_search_no_results(self, searcher):
        search = Searching.Search(
            '""More work needed""',
            Searching.SearchSettings(
                Modes.all_modes,
                (2000, 2020),
                ["safety_issue", "recommendation"],
                0.6,
            ),
        )
        assert search.getSearchType() == "fts"
        result = searcher.search(search, with_rag=False)

        assert result
        assert result.getSummary() is None

        assert result.getContext().shape[0] == 0
