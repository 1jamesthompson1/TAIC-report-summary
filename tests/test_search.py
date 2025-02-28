import os
import urllib.parse

import pandas as pd
import pytest

import engine.utils.Modes as Modes
import viewer.Searching as Searching


class TestSearchSettings:
    def test_basic_creation(self):
        settings = Searching.SearchSettings(
            ["ATSB", "TSB"],
            [Modes.Mode.a, Modes.Mode.r],
            (2000, 2020),
            ["safety_issue", "recommendation"],
            0.1,
        )

        assert settings.get_modes() == [Modes.Mode.a, Modes.Mode.r]
        assert settings.get_year_range() == (2000, 2020)

    def test_failed_creation(self):
        with pytest.raises(TypeError):
            Searching.SearchSettings(
                ["TAIC", "ATSB", "TSB"],
                [Modes.Mode.a, Modes.Mode.r],
                ["safety_issue", "recommendation"],
                (2001, "2020"),
            )

        with pytest.raises(TypeError):
            Searching.SearchSettings(["a", "r"], (2001, 2020))

    def test_to_dict(self):
        settings = Searching.SearchSettings(
            ["ATSB", "TSB"],
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
            "setting_agencies": "['ATSB', 'TSB']",
        }

    def test_from_dict(self):
        settings = Searching.SearchSettings.from_dict(
            {
                "setting_modes": "[0, 1]",
                "setting_max_year": 2020,
                "setting_min_year": 2000,
                "setting_document_types": "['safety_issue', 'recommendation']",
                "setting_relevanceCutoff": 0.1,
                "setting_agencies": "['ATSB', 'TSB']",
            }
        )

        assert settings.get_modes() == [Modes.Mode.a, Modes.Mode.r]
        assert settings.get_year_range() == (2000, 2020)
        assert settings.get_document_types() == ["safety_issue", "recommendation"]
        assert settings.get_relevance_cutoff() == 0.1


class TestSearch:
    def test_basic_creation(self):
        search = Searching.Search(
            "hello",
            Searching.SearchSettings(
                ["ATSB", "TSB"],
                [Modes.Mode.a, Modes.Mode.r],
                (2000, 2020),
                ["safety_issue", "recommendation"],
                0.1,
            ),
        )

        assert search.get_query() == "hello"
        assert search.get_settings().get_modes() == [Modes.Mode.a, Modes.Mode.r]
        assert search.get_settings().get_year_range() == (2000, 2020)
        assert search.get_settings().get_relevance_cutoff() == 0.1
        assert search.get_settings().get_document_types() == [
            "safety_issue",
            "recommendation",
        ]
        assert search.get_settings().get_agencies() == ["ATSB", "TSB"]

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
            "includeATSB": "on",
            "includeTAIC": "on",
        }

        search = Searching.Search.from_form(form)

        assert search.get_query() == "hello"
        assert search.get_settings().get_modes() == [Modes.Mode.a, Modes.Mode.r]
        assert search.get_settings().get_year_range() == (2000, 2020)
        assert search.get_settings().get_relevance_cutoff() == 0.1
        assert search.get_settings().get_document_types() == [
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
                ["ATSB", "TSB"],
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
        assert parsed_params["includeReportText"][0] == "off"
        assert parsed_params["includeATSB"][0] == "on"
        assert parsed_params["includeTSB"][0] == "on"
        assert parsed_params["includeTAIC"][0] == "off"


@pytest.fixture(scope="session")
def searcher() -> Searching.SearchEngine:
    return Searching.SearchEngine(os.environ["db_URI"])


class TestSearcher:
    def get_return_value(self, generator):
        try:
            while True:
                next(generator)
        except StopIteration as e:
            return e.value

    def test_search(self, searcher: Searching.SearchEngine):
        search = Searching.Search(
            "hello",
            Searching.SearchSettings(
                ["ATSB", "TSB"],
                [Modes.Mode.a, Modes.Mode.r],
                (2000, 2020),
                ["safety_issue", "recommendation"],
                0.1,
            ),
        )

        result = self.get_return_value(searcher.search(search, with_rag=False))

        assert result
        assert result.get_summary() is None
        assert isinstance(result.get_search_duration_str(), str)
        assert isinstance(result.get_context(), pd.DataFrame)

    def test_search_with_summary(self, searcher):
        search = Searching.Search(
            "pilot incapacity",
            Searching.SearchSettings(
                ["ATSB", "TAIC", "TSB"],
                Modes.all_modes,
                (2000, 2020),
                ["safety_issue", "recommendation"],
                0.6,
            ),
        )
        result = self.get_return_value(searcher.search(search, with_rag=True))

        assert result
        assert isinstance(result.get_summary(), str)
        assert isinstance(result.get_context(), pd.DataFrame)

    @pytest.mark.parametrize(
        "query, agencies, modes, years, filters, relevance_cutoff, expected_modes, expected_years",
        [
            (
                "pilot",
                ["TAIC"],
                [Modes.Mode.a, Modes.Mode.r],
                (2010, 2015),
                ["safety_issue", "recommendation"],
                0.7,
                ["0", "1"],
                [2010, 2011, 2012, 2013, 2014, 2015],
            ),
            (
                "pilot",
                ["ATSB", "TSB"],
                [Modes.Mode.a],
                (2010, 2015),
                ["safety_issue", "recommendation"],
                0.5,
                ["0"],
                [2010, 2011, 2012, 2013, 2014, 2015],
            ),
            (
                "",
                ["ATSB", "TSB", "TAIC"],
                [Modes.Mode.m, Modes.Mode.r],
                (2002, 2005),
                ["safety_issue", "recommendation"],
                0.6,
                ["2", "1"],
                [2002, 2003, 2004, 2005],
            ),
        ],
    )
    def test_filtered_search(
        self,
        query,
        agencies,
        modes,
        years,
        filters,
        relevance_cutoff,
        expected_modes,
        expected_years,
        searcher,
    ):
        search = Searching.Search(
            query,
            Searching.SearchSettings(
                agencies,
                modes,
                years,
                filters,
                relevance_cutoff,
            ),
        )

        if query == "":
            assert search.get_search_type() == "none"

        result = self.get_return_value(searcher.search(search, with_rag=False))

        assert result
        assert result.get_summary() is None
        assert isinstance(result.get_context(), pd.DataFrame)

        assert result.get_context()["year"].isin(expected_years).all()
        assert result.get_context()["mode"].isin(expected_modes).all()

    def test_search_without_results(self, searcher):
        search = Searching.Search(
            "hello",
            Searching.SearchSettings(
                ["ATSB", "TSB"],
                [Modes.Mode.a, Modes.Mode.r],
                (1897, 1899),
                ["safety_issue", "recommendation"],
                0.6,
            ),
        )
        assert search.get_search_type() == "vector"
        result = self.get_return_value(searcher.search(search, with_rag=False))

        assert result
        assert result.get_summary() is None
        assert result.get_context().empty

    def test_fts_search(self, searcher):
        search = Searching.Search(
            '"work"',
            Searching.SearchSettings(
                ["TAIC"],
                Modes.all_modes,
                (2000, 2020),
                ["safety_issue", "recommendation", "report_section", "report_text"],
                0.6,
            ),
        )
        assert search.get_search_type() == "fts"
        result = self.get_return_value(searcher.search(search, with_rag=False))

        assert result
        assert result.get_summary() is None

        assert result.get_context().shape[0] == 45

    def test_fts_search_no_results(self, searcher):
        search = Searching.Search(
            '""More work needed""',
            Searching.SearchSettings(
                ["TAIC"],
                Modes.all_modes,
                (2000, 2020),
                ["safety_issue", "recommendation"],
                0.3,
            ),
        )
        assert search.get_search_type() == "fts"
        result = self.get_return_value(searcher.search(search, with_rag=False))

        assert result
        assert result.get_summary() is None

        assert result.get_context().shape[0] == 0
