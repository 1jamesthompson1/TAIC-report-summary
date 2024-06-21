import pandas as pd
import pytest

import engine.utils.Modes as Modes
import viewer.Searching as Searching


class TestSearchSettings:
    def test_basic_creation(self):
        settings = Searching.SearchSettings([Modes.Mode.a, Modes.Mode.r], (2000, 2020))

        assert settings.getModes() == [Modes.Mode.a, Modes.Mode.r]
        assert settings.getYearRange() == (2000, 2020)

    def test_failed_creation(self):
        with pytest.raises(TypeError):
            Searching.SearchSettings([Modes.Mode.a, Modes.Mode.r], (2001, "2020"))

        with pytest.raises(TypeError):
            Searching.SearchSettings(["a", "r"], (2001, 2020))


class TestSearch:
    def test_basic_creation(self):
        search = Searching.Search(
            "hello",
            Searching.SearchSettings([Modes.Mode.a, Modes.Mode.r], (2000, 2020)),
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
        }

        search = Searching.Search.from_form(form)

        assert search.getQuery() == "hello"
        assert search.getSettings().getModes() == [Modes.Mode.a, Modes.Mode.r]
        assert search.getSettings().getYearRange() == (2000, 2020)

    def test_failed_form_creation(self):
        form = {"query": "hello"}

        with pytest.raises(ValueError):
            Searching.Search.from_form(form)


class TestSearcher:
    searcher = Searching.SearchEngine("./tests/data/vector_db")

    def test_search(self):
        search = Searching.Search(
            "hello",
            Searching.SearchSettings([Modes.Mode.a, Modes.Mode.r], (2000, 2020)),
        )
        result = self.searcher.search(search, with_rag=False)

        assert result
        assert result.getSummary() is None
        assert isinstance(result.getContext(), pd.DataFrame)

    def test_search_with_summary(self):
        search = Searching.Search(
            "pilot incapacity", Searching.SearchSettings(Modes.all_modes, (2000, 2020))
        )
        result = self.searcher.search(search, with_rag=True)

        assert result
        assert isinstance(result.getSummary(), str)
        assert isinstance(result.getContext(), pd.DataFrame)

    def test_filtered_search(self):
        search = Searching.Search(
            "pilot",
            Searching.SearchSettings([Modes.Mode.a, Modes.Mode.r], (2010, 2015)),
        )
        result = self.searcher.search(search, with_rag=False)

        assert result
        assert result.getSummary() is None
        assert isinstance(result.getContext(), pd.DataFrame)

        assert (
            result.getContext()["year"].isin([2010, 2011, 2012, 2013, 2014, 2015]).all()
        )

        assert result.getContext()["mode"].isin([0, 1]).all()

    def test_filtered_search_single_mode(self):
        search = Searching.Search(
            "pilot", Searching.SearchSettings([Modes.Mode.a], (2010, 2015))
        )
        result = self.searcher.search(search, with_rag=False)

        assert result
        assert result.getSummary() is None
        assert isinstance(result.getContext(), pd.DataFrame)

        assert (
            result.getContext()["year"].isin([2010, 2011, 2012, 2013, 2014, 2015]).all()
        )

        assert result.getContext()["mode"].isin([0]).all()

    def test_filtered_search_no_query(self):
        search = Searching.Search(
            "", Searching.SearchSettings([Modes.Mode.m, Modes.Mode.r], (2002, 2005))
        )

        result = self.searcher.search(search, with_rag=False)

        assert result
        assert result.getSummary() is None
        assert isinstance(result.getContext(), pd.DataFrame)

        assert result.getContext()["year"].isin([2002, 2003, 2004, 2005]).all()

        assert result.getContext()["mode"].isin([2, 1]).all()

    def test_search_without_results(self):
        search = Searching.Search(
            "hello",
            Searching.SearchSettings([Modes.Mode.a, Modes.Mode.r], (1997, 1999)),
        )
        result = self.searcher.search(search, with_rag=False)

        assert result
        assert result.getSummary() is None
        assert result.getContext().empty

    @pytest.mark.parametrize(
        "safety_issue_id, expected_num",
        [
            pytest.param("2001_007_1", 2),
            pytest.param("2002_007_2", 0),
        ],
    )
    def test_safety_issues_recommendation_retrieval(
        self, safety_issue_id: str, expected_num
    ):
        recommendations_df = self.searcher.get_recommendations_for_safety_issue(
            safety_issue_id
        )

        assert len(recommendations_df) == expected_num
