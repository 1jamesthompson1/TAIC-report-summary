import lancedb
import pandas as pd
import plotly.express as px
import voyageai

import engine.utils.Modes as Modes
from engine.utils.AICaller import AICaller


class SearchSettings:
    def __init__(
        self,
        modes: list[Modes.Mode],
        year_range: tuple[int, int],
        relevanceCutoff: float,
    ):
        """
        Initializes a new instance of the SearchSettings class.
        These are all the settings that the `Search` class will have that the `Searcher` class will use.

        Parameters:
            modes (List[Modes.Mode]): A list of Modes.Mode objects representing the modes to be included in the search.
            year_range (Tuple[int, int]): A tuple representing the range of years to be included in the search.

        """
        if not isinstance(year_range[0], int) or not isinstance(year_range[1], int):
            raise TypeError("year_range must be a tuple of integers")
        self.year_range = year_range

        if not isinstance(modes, list) or not all(
            isinstance(mode, Modes.Mode) for mode in modes
        ):
            raise TypeError("modes must be a list of Modes.Mode objects")
        self.modes = modes

        if not isinstance(relevanceCutoff, float):
            raise TypeError("relevanceCutoff must be an integer")
        self.relevanceCutoff = relevanceCutoff

    def getYearRange(self) -> tuple[int, int]:
        return self.year_range

    def getModes(self) -> list[Modes.Mode]:
        return self.modes

    def getRelevanceCutoff(self) -> int:
        return self.relevanceCutoff


class Search:
    def __init__(self, query: str, settings: SearchSettings):
        self.query = query
        self.settings = settings

    @classmethod
    def from_form(cls, form: dict):
        if form is None or not isinstance(form, dict):
            raise TypeError(f"Form data is not a dictionary but {type(form)}")
        if len(form) == 0:
            raise ValueError("Form data is empty")

        try:
            # Query
            search_query = form["searchQuery"]

            # Modes
            modes_list = list()

            if "includeModeAviation" in form.keys():
                modes_list.append(Modes.Mode.a)
            if "includeModeRail" in form.keys():
                modes_list.append(Modes.Mode.r)
            if "includeModeMarine" in form.keys():
                modes_list.append(Modes.Mode.m)

            # Year
            year_range = (
                int(form.get("yearSlider-min")),
                int(form.get("yearSlider-max")),
            )

            # Relevance

            relevance_cutoff = float(form.get("relevanceCutoff", 0))

            print(form)

            return cls(
                search_query,
                settings=SearchSettings(modes_list, year_range, relevance_cutoff),
            )
        except KeyError as e:
            raise ValueError(f"Form data is missing key: {e}")

    def getQuery(self) -> str:
        return self.query

    def getSettings(self) -> SearchSettings:
        return self.settings


class SearchResult:
    def __init__(self, context: pd.DataFrame, summary: str = None):
        self.context = context
        self.summary = summary

    def getContext(self) -> pd.DataFrame:
        return self.context

    def getContextCleaned(self) -> pd.DataFrame:
        """
        This method retrieves the context dataframe and makes sure that it has a standard format.
        """
        context_df = self.getContext().copy()

        context_df.rename(
            columns={"si": "safety_issue", "section_relevance_score": "relevance"},
            inplace=True,
        )

        context_df["relevance"] = context_df["relevance"].apply(lambda x: f"{x:.4f}")

        context_df = context_df[
            [
                "relevance",
                "report_id",
                "type",
                "safety_issue_id",
                "safety_issue",
                "recommendations",
                "year",
                "mode",
            ]
        ]

        # Make mode a string
        context_df["mode"] = context_df["mode"].apply(
            lambda x: Modes.Mode.as_string(Modes.Mode(x))
        )
        return context_df

    def addVisualLayout(self, fig):
        fig = fig.update_layout(width=500)

        # If fig a pie chart
        if fig.data[0].type == "pie":
            fig.update_traces(
                textposition="inside",
                textinfo="percent+label",
                insidetextorientation="radial",
            )

            # Remove legend
            fig.update_layout(showlegend=False)

        return fig

    def getModePieChart(self):
        context_df = self.getContextCleaned()["mode"].value_counts().reset_index()
        context_df.columns = ["mode", "count"]
        fig = px.pie(
            context_df,
            values="count",
            names="mode",
            title="Mode distribution in search results",
        )

        return self.addVisualLayout(fig)

    def getYearHistogram(self):
        context_df = self.getContextCleaned()
        fig = px.histogram(
            context_df,
            x="year",
            title="Year distribution in search results",
        )
        return self.addVisualLayout(fig)

    def getMostCommonEventTypes(self):
        context_df = self.getContextCleaned()
        type_counts = context_df.groupby("type")["safety_issue"].count()

        top_5_types = type_counts.nlargest(5).reset_index()

        others_count = type_counts[~type_counts.index.isin(top_5_types["type"])].sum()

        combined_df = pd.concat(
            [
                top_5_types,
                pd.DataFrame(
                    [["Others", others_count]], columns=["type", "safety_issue"]
                ),
            ],
            ignore_index=True,
        )

        combined_df.columns = ["Event type", "Count"]

        fig = px.pie(
            combined_df,
            values="Count",
            names="Event type",
            title="Top 5 most common event types in search results",
        )

        return self.addVisualLayout(fig)

    def getSummary(self) -> str | None:
        return self.summary


class SearchEngine:
    def __init__(self, db_uri: str):
        self.db = lancedb.connect(db_uri)

        self.si_table = self.db.open_table("safety_issue_embeddings")
        self.report_sections_table = self.db.open_table("report_section_embeddings")

        self.safety_issue_recommendations = self.db.open_table(
            "safety_issue_recommendations"
        )

        self.vo = voyageai.Client()

    def search(self, search: Search, with_rag=True) -> SearchResult:
        """
        This function takes a search object with some parameters and will create the right `SearchEngineSearcher`
        """

        searchEngineSearcher = SearchEngineSearcher(
            search, self.si_table, self.report_sections_table, self.vo
        )

        response = None

        if with_rag and search.getQuery() != "":
            response = searchEngineSearcher.rag_search()
        else:
            results = searchEngineSearcher.safety_issue_search_with_report_relevance()
            response = SearchResult(results, None)

        response.getContext()["recommendations"] = response.getContext()[
            "recommendations"
        ].apply(len)

        return response

    def get_recommendations_for_safety_issue(self, safety_issue_id: str):
        search_results = (
            self.si_table.search()
            .where(f"safety_issue_id == '{safety_issue_id}'")
            .limit(1)
            .select(["recommendations"])
            .to_list()
        )

        if len(search_results) == 0:
            return []

        return search_results[0]["recommendations"]


class SearchEngineSearcher:
    def __init__(
        self,
        search: Search,
        si_table: lancedb.table.Table,
        report_sections_table: lancedb.table.Table,
        vo: voyageai.Client,
    ):
        self.query = search.getQuery()
        self.settings = search.getSettings()

        self.report_sections_table = report_sections_table
        self.si_table = si_table

        self.vo = vo

    def _embed_query(self, query: str) -> list[float]:
        return self.vo.embed(
            query, model="voyage-large-2-instruct", input_type="query", truncation=False
        ).embeddings[0]

    def _table_search(
        self,
        filter: str,
        table: lancedb.table.Table,
        limit=100,
        type: str = ["hybrid", "fts", "vector"],
    ) -> pd.DataFrame:
        if type == "hybrid":
            results = (
                table.search(
                    (self._embed_query(self.query), self.query), query_type="hybrid"
                )
                .metric("cosine")
                .where(filter, prefilter=True)
                .limit(limit)
                .to_pandas()
            )
            results.rename(
                columns={"_relevance_score": "section_relevance_score"}, inplace=True
            )
        elif type == "fts":
            results = (
                table.search(self.query, query_type="fts")
                .limit(limit)
                .where(filter, prefilter=True)
                .to_pandas()
            )
            results.rename(columns={"score": "section_relevance_score"}, inplace=True)
        else:  # type == 'vector'
            results = (
                table.search(self._embed_query(self.query), query_type="vector")
                .metric("cosine")
                .limit(limit)
                .where(filter, prefilter=True)
                .to_pandas()
            )
            results.rename(
                columns={"_distance": "section_relevance_score"}, inplace=True
            )
            # THe section relevance score should always be ascending. Therefore I need to flip the distance which is ascending.
            results["section_relevance_score"] = 1 - results["section_relevance_score"]

        results.sort_values(by="section_relevance_score", ascending=False, inplace=True)

        return results

    def safety_issue_search_with_report_relevance(self) -> pd.DataFrame:
        where_statement = " AND ".join(
            [
                f"year >= {str(self.settings.getYearRange()[0])} AND year <= {str(self.settings.getYearRange()[1])}",
                f"mode IN {tuple([mode.value for mode in self.settings.getModes()])}"
                if len(self.settings.getModes()) > 1
                else f"mode = {self.settings.getModes()[0].value}",
            ]
        )
        if self.query == "" or self.query is None:
            return (
                self.si_table.search()
                .limit(None)
                .where(where_statement, prefilter=True)
                .to_pandas()
                .assign(section_relevance_score=0)
            )

        report_sections_search_results = self._table_search(
            table=self.report_sections_table,
            limit=50000,
            filter=where_statement,
            type="fts",
        )
        report_sections_search_results["section_relevance_score"] = (
            report_sections_search_results["section_relevance_score"]
            - report_sections_search_results["section_relevance_score"].min()
        ) / (
            report_sections_search_results["section_relevance_score"].max()
            - report_sections_search_results["section_relevance_score"].min()
        )
        if report_sections_search_results.shape[0] == 0:
            return self._table_search(
                table=self.si_table, type="vector", filter=where_statement, limit=500
            )

        reports_relevance = (
            report_sections_search_results.groupby("report_id")
            .head(50)
            .groupby("report_id")["section_relevance_score"]
            .mean()
            .sort_values(ascending=False)
            .to_dict()
        )

        safety_issues_search_results = self._table_search(
            table=self.si_table, type="vector", filter=where_statement, limit=500
        )

        safety_issues_search_results["section_relevance_score"] = (
            safety_issues_search_results.apply(
                lambda row: row["section_relevance_score"]
                * (
                    reports_relevance[row["report_id"]]
                    if row["report_id"] in reports_relevance
                    else 0
                ),
                axis=1,
            )
        )

        safety_issues_search_results.sort_values(
            by="section_relevance_score", inplace=True, ascending=False
        )

        safety_issues_search_results.reset_index(drop=False, inplace=True)

        # reranked = self._reranked_results(safety_issues_search_results)

        # return reranked
        return safety_issues_search_results

    def _reranked_results(self, results: pd.DataFrame) -> pd.DataFrame:
        """
        This method will take the results from the `safety_issue_search_with_report_relevance` and will filter the results ot only show the most relevant. Safety issues.
        """

        top_results = results.head(200)

        reranking_results = self.vo.rerank(
            documents=top_results["safety_issue"].tolist(),
            query=self.query,
            model="rerank-1",
            truncation=False,
        ).results

        reranked_indices = [result.index for result in reranking_results]
        reranked_relevance_scores = [
            result.relevance_score for result in reranking_results
        ]

        top_results.loc[reranked_indices, "section_relevance_score"] = (
            reranked_relevance_scores
        )

        top_results.sort_values(
            by="section_relevance_score", ascending=False, inplace=True
        )

        top_results.reset_index(drop=False, inplace=True)

        return top_results

    def _filter_results(self, results: pd.DataFrame) -> pd.DataFrame:
        return results.query(
            f"section_relevance_score > {self.settings.getRelevanceCutoff()}"
        )

    def _get_rag_prompt(self, query: str, search_results: pd.DataFrame):
        context = "\n\n".join(
            f"""{id} from report {report} of type {type} with relevance {rel:.4f}:
'{si}'
{f"This safety issue had recommendations: {recs}" if recs != "" else "No recommendations were made"}
"""
            for id, report, type, si, rel, recs in zip(
                search_results["safety_issue_id"],
                search_results["report_id"],
                search_results["type"],
                search_results["safety_issue"],
                search_results["section_relevance_score"],
                [
                    "\n".join(
                        [
                            f"Recommendation {rec['recommendation_id']} made to {rec['recipient']}: {rec['recommendation']}"
                            for rec in recs
                        ]
                    )
                    for recs in search_results["recommendations"]
                ],
            )
        )
        return f"""
        My question is: {query}

        Use the following pieces of retrieved context and your common knowledge to answer the question. If you don't know the answer, just say that you don't know.
        {context}

        It is important to provide references to specific reports and safety issues in your answer.
        Remember to keep your answer concise and no longer than a few sentences. If you feel it necessary you can format our answer with markdown but no links.
        """

    def rag_search(self):
        print(("Understanding query..."))

        formatted_query = AICaller.query(
            system="""
    You will receive a query from the user and return a query that is optimized for a vector search of a safety issue and recommendation database.

    The vector database is an embedded dataset of safety issues from the New Zealand Transport Accident Investigation Commission.

    Please don't include the words "report" or "safety issue" in your query.

    A couple of useful definitions for you are:

    Safety factor - Any (non-trivial) events or conditions, which increases safety risk. If they occurred in the future, these would
    increase the likelihood of an occurrence, and/or the
    severity of any adverse consequences associated with the
    occurrence.

    Safety issue - A safety factor that:
    • can reasonably be regarded as having the
    potential to adversely affect the safety of future
    operations, and
    • is characteristic of an organization, a system, or an
    operational environment at a specific point in time.
    Safety Issues are derived from safety factors classified
    either as Risk Controls or Organisational Influences.

    Safety theme - Indication of recurring circumstances or causes, either across transport modes or over time. A safety theme may
    cover a single safety issue, or two or more related safety
    issues.  
    """,
            user=self.query,
            model="gpt-4",
            temp=0.0,
        )
        print(f' Going to run query: "{formatted_query}"')

        print("Getting relevant safety issues...")
        original_query = self.query
        self.query = formatted_query

        search_results = self.safety_issue_search_with_report_relevance()
        search_results = self._filter_results(search_results)

        print("Summarizing relevant safety issues...")
        response = AICaller.query(
            system="""
    You are a helpful AI that is part of a RAG system. You are going to help answer questions about transport accident investigations.

    The questions are from investigators and researchers from the Transport Accident Investigation Commission. The context you will be given are safety issues extracted from all of TAICs reports.

    You will be given a question and some context documents. You will then need to answer the question as best you can. It might be possible that hte question can't be answered with the given context which you should just say that.

    A couple of useful definitions for you are:

    Safety factor - Any (non-trivial) events or conditions, which increases safety risk. If they occurred in the future, these would
    increase the likelihood of an occurrence, and/or the
    severity of any adverse consequences associated with the
    occurrence.

    Safety issue - A safety factor that:
    • can reasonably be regarded as having the
    potential to adversely affect the safety of future
    operations, and
    • is characteristic of an organization, a system, or an
    operational environment at a specific point in time.
    Safety Issues are derived from safety factors classified
    either as Risk Controls or Organisational Influences.

    Safety theme - Indication of recurring circumstances or causes, either across transport modes or over time. A safety theme may
    cover a single safety issue, or two or more related safety
    issues.  
    """,
            user=self._get_rag_prompt(original_query, search_results),
            model="gpt-4",
            temp=0,
            max_tokens=8000,
        )
        formatted_response = f"""Query made to the database was: '{self.query}'

{response}
        """
        return SearchResult(search_results, formatted_response)
