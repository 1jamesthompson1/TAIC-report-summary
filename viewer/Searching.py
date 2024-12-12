import time
import urllib.parse
import uuid
from ast import literal_eval

import lancedb
import pandas as pd
import plotly.express as px

import engine.utils.Modes as Modes
from engine.analyze import Embedding
from engine.utils.AICaller import AICaller


class SearchSettings:
    def __init__(
        self,
        agencies: list[str],
        modes: list[Modes.Mode],
        year_range: tuple[int, int],
        document_types: list[str],
        relevanceCutoff: float,
    ):
        """
        Initializes a new instance of the SearchSettings class.
        These are all the settings that the `Search` class will have that the `Searcher` class will use.

        Parameters:
            modes (List[Modes.Mode]): A list of Modes.Mode objects representing the modes to be included in the search.
            year_range (Tuple[int, int]): A tuple representing the range of years to be included in the search.

        """
        if not isinstance(agencies, list) or not all(
            agency in ["TAIC", "ATSB", "TSB"] for agency in agencies
        ):
            raise TypeError("agencies must be a list of strings")
        if len(agencies) == 0:
            raise ValueError("agencies must contain at least one string")
        self.agencies = agencies

        if not isinstance(year_range[0], int) or not isinstance(year_range[1], int):
            raise TypeError("year_range must be a tuple of integers")
        self.year_range = year_range

        if not isinstance(modes, list) or not all(
            isinstance(mode, Modes.Mode) for mode in modes
        ):
            raise TypeError("modes must be a list of Modes.Mode objects")
        if len(modes) == 0:
            raise ValueError("modes must contain at least one Modes.Mode object")
        self.modes = modes

        if not isinstance(relevanceCutoff, float):
            raise TypeError("relevanceCutoff must be an float")
        self.relevanceCutoff = relevanceCutoff

        if not isinstance(document_types, list) or not all(
            isinstance(document_type, str) for document_type in document_types
        ):
            raise TypeError("document_types must be a list of strings")
        if len(document_types) == 0:
            raise ValueError("document_types must contain at least one string")
        self.document_types = document_types

    def get_agencies(self) -> list[str]:
        return self.agencies

    def get_year_range(self) -> tuple[int, int]:
        return self.year_range

    def get_modes(self) -> list[Modes.Mode]:
        return self.modes

    def get_relevance_cutoff(self) -> int:
        return self.relevanceCutoff

    def get_document_types(self) -> list[str]:
        return self.document_types

    def to_dict(self) -> dict:
        return {
            "setting_min_year": self.year_range[0],
            "setting_max_year": self.year_range[1],
            "setting_modes": str([mode.value for mode in self.modes]),
            "setting_relevanceCutoff": self.relevanceCutoff,
            "setting_document_types": str(self.document_types),
            "setting_agencies": str(self.agencies),
        }

    @classmethod
    def from_dict(cls, data: dict):
        if data is None or not isinstance(data, dict):
            raise TypeError(f"Data is not a dictionary but {type(data)}")
        return cls(
            modes=[
                Modes.Mode(mode)
                for mode in (
                    literal_eval(data["setting_modes"])
                    if isinstance(data["setting_modes"], str)
                    else data["setting_modes"]
                )
            ],
            year_range=(int(data["setting_min_year"]), int(data["setting_max_year"])),
            document_types=literal_eval(data["setting_document_types"])
            if isinstance(data["setting_document_types"], str)
            else data["setting_document_types"],
            relevanceCutoff=float(data["setting_relevanceCutoff"]),
            agencies=literal_eval(data["setting_agencies"])
            if isinstance(data["setting_agencies"], str)
            else data["setting_agencies"],
        )


class Search:
    def __init__(self, query: str, settings: SearchSettings):
        self.query = query
        self.settings = settings
        self.creation_time = time.time()
        self.uuid = uuid.uuid4()

        self.search_type = (
            "none"
            if (query == "" or query is None)
            else ("fts" if query[0] == '"' and query[-1] == '"' else "vector")
        )

    @classmethod
    def from_form(cls, form: dict):
        if form is None or not isinstance(form, dict):
            raise TypeError(f"Form data is not a dictionary but {type(form)}")
        if len(form) == 0:
            raise ValueError("Form data is empty")

        try:
            # Query
            search_query = form["searchQuery"]

            # Agencies
            agencies_list = list()

            if "includeTAIC" in form.keys():
                agencies_list.append("TAIC")
            if "includeATSB" in form.keys():
                agencies_list.append("ATSB")
            if "includeTSB" in form.keys():
                agencies_list.append("TSB")

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

            # Document Types
            document_types = list()
            if "includeSafetyIssues" in form.keys():
                document_types.append("safety_issue")
            if "includeRecommendations" in form.keys():
                document_types.append("recommendation")
            if "includeReportSection" in form.keys():
                document_types.append("report_section")
            if "includeReportText" in form.keys():
                document_types.append("report_text")

            return cls(
                search_query,
                settings=SearchSettings(
                    agencies_list,
                    modes_list,
                    year_range,
                    document_types,
                    relevance_cutoff,
                ),
            )
        except KeyError as e:
            raise ValueError(f"Form data is missing key: {e}")

    def get_query(self) -> str:
        return self.query

    def get_settings(self) -> SearchSettings:
        return self.settings

    def get_search_type(self) -> str:
        return self.search_type

    def get_start_time(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.creation_time))

    def to_url_params(self) -> str:
        params = {
            "searchQuery": self.query,
            "yearSlider-min": self.settings.year_range[0],
            "yearSlider-max": self.settings.year_range[1],
            "relevanceCutoff": self.settings.relevanceCutoff,
        }

        params["includeModeAviation"] = (
            "on" if Modes.Mode.a in self.settings.modes else "off"
        )
        params["includeModeRail"] = (
            "on" if Modes.Mode.r in self.settings.modes else "off"
        )
        params["includeModeMarine"] = (
            "on" if Modes.Mode.m in self.settings.modes else "off"
        )

        params["includeSafetyIssues"] = (
            "on" if "safety_issue" in self.settings.document_types else "off"
        )
        params["includeRecommendations"] = (
            "on" if "recommendation" in self.settings.document_types else "off"
        )
        params["includeReportSection"] = (
            "on" if "report_section" in self.settings.document_types else "off"
        )
        params["includeReportText"] = (
            "on" if "report_text" in self.settings.document_types else "off"
        )

        params["includeTAIC"] = "on" if "TAIC" in self.settings.agencies else "off"
        params["includeATSB"] = "on" if "ATSB" in self.settings.agencies else "off"
        params["includeTSB"] = "on" if "TSB" in self.settings.agencies else "off"

        return urllib.parse.urlencode(params)


class SearchResult:
    def __init__(self, search: Search, context: pd.DataFrame, summary: str = None):
        self.search = search
        self.context = context
        self.summary = summary
        self.duration = time.time() - search.creation_time

        self.context_required_columns = [
            "relevance",
            "document_type",
            "document_id",
            "report_id",
            "type",
            "document",
            "year",
            "mode",
            "agency",
            "agency_id",
            "url",
        ]

    def get_search_duration(self) -> str:
        return time.strftime("%M:%S", time.gmtime(self.duration))

    def get_context(self) -> pd.DataFrame:
        if self.context is None:
            return pd.DataFrame(columns=self.context_required_columns)
        return self.context

    def get_context_cleaned(self) -> pd.DataFrame:
        """
        This method retrieves the context dataframe and makes sure that it has a standard format.
        """
        context_df = self.get_context().copy()

        context_df.rename(
            columns={"section_relevance_score": "relevance"},
            inplace=True,
        )

        context_df["relevance"] = context_df["relevance"].apply(lambda x: f"{x:.4f}")

        context_df["document"] = context_df["document"].apply(
            lambda doc: doc
            if len(doc) < 1200
            else doc[:1200] + "... (Document too long to display)"
        )

        context_df = context_df[self.context_required_columns]

        # Make mode a string
        context_df["mode"] = context_df["mode"].apply(
            lambda x: Modes.Mode.as_string(Modes.Mode(int(x)))
        )
        return context_df

    def add_visual_layout(self, fig):
        fig = fig.update_layout(width=310)

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

    def get_document_type_pie_chart(self):
        context_df = (
            self.get_context_cleaned()["document_type"].value_counts().reset_index()
        )
        context_df.columns = ["document_type", "count"]
        fig = px.pie(
            context_df,
            values="count",
            names="document_type",
            title="Document type distribution",
        )

        return self.add_visual_layout(fig)

    def get_mode_pie_chart(self):
        context_df = self.get_context_cleaned()["mode"].value_counts().reset_index()
        context_df.columns = ["mode", "count"]
        fig = px.pie(
            context_df,
            values="count",
            names="mode",
            title="Mode distribution",
        )

        return self.add_visual_layout(fig)

    def get_year_histogram(self):
        context_df = self.get_context_cleaned()
        fig = px.histogram(
            context_df,
            x="year",
            title="Year distributions",
        )
        return self.add_visual_layout(fig)

    def get_most_common_event_types(self):
        context_df = self.get_context_cleaned()
        type_counts = context_df.groupby("type")["document"].count()

        top_5_types = type_counts.nlargest(5).reset_index()

        others_count = type_counts[~type_counts.index.isin(top_5_types["type"])].sum()

        combined_df = pd.concat(
            [
                top_5_types,
                pd.DataFrame([["Others", others_count]], columns=["type", "document"]),
            ],
            ignore_index=True,
        )

        combined_df.columns = ["Event type", "Count"]

        fig = px.pie(
            combined_df,
            values="Count",
            names="Event type",
            title="Top 5 most common event types",
        )

        return self.add_visual_layout(fig)

    def get_agency_pie_chart(self):
        context_df = self.get_context_cleaned()["agency"].value_counts().reset_index()
        context_df.columns = ["agency", "count"]
        fig = px.pie(
            context_df,
            values="count",
            names="agency",
            title="Agency distribution",
        )

        return self.add_visual_layout(fig)

    def get_summary(self) -> str | None:
        return self.summary


class SearchEngine:
    def __init__(self, db_uri: str, model="voyage-large-2-instruct"):
        self.db = lancedb.connect(db_uri)
        self.all_document_types_table = self.db.open_table("all_document_types")
        self.model = model

    def search(self, search: Search, with_rag=True) -> SearchResult:
        """
        This function takes a search object with some parameters and will create the right `SearchEngineSearcher`
        """

        searchEngineSearcher = SearchEngineSearcher(
            search, self.all_document_types_table, self.model
        )

        response = None
        # All situations that results in a search without rag
        if (
            search.get_query() == ""
            or search.get_query() is None
            or not with_rag
            or search.get_search_type() == "fts"
        ):
            results = searchEngineSearcher.search()
            response = SearchResult(search, results, None)
        # Otherwise do a rag search
        elif with_rag and search.get_query() != "":
            response = searchEngineSearcher.rag_search()

        print(
            f"Search engine response: {response.summary}, with context {response.context}"
        )
        return response


class SearchEngineSearcher:
    def __init__(
        self,
        search: Search,
        vector_db_table: lancedb.table.Table,
        embedding_model: str = "voyage-large-2-instruct",
    ):
        self.search_obj = search
        self.query = search.get_query()
        self.settings = search.get_settings()

        self.vector_db_table = vector_db_table

        self.embedder = Embedding.Embedder(embedding_model)

    def _table_search(
        self,
        filter: str,
        table: lancedb.table.Table,
        limit=100,
        type: str = ["hybrid", "fts", "vector"],
    ) -> pd.DataFrame:
        print(
            f"Conducting search with filter: {filter}, limit: {limit}, type: {type} for query: {self.query}"
        )
        if type == "hybrid":
            results = (
                table.search(
                    (self.embedder.embed_query(self.query), self.query),
                    query_type="hybrid",
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
                table.search(self.query[1:-1], query_type="fts")
                .limit(limit)
                .where(filter)
                .to_pandas()
            )
            results.rename(columns={"_score": "section_relevance_score"}, inplace=True)
        else:  # type == 'vector'
            results = (
                table.search(self.embedder.embed_query(self.query), query_type="vector")
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

    def search(self):
        where_statement = " AND ".join(
            [
                f"year >= {str(self.settings.get_year_range()[0])} AND year <= {str(self.settings.get_year_range()[1])}",
                f"document_type IN {tuple(self.settings.get_document_types())}"
                if len(self.settings.get_document_types()) > 1
                else f"document_type = '{self.settings.get_document_types()[0]}'",
                f"mode IN {tuple([str(mode.value) for mode in self.settings.get_modes()])}"
                if len(self.settings.get_modes()) > 1
                else f"mode = '{str(self.settings.get_modes()[0].value)}'",
                f"agency IN {tuple(self.settings.get_agencies())}"
                if len(self.settings.get_agencies()) > 1
                else f"agency = '{self.settings.get_agencies()[0]}'",
            ]
        )
        if self.search_obj.get_search_type() == "none":
            print(f"Searching for everything that matches {where_statement}")
            return (
                self.vector_db_table.search()
                .where(where_statement, prefilter=True)
                .limit(1_000_000)
                .to_pandas()
                .assign(section_relevance_score=0)
            )

        search_results = self._table_search(
            table=self.vector_db_table,
            type=self.search_obj.get_search_type(),
            filter=where_statement,
            limit=5000,
        )

        if len(search_results) == 0:
            return None
        return search_results

    def _filter_results(self, results: pd.DataFrame) -> pd.DataFrame:
        return results.query(
            f"section_relevance_score > {self.settings.get_relevance_cutoff()}"
        )

    def _get_rag_prompt(self, search: Search, search_results: pd.DataFrame):
        context = "\n\n".join(
            f"""{document_type}:{id} from report {report} of type {report_type} with relevance {rel:.4f}:
'{document}'
"""
            for id, report, report_type, document_type, document, rel in zip(
                search_results["document_id"],
                search_results["report_id"],
                search_results["type"],
                search_results["document_type"],
                search_results["document"],
                search_results["section_relevance_score"],
            )
        )
        return f"""
        My question is: {search.get_query()}

        Use the following pieces of retrieved context and your common knowledge to answer the question. Here is what my search settings looked like:
        
        {search.get_settings().to_dict()}

        If you don't know the answer, just say that you don't know.
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

    I don't want you to in any way change the meaning of the search just remove unnecessary words.

    "What are common elements in floatation devices and fires?" -> "Flotation devices and fires"

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
        self.query = formatted_query

        search_results = self.search()
        if search_results is None:
            return SearchResult(self.search_obj, None, None)
        search_results = self._filter_results(search_results)

        print(f"Summarizing {len(search_results)} documents...")
        response = AICaller.query(
            system="""
    You are a helpful AI that is part of a RAG system. You are going to help answer questions about transport accident investigations.

    The questions are from investigators and researchers from the Transport Accident Investigation Commission. The context you will be given are safety issues extracted from all of TAICs reports.

    You will be given a question and some context documents. You will then need to answer the question as best you can. It might be possible that the question can't be answered with the given context which you should just say that.

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
            user=self._get_rag_prompt(self.search_obj, search_results),
            model="claude-3.5-sonnet",
            temp=0,
            max_tokens=8096,
        )
        if response is None:
            response = "Too many relevant documents so could not summarize. Try increasing the relevance cutoff in search settings."
        formatted_response = f"""Query made to the database was: '{self.query}'

{response}
        """
        print("Got summary returning search result")

        return SearchResult(self.search_obj, search_results, formatted_response)
