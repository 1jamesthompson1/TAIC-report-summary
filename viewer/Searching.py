
# Local
from engine.OpenAICaller import openAICaller
import engine.Modes as Modes

# Third party
import pandas as pd
import lancedb
import voyageai

# built in
from typing import Callable


class SearchSettings:
    def __init__(self, modes: list[Modes.Mode], year_range: tuple[int, int]):
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
        
        if not isinstance(modes, list) or not all(isinstance(mode, Modes.Mode) for mode in modes):
            raise TypeError("modes must be a list of Modes.Mode objects")
        self.modes = modes

    def getYearRange(self) -> tuple[int, int]:
        return self.year_range

    def getModes(self) -> list[Modes.Mode]:
        return self.modes

class Search:
    def __init__(self, query: str, settings: SearchSettings):
        self.query = query
        self.settings = settings
    @classmethod
    def from_form(cls, form: dict):
        if form is None or not isinstance(form, dict):
            raise TypeError(f"Form data is not a dictionary but {type(form)}")
        if len(form) == 0:
            raise ValueError(f"Form data is empty")

        try:
            # Query
            search_query = form['searchQuery']

            # Modes
            modes_list = list()

            if form['includeModeAviation'] == "on":
                modes_list.append(Modes.Mode.a)
            if form['includeModeRail'] == "on":
                modes_list.append(Modes.Mode.r)
            if form['includeModeMarine'] == "on":
                modes_list.append(Modes.Mode.m)

            # Year
            year_range = int(form.get('yearSlider-min')), int(form.get('yearSlider-max'))
            return cls(search_query, settings = SearchSettings(modes_list, year_range))
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

        context_df.rename(columns = {'si': 'safety_issue', '_distance': 'relevance'}, inplace = True)
        context_df['relevance'] = 1- context_df['relevance'] 

        context_df = context_df[['relevance', 'report_id', 'safety_issue_id', 'safety_issue', 'year']]
        return context_df
    def getSummary(self) -> str | None:
        return self.summary

class Searcher:
    def __init__(self, db_uri: str):
        self.db = lancedb.connect(db_uri)

        self.report_sections_table = self.db.open_table('report_section_embeddings')
        self.si_table = self.db.open_table('safety_issue_embeddings')

        self.vo = voyageai.Client()

    def search(self, search: Search, with_rag = True) -> SearchResult:

        if search.getQuery() == "":
            return SearchResult(
                self.si_table.search().limit(None).to_pandas().assign(relevance=1),
                "Not applicable as no query was given"
            )

        if with_rag:
            return RAG().rag_search(search.getQuery(), self.safety_issue_search_with_report_relevance)
        else:
            results = self.safety_issue_search_with_report_relevance(search.getQuery())
            return SearchResult(results, None)
    
    def _embed_query(self, query: str) -> list[float]:

        return self.vo.embed(query, model="voyage-large-2-instruct", input_type="query", truncation=False).embeddings[0]
        
    def _table_search(self, table: lancedb.table.Table, query: str, limit = 100, type: str = ['hybrid', 'fts', 'vector']) -> pd.DataFrame:
        if type == 'hybrid':
            results = table.search((self._embed_query(query), query),  query_type='hybrid') \
                .metric("cosine") \
                .limit(limit) \
                .to_pandas()
            results.rename(columns={'_relevance_score': 'section_relevance_score'}, inplace = True)
        elif type == 'fts':
            results = table.search(query,  query_type='fts') \
                .limit(limit) \
                .to_pandas()
            results.rename(columns={'score': 'section_relevance_score'}, inplace = True)
        else: # type == 'vector'
            results = table.search(self._embed_query(query),  query_type='vector') \
                .metric("cosine") \
                .limit(limit) \
                .to_pandas()
            results.rename(columns={'_distance': 'section_relevance_score'}, inplace = True)
            results['section_relevance_score'] = 1 - results['section_relevance_score']

        results['section_relevance_score'] = (results['section_relevance_score'] - results['section_relevance_score'].min()) / (results['section_relevance_score'].max() - results['section_relevance_score'].min())

        return results

    def safety_issue_search_with_report_relevance(self, query: str) -> pd.DataFrame:
        
        report_sections_search_results = self._table_search(query = query, table = self.report_sections_table, limit = 5000, type = 'fts')

        print(report_sections_search_results)
        reports_relevance =  report_sections_search_results.groupby('report_id').head(50).groupby('report_id')['section_relevance_score'].mean().sort_values(ascending=False).to_dict()

        safety_issues_search = lambda query, limit: self.si_table.search(self._embed_query(query)) \
            .metric("cosine") \
            .where("year > 2006", prefilter=True) \
            .limit(limit) \
            .to_pandas()

        safety_issues_search_results = safety_issues_search(query, limit = 500)

        safety_issues_search_results['_distance'] = safety_issues_search_results.apply(lambda row: row['_distance'] * (1 -reports_relevance[row['report_id']] if row['report_id'] in reports_relevance else 1), axis = 1)

        safety_issues_search_results.sort_values(by = '_distance', inplace=True)

        safety_issues_search_results.reset_index(drop=False, inplace=True)

        return safety_issues_search_results

class RAG():
    def __init__(self):
        pass

    def get_rag_prompt(self, query: str, context: str):
        return f"""
        Use the following pieces of retrieved context to answer the question. If you don't know the answer, just say that you don't know.
        My question is: {query}

        Here are relevant safety issues as context:
        {context}

        It is important to provide references to specifc reports and safety issues in your answer.
        """

    def rag_search(self, query, search_function: Callable[[str], pd.DataFrame]):

        print((f"Understanding query..."))

        formatted_query = openAICaller.query(
            system = """
    You are a helpful agent inside a RAG system.

    You will recieve a query from the user and return a query that should be sent to a vector database.

    The database will search a dataset of safety issues from transport accident investigation reports.  It will use both embeddings and full text search.
    """,
            user = query,
            model="gpt-4",
            temp = 0.0
        )
        print(f' Going to run query: "{formatted_query}"')

        print(f"Getting relevant safety issues...")
        
        search_results = search_function(formatted_query).head(50)

        user_message = "\n".join(f"{id} from report {report} with relevance {rel} - {si}" for id, report, si, rel in zip(search_results['safety_issue_id'], search_results['report_id'], search_results['si'], search_results['_distance'])) 

        print(f"Summarizing relevant safety issues...")
        response = openAICaller.query(
            system = """
    You are a helpful AI that is part of a RAG system. You are going to help answer questions about transport accident investigations.

    The questions are from investigators and researchers from the Transport Accident Investigation Commission. The context you will be given are safety issues extracted from all of TAICs reports.

    A couple of useful defintions for you are:

    Safety factor - Any (non-trivial) events or conditions, which increases safety risk. If they occurred in the future, these would
    increase the likelihood of an occurrence, and/or the
    severity of any adverse consequences associated with the
    occurrence.

    Safety issue - A safety factor that:
    • can reasonably be regarded as having the
    potential to adversely affect the safety of future
    operations, and
    • is characteristic of an organisation, a system, or an
    operational environment at a specific point in time.
    Safety Issues are derived from safety factors classified
    either as Risk Controls or Organisational Influences.

    Safety theme - Indication of recurring circumstances or causes, either across transport modes or over time. A safety theme may
    cover a single safety issue, or two or more related safety
    issues.  
    """,       
        user=self.get_rag_prompt(query, user_message),
        model="gpt-4",
        temp = 0.2
        )
        return SearchResult(search_results, response)