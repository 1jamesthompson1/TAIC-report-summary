import viewer.search as search


class TestRegexCreation:
    
    def setup_method(self):
        self.searcher = search.Searcher()

    
    def test_basic(self):
        query = "hello"
        regex = search.Searcher.get_regex(search.Search(query, {'use_synonyms': False}))
        assert regex[0].pattern == r'hello'

    def test_basic_exact(self):
        query = '"hello"'
        regex = search.Searcher.get_regex(search.Search(query, {'use_synonyms': False}))
        assert regex[0].pattern == r'\b(hello)\b'

    def test_basic_or_word(self):
        query = "hello OR world"
        regex = search.Searcher.get_regex(search.Search(query, {'use_synonyms': False}))
        assert regex[0].pattern == r'hello|world'

    def test_basic_or_symbol(self):
        query = "hello | world"
        regex = search.Searcher.get_regex(search.Search(query, {'use_synonyms': False}))
        assert regex[0].pattern == r'hello|world'

    def test_basic_exclusion(self):
        query = "hello -world"
        regex = search.Searcher.get_regex(search.Search(query, {'use_synonyms': False}))
        assert regex[0].pattern == r'hello(?! world)'

    def test_prior_exclusion(self):
        query = "-not hello world"
        regex = search.Searcher.get_regex(search.Search(query, {'use_synonyms': False}))
        assert regex[0].pattern == r'(?<!not )hello world'

    def test_basic_wildcard(self):
        query = "hello*"
        regex = search.Searcher.get_regex(search.Search(query, {'use_synonyms': False}))
        assert regex[0].pattern == r'hello.*'

    def test_basic_and(self):
        query = "hello AND world"
        regexes = search.Searcher.get_regex(search.Search(query, {'use_synonyms': False}))
        regex_patterns = [regex.pattern for regex in regexes]
        assert regex_patterns == [r'hello', r'world']

    def test_and_or_combination(self):
        query = "hello AND world OR goodbye"
        regexes = search.Searcher.get_regex(search.Search(query, {'use_synonyms': False}))
        regex_patterns = [regex.pattern for regex in regexes]
        assert regex_patterns == [r'hello', r'world|goodbye']
    
class TestSearchReport:

    def setup_method(self):
        self.searcher = search.Searcher()

        self.example_report = """
This is a boat crash.

A boat crashed into a bridge.

It crashed into the bridge because the driver was drunk.

The driver almost drowned. The driver was not wearing a life jackets.
"""
        self.example_theme = """
Driving a vessel while drunk is dangerous.

Driving a boat while drunk is illegal.

Life jackets are important.
"""
        self.example_reasoning = [
            "This accident was caused by the driver being drunk. This is means it was the sole cause of the accident.",
            "The driver was not wearing a life jacket. This is means it was a contributing factor to the consequences of the accident."
        ]

        self.settings = {
            'use_synonyms': False,
            'search_report_text': True,
            'search_theme_text': True,
            'search_weighting_reasoning': True,
            'include_incomplete_reports': True,
        }
    
    def test_non_search(self):
        result = self.searcher.search_report(self.example_report, self.example_theme, self.example_reasoning, search.Search("", self.settings))
        assert result.matches == {}

    def test_basic_search(self):
        result = self.searcher.search_report(self.example_report, self.example_theme, self.example_reasoning, search.Search("boat", self.settings))
        assert result.matches == {
            'report_result': [2],
            'theme_result': [1],
            'weighting_reasoning_result': [0]
        }

    def test_basic_and_search(self):
        result = self.searcher.search_report(self.example_report, self.example_theme, self.example_reasoning, search.Search("boat AND drunk", self.settings))
        assert result.matches == {
            'report_result': [2,1],
            'theme_result': [1,2],
            'weighting_reasoning_result': [0,1]
        }

class TestSearchResult:
    
    def test_simple_include(self):
        result = search.SearchResult({
            'report_result': [2],
            'theme_result': [1],
            'weighting_reasoning_result': [1]
        })

        assert result.include() == True
    
    def test_simple_exclude(self):
        result = search.SearchResult({
            'report_result': [0],
            'theme_result': [0],
            'weighting_reasoning_result': [0]
        })

        assert result.include() == False

    def test_and_query_include(self):
        result = search.SearchResult({
            'report_result': [2,1],
            'theme_result': [1,2],
            'weighting_reasoning_result': [0,1]
        })

        assert result.include() == True

    def test_and_query_exclude(self):
        result = search.SearchResult({
            'report_result': [2,0],
            'theme_result': [1,0],
            'weighting_reasoning_result': [0,0]
        })

        assert result.include() == False

class TestHighlighting:
    
    def setup_method(self):
        self.searcher = search.Searcher()

        self.example_report = """
This is a boat crash.

A boat crashed into a bridge.

It crashed into the bridge because the driver was drunk.

The driver almost drowned. The driver was not wearing a life jackets.
"""
    
    def test_basic_highlight(self):
        regexes = search.Searcher.get_regex(search.Search("boat", {'use_synonyms': False}))

        highlights = self.searcher.highlight_matches(self.example_report, regexes)


        assert highlights == """
This is a <span class="match-highlight">boat</span> crash.

A <span class="match-highlight">boat</span> crashed into a bridge.

It crashed into the bridge because the driver was drunk.

The driver almost drowned. The driver was not wearing a life jackets.
"""
    def test_no_query_highlight(self):
        regexes = search.Searcher.get_regex(search.Search("", {'use_synonyms': False}))

        highlights = self.searcher.highlight_matches(self.example_report, regexes)


        assert highlights == self.example_report

    def test_and_query_highlight(self):
        regexes = search.Searcher.get_regex(search.Search("boat AND drunk", {'use_synonyms': False}))

        highlights = self.searcher.highlight_matches(self.example_report, regexes)


        assert highlights == """
This is a <span class="match-highlight">boat</span> crash.

A <span class="match-highlight">boat</span> crashed into a bridge.

It crashed into the bridge because the driver was <span class="match-highlight">drunk</span>.

The driver almost drowned. The driver was not wearing a life jackets.
"""

    def test_no_highlighting(self):
        regexes = search.Searcher.get_regex(search.Search("pizza", {'use_synonyms': False}))

        highlights = self.searcher.highlight_matches(self.example_report, regexes)


        assert highlights == self.example_report