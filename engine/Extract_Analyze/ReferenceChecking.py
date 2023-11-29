from enum import Enum
import re
from .Summarizer import ReportExtractor
from ..OpenAICaller import openAICaller

class ReferenceType(Enum):
    """
    Enum for the type of reference. These can either be a citation or a quote.
    """
    citation = "citation"
    quote = "quote"

class Reference():
    """
    Reference object used as a helper in the ReferenceValidator class.
    """
    def __init__(self, text: str, reference: str, type: ReferenceType):
        self.text = text # This is the text that is being referenced
        self.reference = reference # This is the reference pointing to a partiuclar section, paragraph, or subparagraph.
        self.type = type
        

class ReferenceValidator():
    """
    Can be used to check if the references in a section of text are valid or not but comparing them to the original text.
    """
    def __init__(self, original_text: str, debug=False):
        self.original_text = original_text
        self.debug = debug

    def _print(self, message):
        """
        Prints the given message if debug is set to true.
        """
        if self.debug:
            print(message)

    def validate_references(self, text):
        """
        Checks if all of the references are valid or not. A single wrong references will return false for the whole text.
        """
        references = self._extract_references(text)
        for reference in references:
            if not self._validate_reference(reference):
                return False
        return True

    def _extract_references(self, text) -> [Reference]:
        """
        Extracts all the references from the given text and returns a list of them.
        """
        reference_regex = r'''("([^"]+)" {0,2}\((\d+\.\d+(\.\d{1,2})?)\))|(([\w\d\s,':;/()$%-])*\((\d+\.\d+(\.\d{1,2})?)\))'''

        references = []
        for match in re.finditer(reference_regex, text.lower()):
        
            if match.group(1) is not None:
                quote = match.group(2).replace("\n", "").replace("’","'").lower()
                references.append(Reference(quote, match.group(3), ReferenceType.quote))
            else:
                references.append(Reference(match.group(5), match.group(7), ReferenceType.citation))

        return references

    def _validate_reference(self, reference: Reference) -> bool:
        """
        Checks if the given reference is valid or not.
        """
        source_section = ReportExtractor(self.original_text, "Not known").extract_section(reference.reference)
        source_section = source_section.replace("\n", "").replace("’","'").lower()

        match reference.type:
            case ReferenceType.citation:
                return self._validate_citation(reference, source_section)
            case ReferenceType.quote:
                return self._validate_quote(reference, source_section)

    def _validate_citation(self, citation: Reference, source_section: str) -> bool:
        """
        Checks if the given citation is valid or not. Uses a llm to see if the quotation makes sense.
        """
        self._print(f"   Validating citation: {citation.reference} with reference {citation.text}")
        system_message = """
You are helping me check that references are correct.

Whitespace differences are too be ignored.

You will be given a citation and the source text. Return "yes" if you think that the citation is correct. Return "no" if you cant find any evidence for the citation in the source text.
"""

        user_message = f"""
Here is the reference:
{citation.reference}

Here is the source text:
{source_section}
""" 
        valid = openAICaller.query(
            system_message,
            user_message,
            large_model=True,
            temp = 0
        )

        if valid.lower() == "yes":
            self._print(f"    Validated citation")
            return True
        elif valid.lower() == "no":
            self._print(f"   Invalid citation could be justified to have come from\n   {source_section}")
            return False
        else:
            self._print(f"  Invalid response from model: {valid}, going to retry")
            return self._validate_citation(citation, source_section)


    def _validate_quote(self, quote: Reference, source_section: str) -> bool:
        """
        Checks if the given quote is valid or not. This is done by just using Regex. If the quote cant be found in the source section then it is invalid. There may be extra problems with additional spaces that can be added into the source section by the text extraction from a pdf.
        """
        self._print(f"   Validating quote: {quote.text} with reference {quote.reference}")
        quote_regex = re.compile(quote.text, re.MULTILINE | re.IGNORECASE)
        if not re.search(quote_regex, source_section) is None:
            self._print(f"   Validated quote")
            return True

        # Add a opional space between each character in the quote
        quote_regex = re.compile(r"[\n ]?".join(list(quote.text)), re.MULTILINE | re.IGNORECASE)
        if not re.search(quote_regex, source_section) is None:
            self._print(f"   Validated quote with extra spaces")
            return True
        
        self._print(f"   Invalid quote not found in\n   {source_section}")

        return False
