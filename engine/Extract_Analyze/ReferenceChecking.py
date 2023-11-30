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
        self.validated = False
        self.updated = False
        self.unrepairable = False
    
    def set_validated(self):
        """
        Sets the validated value of the reference.
        """
        self.validated = True

    def set_repaired(self, new_reference):
        """
        Sets the repaired value of the reference.
        """
        self.updated = True
        self.reference = new_reference.reference
        self.text = new_reference.text
        self.type = new_reference.type

    def set_unrepairable(self):
        """
        Sets the unrepairable value of the reference.
        """
        self.unrepairable = True

    def is_validated(self):
        """
        Returns the validated value of the reference.
        """
        if self.validated:
            return True
        
        if self.updated:
            return Reference(self.text, self.reference, self.type)
        elif self.unrepairable:
            return False
        

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

    def validate_references(self, text) -> [Reference]:
        """
        Checks if all of the references are valid or not. A single wrong references will return false for the whole text.
        """
        references = self._extract_references(text)
        processed_references = []
        for reference in references:
            processed_references.append(self._validate_reference(reference))
        return processed_references

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

    def _validate_reference(self, reference: Reference, attempt_repair: bool = False) -> bool:
        """
        Checks if the given reference is valid or not.
        """
        source_section = ReportExtractor(self.original_text, "Not known").extract_section(reference.reference)
        source_section = source_section.replace("\n", "").replace("’","'").lower()

        match reference.type:
            case ReferenceType.citation:
                return self._validate_citation(reference, source_section)
            case ReferenceType.quote:
                return self._validate_quote(reference, source_section, attempt_repair)

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
            citation.set_validated()
            return citation
        elif valid.lower() == "no":
            print(f"   Invalid citation couldn't be justified to have come from\n   {source_section}")
            citation.set_unrepairable()
            return citation

        else:
            self.print(f"  Invalid response from model: {valid}, going to retry")
            return self._validate_citation(citation, source_section)


    def _validate_quote(self, quote: Reference, source_section: str, straight_response: bool = False) -> bool:
        """
        Checks if the given quote is valid or not. This is done by just using Regex. If the quote cant be found in the source section then it is invalid. There may be extra problems with additional spaces that can be added into the source section by the text extraction from a pdf.
        """
        self._print(f"   Validating quote: {quote.text} with reference {quote.reference}")
        quote_regex = re.compile(quote.text, re.MULTILINE | re.IGNORECASE)
        if not re.search(quote_regex, source_section) is None:
            self._print(f"   Validated quote")
            quote.set_validated()
            return quote

        # Add a opional space between each character in the quote
        quote_regex = re.compile(r"[\n ]?".join(list(quote.text)), re.MULTILINE | re.IGNORECASE)
        if not re.search(quote_regex, source_section) is None:
            self._print(f"   Validated quote with extra spaces")
            quote.set_validated()
            return quote
        
        if not straight_response:
            self._print(f"   Invalid quote not found in\n   {source_section}")

        # There can be a tendency to get the attributue section wrong. Therefore we will check if the quote is in one of the sections either just before or just after.

        if straight_response:
            return False


        quote_repairer = QuoteRepairer(self, self.debug)

        fixed_quote = quote_repairer._find_quote_location(quote)

        if fixed_quote:
            self._print(f"   Fixed quote to be {fixed_quote.reference}")
            quote.set_repaired(fixed_quote)
            return quote
        
        quote.set_unrepairable()
        return quote
    

class QuoteRepairer():
    """
    Used to help fix a reference that is broken. Either by finding the correct location, or by finding the correct quote.
    """
    def __init__(self, ReferenceValidator: ReferenceValidator,  debug=False):
        self.reference_validator = ReferenceValidator
        self.debug = debug

    def _print(self, message):
        """
        Prints the given message if debug is set to true.
        """
        if self.debug:
            print(message)


    def _find_quote_location(self, quote: Reference):
        """
        Search neibouring sections and find where the quote was from
        """
        potential_locations = self._get_quote_potential_locations(quote)

        self._print(f"  Potential locations: {potential_locations}")

        for location in potential_locations:
            temp_reference = Reference(quote.text, location, ReferenceType.quote)
            if self.reference_validator._validate_reference(temp_reference,True):
                return temp_reference

        self._print("  Couldn't find quote location")

        return False
    
    def _get_quote_potential_locations(self, reference: Reference):
        """
        Get the two neighbourough section to a reference.
        """
        parts = reference.reference.split('.')
        if len(parts) == 1:
            # Handling sections like "5"
            section, paragraph, subparagraph = int(parts[0]), 0, 0
            preceding_sections = map(lambda args: self._parse_section(*args) ,[(section-i, paragraph, subparagraph) for i in range(1, 3)])
            succeeding_sections = map(lambda args: self._parse_section(*args),[(section + i, paragraph, subparagraph) for i in range(1, 3)])
            jump_to_next = (section + 1 ,0, 0)
        elif len(parts) == 2:
            # Handling sections like "5.2"
            section, paragraph, subparagraph = list(map(int, parts)) + [0]
            preceding_sections = map(lambda args: self._parse_section(*args) ,[(section, paragraph  - i, subparagraph) for i in range(1, 3)])

            succeeding_sections = map(lambda args: self._parse_section(*args),[(section, paragraph + i, subparagraph) for i in range(1, 3)])
            jump_to_next = (section + 1, 1, 0)
        elif len(parts) == 3:
            # Handling sections like "5.2.1"
            section, paragraph, subparagraph = list(map(int, parts))
            preceding_sections = map(lambda args: self._parse_section(*args) ,[(section, paragraph, subparagraph - i) for i in range(1, 3)])

            succeeding_sections = map(lambda args: self._parse_section(*args),[(section, paragraph, subparagraph + i) for i in range(1, 3)])
            jump_to_next = (section, paragraph+1, 1)
        else:
            # Invalid input
            return []

        possible_locations = list(preceding_sections) + [reference.reference] + list(succeeding_sections) + [self._parse_section(*jump_to_next)]

        return list(dict.fromkeys(possible_locations))

    def _parse_section(self, section, paragraph, subparagraph):
        if subparagraph < 0:
            subparagraph = 0
            paragraph -= 1
        if paragraph < 0:
            paragraph = 0
            section -= 1
        if section < 0:
            return None

        formatted_reference = "{}.{}.{}".format(section, paragraph, subparagraph)

        self._print(f"  Formatted reference from {section}.{paragraph}.{subparagraph} to {formatted_reference}")
    
        return re.sub(r'.0(?!.)', "", formatted_reference)
