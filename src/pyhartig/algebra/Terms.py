from dataclasses import dataclass
from typing import Union
import re


class InvalidIRIError(ValueError):
    """Exception raised when an invalid IRI is provided."""
    pass


# IRI validation regex based on RFC 3987
# This pattern validates the general structure of an IRI
_IRI_PATTERN = re.compile(
    r'^'
    r'[a-zA-Z][a-zA-Z0-9+.-]*:'  # scheme
    r'(//'  # authority (optional)
    r'([^/?#]*)'  # userinfo + host + port
    r')?'
    r'([^?#]*)'  # path
    r'(\?[^#]*)?'  # query (optional)
    r'(#.*)?'  # fragment (optional)
    r'$',
    re.UNICODE
)


def _validate_iri(value: str) -> None:
    """
    Validate that the given string is a valid IRI according to RFC 3987.

    :param value: The string to validate as an IRI
    :raises InvalidIRIError: If the string is not a valid IRI
    """
    if not value:
        raise InvalidIRIError("IRI cannot be empty")

    if not _IRI_PATTERN.match(value):
        raise InvalidIRIError(f"Invalid IRI syntax: '{value}'")


@dataclass(frozen=True)
class IRI:
    """
    Represents an Internationalized Resource Identifier (IRI).
    Examples: <http://example.org/resource>, <urn:isbn:0451450523>

    Raises:
        InvalidIRIError: If the provided value is not a valid IRI according to RFC 3987.
    """
    value: str

    def __post_init__(self) -> None:
        """
        Validate the IRI after initialization.
        :return: None
        """
        _validate_iri(self.value)

    def __repr__(self) -> str:
        """
        String representation of the IRI
        :return: String representation of the IRI
        """
        return f"<{self.value}>"

class InvalidLanguageTagError(ValueError):
    """
    Exception raised when an invalid language tag is provided.
    """
    pass


# Language tag validation regex based on BCP 47 (simplified)
# Format: primary language subtag (2-3 letters) optionally followed by subtags
_LANGUAGE_TAG_PATTERN = re.compile(
    r'^[a-zA-Z]{2,3}(-[a-zA-Z0-9]{1,8})*$'
)


def _validate_language_tag(tag: str) -> None:
    """
    Validate that the given string is a valid language tag according to BCP 47.

    :param tag: The string to validate as a language tag
    :raises InvalidLanguageTagError: If the string is not a valid language tag
    """
    if not tag:
        raise InvalidLanguageTagError("Language tag cannot be empty")

    if not _LANGUAGE_TAG_PATTERN.match(tag):
        raise InvalidLanguageTagError(f"Invalid language tag syntax: '{tag}'")


@dataclass(frozen=True)
class Literal:
    """
    Represents an RDF Literal with optional language tag support.

    Examples:
        - Simple string: "Hello World"
        - Typed literal: "42"^^http://www.w3.org/2001/XMLSchema#integer
        - Language-tagged literal: "Bonjour"@fr

    Note:
        According to RDF 1.1 specification, a literal cannot have both a language tag
        and a datatype other than rdf:langString. When a language tag is provided,
        the datatype is automatically set to rdf:langString.

    Raises:
        InvalidLanguageTagError: If an invalid language tag is provided.
        ValueError: If both a non-default datatype and a language tag are provided.
    """
    lexical_form: str
    datatype_iri: str = "http://www.w3.org/2001/XMLSchema#string"
    language: str = None

    def __post_init__(self) -> None:
        """
        Validate language tag and enforce RDF 1.1 rules regarding language-tagged literals.
        :return: None
        """
        if self.language is not None:
            # Validate language tag
            _validate_language_tag(self.language)

            # Per RDF 1.1: language-tagged literals have datatype rdf:langString
            # If user specified a different datatype, it's an error
            if self.datatype_iri not in (
                "http://www.w3.org/2001/XMLSchema#string",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
            ):
                raise ValueError(
                    f"Cannot specify both language tag '{self.language}' and "
                    f"datatype '{self.datatype_iri}'. Language-tagged literals "
                    f"must have datatype rdf:langString."
                )

            # Automatically set datatype to rdf:langString for language-tagged literals
            # Since dataclass is frozen, we need to use object.__setattr__
            object.__setattr__(
                self,
                'datatype_iri',
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
            )

    def __repr__(self) -> str:
        """
        String representation of the Literal in N-Triples format.

        :return: String representation of the Literal
        """
        # Language-tagged literal: "value"@lang
        if self.language is not None:
            return f'"{self.lexical_form}"@{self.language}'

        # Simple string literal (default datatype): "value"
        if self.datatype_iri == "http://www.w3.org/2001/XMLSchema#string":
            return f'"{self.lexical_form}"'

        # Typed literal: "value"^^datatype
        return f'"{self.lexical_form}"^^{self.datatype_iri}'

@dataclass(frozen=True)
class BlankNode:
    """
    Represents an RDF Blank Node
    Examples: _:b0, _:node1
    """
    identifier: str

    def __repr__(self) -> str:
        """
        String representation of the Blank Node
        :return: String representation of the Blank Node
        """
        return f"_:{self.identifier}"

RdfTerm = Union[IRI, Literal, BlankNode]