from typing import Optional

from pyhartig.algebra.Tuple import MappingTuple, EPSILON
from pyhartig.algebra.Terms import IRI, Literal, BlankNode
from pyhartig.namespaces import XSD_STRING


class NTriplesSerializer:
    """
    Serializer to convert MappingTuples into N-Triples format strings.
    Adheres to W3C N-Triples recommendation.
    """

    def serialize(self, row: MappingTuple) -> Optional[str]:
        """
        Converts a single MappingTuple into an N-Triples line.
        Expects keys 'subject', 'predicate', 'object' in the tuple.

        :param row: The tuple to serialize
        :return: A formatted string "<s> <p> <o> ." or None if tuple is incomplete/invalid.
        """
        # 1. Extract RDF Terms
        subject = row.get("subject")
        predicate = row.get("predicate")
        obj = row.get("object")

        # 2. Validation: Ensure all parts are present and valid RDF terms
        if not (subject and predicate and obj):
            return None

        if subject == EPSILON or predicate == EPSILON or obj == EPSILON:
            return None

        # 3. Format each term
        try:
            s_str = self._format_term(subject, allowed_types=(IRI, BlankNode))
            p_str = self._format_term(predicate, allowed_types=(IRI,))
            o_str = self._format_term(obj, allowed_types=(IRI, BlankNode, Literal))
        except TypeError:
            # Term type mismatch (e.g., Literal as Subject)
            return None

        # 4. Assemble N-Triples line
        return f"{s_str} {p_str} {o_str} ."

    def _format_term(self, term, allowed_types) -> str:
        """
        Formats an RDF term for N-Triples output with escaping.
        :param term: The RDF term to format
        :param allowed_types: Tuple of allowed RDF term types for this position
        :return: Formatted string representation of the term
        """
        if not isinstance(term, allowed_types):
            raise TypeError(f"Invalid term type {type(term)} for position")

        if isinstance(term, IRI):
            return f"<{term.value}>"

        if isinstance(term, BlankNode):
            return f"_:{term.identifier}"

        if isinstance(term, Literal):
            encoded_value = self._escape_string(term.lexical_form)

            # Case 1: Language tag
            if getattr(term, 'language', None):
                return f'"{encoded_value}"@{term.language}'

            # Case 2: Standard Datatype
            if term.datatype_iri == XSD_STRING.value:
                return f'"{encoded_value}"'

            # Case 3: Typed Literal
            return f'"{encoded_value}"^^<{term.datatype_iri}>'

        raise ValueError(f"Unknown term type: {type(term)}")

    def _escape_string(self, value: str) -> str:
        """
        Escapes special characters according to N-Triples spec.
        :param value: The string to escape
        :return: Escaped string
        """
        # Backslash must be escaped first
        value = value.replace("\\", "\\\\")
        value = value.replace('"', '\\"')
        value = value.replace("\n", "\\n")
        value = value.replace("\r", "\\r")
        value = value.replace("\t", "\\t")
        return value