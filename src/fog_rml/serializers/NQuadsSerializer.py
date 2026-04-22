from typing import Optional, TypeAlias

from fog_rml.algebra.Tuple import MappingTuple, EPSILON
from fog_rml.algebra.Terms import IRI, Literal, BlankNode
from fog_rml.namespaces import RR_DEFAULT_GRAPH, XSD_STRING
from urllib.parse import urlsplit, urlunsplit, quote

SerializedNQuad: TypeAlias = tuple[str, tuple[str, str, str], bool]


class NQuadsSerializer:
    """
    Serializer to convert MappingTuples into N-Quads format strings.
    Emits a quad if `graph` is present in the tuple, otherwise emits a triple line.
    """

    def serialize(self, row: MappingTuple) -> Optional[SerializedNQuad]:
        subject = row.get("subject")
        predicate = row.get("predicate")
        obj = row.get("object")
        graph = row.get("graph")

        if not (subject and predicate and obj):
            return None

        if subject == EPSILON or predicate == EPSILON or obj == EPSILON:
            return None

        try:
            s_str = self._format_term(subject, allowed_types=(IRI, BlankNode))
            p_str = self._format_term(predicate, allowed_types=(IRI,))
            o_str = self._format_term(obj, allowed_types=(IRI, BlankNode, Literal))
        except TypeError:
            return None

        if isinstance(graph, IRI) and graph.value == RR_DEFAULT_GRAPH.value:
            graph = None

        if graph:
            try:
                g_str = self._format_term(graph, allowed_types=(IRI,))
            except TypeError:
                # ignore invalid graph term
                g_str = None
        else:
            g_str = None

        if g_str:
            line = f"{s_str} {p_str} {o_str} {g_str} ."
            key = (s_str, p_str, o_str)
            return line, key, True
        line = f"{s_str} {p_str} {o_str} ."
        key = (s_str, p_str, o_str)
        return line, key, False

    def format_term(self, term, allowed_types) -> str:
        """
        Public wrapper around the RDF term formatter.
        :param term: The RDF term to format.
        :param allowed_types: Tuple of allowed RDF term types.
        :return: Formatted RDF term string.
        """
        return self._format_term(term, allowed_types)

    def _format_term(self, term, allowed_types) -> str:
        if not isinstance(term, allowed_types):
            raise TypeError(f"Invalid term type {type(term)} for position")

        if isinstance(term, IRI):
            try:
                parts = urlsplit(term.value)
                # Allow existing percent-encodings to pass through (avoid double-encoding)
                path = quote(parts.path, safe="/:@;,%")
                query = quote(parts.query, safe="=&?/:,;@%")
                fragment = quote(parts.fragment, safe=";@,/?%")
                safe_iri = urlunsplit((parts.scheme, parts.netloc, path, query, fragment))
            except Exception:
                safe_iri = term.value.replace(' ', '%20')
            return f"<{safe_iri}>"

        if isinstance(term, BlankNode):
            return f"_:{term.identifier}"

        if isinstance(term, Literal):
            encoded_value = self._escape_string(term.lexical_form)
            if getattr(term, 'language', None):
                return f'"{encoded_value}"@{term.language}'
            if term.datatype_iri == XSD_STRING.value:
                return f'"{encoded_value}"'
            return f'"{encoded_value}"^^<{term.datatype_iri}>'

        raise ValueError(f"Unknown term type: {type(term)}")

    def _escape_string(self, value: str) -> str:
        value = value.replace("\\", "\\\\")
        value = value.replace('"', '\\"')
        value = value.replace("\n", "\\n")
        value = value.replace("\r", "\\r")
        value = value.replace("\t", "\\t")
        return value

