from __future__ import annotations

import pytest

import pyhartig.serializers.NQuadsSerializer as nquads_module
from pyhartig.algebra.Terms import BlankNode, IRI, Literal
from pyhartig.algebra.Tuple import EPSILON, MappingTuple
from pyhartig.namespaces import RR_DEFAULT_GRAPH
from pyhartig.serializers.NQuadsSerializer import NQuadsSerializer


@pytest.mark.coverage_suite
def test_nquads_serializer_serializes_named_graph_rows():
    row = MappingTuple(
        {
            "subject": IRI("http://example.org/s"),
            "predicate": IRI("http://example.org/p"),
            "object": Literal("hello"),
            "graph": IRI("http://example.org/g"),
        }
    )
    line, _key, is_quad = NQuadsSerializer().serialize(row)
    assert "<http://example.org/g> ." in line
    assert is_quad is True


@pytest.mark.edge_case
def test_nquads_serializer_falls_back_to_triples_for_invalid_graph():
    row = MappingTuple(
        {
            "subject": IRI("http://example.org/s"),
            "predicate": IRI("http://example.org/p"),
            "object": Literal("hello"),
            "graph": Literal("not-an-iri"),
        }
    )
    line, _key, is_quad = NQuadsSerializer().serialize(row)
    assert line.endswith(" .")
    assert is_quad is False


@pytest.mark.coverage_suite
def test_nquads_serializer_escape_and_format_helpers():
    serializer = NQuadsSerializer()
    assert serializer._escape_string('a"b\n') == 'a\\"b\\n'
    with pytest.raises(TypeError):
        serializer._format_term(Literal("bad"), allowed_types=(IRI,))


@pytest.mark.coverage_suite
def test_nquads_serializer_formats_default_graph_blank_nodes_and_literals(monkeypatch):
    serializer = NQuadsSerializer()
    assert serializer.serialize(MappingTuple({"subject": EPSILON, "predicate": IRI("http://example.org/p"), "object": Literal("x")})) is None
    assert serializer._format_term(BlankNode("b1"), allowed_types=(BlankNode,)) == "_:b1"
    assert serializer._format_term(Literal("bonjour", language="fr"), allowed_types=(Literal,)) == '"bonjour"@fr'
    assert serializer._format_term(Literal("1", datatype_iri="http://www.w3.org/2001/XMLSchema#integer"), allowed_types=(Literal,)) == '"1"^^<http://www.w3.org/2001/XMLSchema#integer>'

    row = MappingTuple(
        {
            "subject": IRI("http://example.org/s"),
            "predicate": IRI("http://example.org/p"),
            "object": Literal("hello"),
            "graph": IRI(RR_DEFAULT_GRAPH.value),
        }
    )
    assert NQuadsSerializer().serialize(row)[2] is False

    monkeypatch.setattr(nquads_module, "urlsplit", lambda value: (_ for _ in ()).throw(ValueError("boom")))
    assert serializer._format_term(IRI("http://example.org/a path"), allowed_types=(IRI,)) == "<http://example.org/a%20path>"


@pytest.mark.edge_case
def test_nquads_serializer_rejects_unknown_runtime_term_types():
    with pytest.raises(ValueError):
        NQuadsSerializer()._format_term(object(), allowed_types=(object,))
