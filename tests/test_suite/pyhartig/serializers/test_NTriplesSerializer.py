from __future__ import annotations

import pytest

import pyhartig.serializers.NTriplesSerializer as ntriples_module
from pyhartig.algebra.Terms import BlankNode, IRI, Literal
from pyhartig.algebra.Tuple import EPSILON, MappingTuple
from pyhartig.serializers.NTriplesSerializer import NTriplesSerializer


@pytest.mark.coverage_suite
def test_ntriples_serializer_serializes_triples():
    row = MappingTuple(
        {
            "subject": IRI("http://example.org/s"),
            "predicate": IRI("http://example.org/p"),
            "object": Literal("hello"),
        }
    )
    line, key, is_quad = NTriplesSerializer().serialize(row)
    assert line.endswith(" .")
    assert key == ("<http://example.org/s>", "<http://example.org/p>", '"hello"')
    assert is_quad is False
    assert NTriplesSerializer()._escape_string('a"b\n') == 'a\\"b\\n'


@pytest.mark.edge_case
def test_ntriples_serializer_returns_none_for_incomplete_rows():
    assert NTriplesSerializer().serialize(MappingTuple({"subject": IRI("http://example.org/s")})) is None


@pytest.mark.edge_case
def test_ntriples_serializer_rejects_invalid_term_positions():
    with pytest.raises(TypeError):
        NTriplesSerializer()._format_term(Literal("bad"), allowed_types=(IRI,))


@pytest.mark.coverage_suite
def test_ntriples_serializer_formats_blank_nodes_and_literal_variants(monkeypatch):
    serializer = NTriplesSerializer()
    assert serializer.serialize(MappingTuple({"subject": EPSILON, "predicate": IRI("http://example.org/p"), "object": Literal("x")})) is None
    assert serializer._format_term(BlankNode("b1"), allowed_types=(BlankNode,)) == "_:b1"
    assert serializer._format_term(Literal("bonjour", language="fr"), allowed_types=(Literal,)) == '"bonjour"@fr'
    assert serializer._format_term(Literal("1", datatype_iri="http://www.w3.org/2001/XMLSchema#integer"), allowed_types=(Literal,)) == '"1"^^<http://www.w3.org/2001/XMLSchema#integer>'

    monkeypatch.setattr(ntriples_module, "urlsplit", lambda value: (_ for _ in ()).throw(ValueError("boom")))
    assert serializer._format_term(IRI("http://example.org/a path"), allowed_types=(IRI,)) == "<http://example.org/a%20path>"


@pytest.mark.edge_case
def test_ntriples_serializer_rejects_unknown_runtime_term_types():
    with pytest.raises(ValueError):
        NTriplesSerializer()._format_term(object(), allowed_types=(object,))
