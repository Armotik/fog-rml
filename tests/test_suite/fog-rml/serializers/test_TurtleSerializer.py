from __future__ import annotations

import pytest

from fog_rml.algebra.Terms import IRI, Literal
from fog_rml.algebra.Tuple import MappingTuple
from fog_rml.serializers.TurtleSerializer import TurtleSerializer


@pytest.mark.coverage_suite
def test_turtle_serializer_serializes_triples():
    row = MappingTuple(
        {
            "subject": IRI("http://example.org/s"),
            "predicate": IRI("http://example.org/p"),
            "object": Literal("hello"),
        }
    )

    line, key, is_quad = TurtleSerializer().serialize(row)
    assert line.endswith(" .")
    assert key == ("<http://example.org/s>", "<http://example.org/p>", '"hello"')
    assert is_quad is False
