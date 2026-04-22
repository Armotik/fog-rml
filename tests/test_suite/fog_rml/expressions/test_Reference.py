from __future__ import annotations

import pytest

from fog_rml.algebra.Tuple import EPSILON, MappingTuple
from fog_rml.expressions.Reference import Reference


@pytest.mark.coverage_suite
def test_reference_reads_existing_attribute():
    assert Reference("name").evaluate(MappingTuple({"name": "Alice"})) == "Alice"


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_reference_returns_epsilon_for_missing_attribute():
    assert Reference("missing").evaluate(MappingTuple()) == EPSILON
