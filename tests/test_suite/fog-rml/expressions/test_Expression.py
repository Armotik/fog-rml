from __future__ import annotations

import pytest

from fog_rml.algebra.Tuple import EPSILON, MappingTuple
from fog_rml.expressions.Expression import Expression


class _Identity(Expression):
    def evaluate(self, mapping: MappingTuple):
        return mapping.get("value", EPSILON)


@pytest.mark.coverage_suite
def test_expression_subclass_evaluates_mapping():
    assert _Identity().evaluate(MappingTuple({"value": 5})) == 5


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_expression_subclass_returns_epsilon_for_missing_key():
    assert _Identity().evaluate(MappingTuple()) == EPSILON

