from __future__ import annotations

import pytest

from pyhartig.algebra.Terms import Literal
from pyhartig.algebra.Tuple import MappingTuple
from pyhartig.expressions.Constant import Constant


@pytest.mark.coverage_suite
def test_constant_returns_same_value():
    constant = Constant(Literal("hello"))
    assert constant.evaluate(MappingTuple({"id": 1})).lexical_form == "hello"


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_constant_repr_is_stable():
    assert "Const(" in repr(Constant(Literal("hello")))
