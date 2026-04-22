from __future__ import annotations

import pytest

from fog_rml.algebra.Terms import Literal
from fog_rml.algebra.Tuple import EPSILON, MappingTuple
from fog_rml.expressions.Constant import Constant
from fog_rml.expressions.FunctionCall import FunctionCall
from fog_rml.expressions.Reference import Reference
from fog_rml.functions.registry import FunctionRegistry


@pytest.mark.coverage_suite
def test_function_call_resolves_registry_function():
    FunctionRegistry.register("http://example.org/f#join", lambda a, b: Literal(f"{a.lexical_form}-{b.lexical_form}"))
    expr = FunctionCall("http://example.org/f#join", [Constant(Literal("a")), Constant(Literal("b"))])
    assert expr.evaluate(MappingTuple()).lexical_form == "a-b"
    FunctionRegistry.unregister("http://example.org/f#join")


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_function_call_returns_epsilon_on_missing_argument():
    expr = FunctionCall(lambda value: value, [Reference("missing")])
    assert expr.evaluate(MappingTuple()) == EPSILON


@pytest.mark.coverage_suite
def test_function_call_handles_registry_miss_and_runtime_failures():
    assert FunctionCall("http://example.org/f#missing", [Constant(Literal("x"))]).evaluate(MappingTuple()) == EPSILON
    assert FunctionCall(lambda value: None, [Constant(Literal("x"))]).evaluate(MappingTuple()) == EPSILON
    assert FunctionCall(lambda value: (_ for _ in ()).throw(RuntimeError("boom")), [Constant(Literal("x"))]).evaluate(MappingTuple()) == EPSILON
