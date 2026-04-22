from __future__ import annotations

import pytest

from fog_rml.algebra.Terms import BlankNode, Literal
from fog_rml.algebra.Tuple import MappingTuple
from fog_rml.expressions.Constant import Constant
from fog_rml.expressions.FunctionCall import FunctionCall
from fog_rml.expressions.Reference import Reference
from fog_rml.operators.ExtendOperator import ExtendOperator
from fog_rml.operators.Operator import Operator


class _StaticOperator(Operator):
    def __init__(self, rows):
        self.rows = rows

    def execute(self):
        return iter(self.rows)

    def explain(self, indent: int = 0, prefix: str = "") -> str:
        return "static"

    def explain_json(self):
        return {"type": "static"}


@pytest.mark.coverage_suite
def test_extend_operator_adds_computed_attribute(stream_to_list):
    operator = ExtendOperator(_StaticOperator([MappingTuple({"id": 1})]), "label", Constant(Literal("ok")))
    rows = stream_to_list(operator.execute())
    assert rows[0]["label"].lexical_form == "ok"
    assert operator.explain_json()["parameters"]["new_attribute"] == "label"
    assert "Extend(" in operator.explain()


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_extend_operator_fallback_path_handles_non_mapping_rows(stream_to_list):
    operator = ExtendOperator(_StaticOperator([{"id": 1}]), "copy", Reference("id"))
    rows = stream_to_list(operator.execute())
    assert rows[0]["copy"] == 1


@pytest.mark.coverage_suite
def test_extend_operator_expression_helpers_cover_all_supported_shapes():
    operator = ExtendOperator(_StaticOperator([]), "value", Constant(Literal("x")))
    assert operator._explain_expression(Constant(Literal("x"))).startswith("Const(")
    assert operator._explain_expression(Reference("id")) == "Ref(id)"
    assert "<lambda>" in operator._explain_expression(FunctionCall(lambda value: value, [Constant(Literal("x"))]))
    assert operator._explain_expression(object()).startswith("<object object")


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_extend_operator_expression_json_handles_unknown_values():
    operator = ExtendOperator(_StaticOperator([]), "value", Constant(Literal("x")))
    assert operator._expression_to_json(object())["type"] == "Unknown"


@pytest.mark.coverage_suite
def test_extend_operator_expression_json_covers_all_supported_nodes():
    operator = ExtendOperator(_StaticOperator([]), "value", Constant(Literal("x")))
    assert operator._expression_to_json(Constant(BlankNode("b1")))["value_type"] == "BlankNode"
    assert operator._expression_to_json(Constant(5))["value"] == "5"
    assert operator._expression_to_json(Reference("id"))["attribute"] == "id"
    fn_json = operator._expression_to_json(FunctionCall("http://example.org/f", [Reference("id")]))
    assert fn_json["type"] == "FunctionCall"
    assert fn_json["arguments"][0]["type"] == "Reference"

