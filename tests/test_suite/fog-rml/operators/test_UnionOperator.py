from __future__ import annotations

import pytest

from fog_rml.algebra.Tuple import MappingTuple
from fog_rml.operators.Operator import Operator
from fog_rml.operators.UnionOperator import UnionOperator


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
def test_union_operator_merges_rows_with_distinct_mode(stream_to_list):
    row = MappingTuple({"id": 1})
    union = UnionOperator([_StaticOperator([row]), _StaticOperator([row])], distinct=True)
    assert stream_to_list(union.execute()) == [row]
    assert "Union(" in union.explain()
    assert union.explain_json()["parameters"]["distinct"] is True


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_union_operator_keeps_duplicates_in_bag_mode(stream_to_list):
    row = MappingTuple({"id": 1})
    union = UnionOperator([_StaticOperator([row]), _StaticOperator([row])], distinct=False)
    assert len(stream_to_list(union.execute())) == 2


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_union_operator_distinct_key_falls_back_to_string(monkeypatch):
    class _Row:
        def items(self):
            raise RuntimeError("boom")

        def __str__(self):
            return "row"

    assert UnionOperator._build_distinct_key(_Row()) == "row"

