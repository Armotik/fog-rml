from __future__ import annotations

import pytest

from fog_rml.algebra.Tuple import MappingTuple
from fog_rml.operators.Operator import Operator
from fog_rml.operators.ProjectOperator import ProjectOperator


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
def test_project_operator_keeps_requested_attributes(stream_to_list):
    operator = ProjectOperator(_StaticOperator([MappingTuple({"id": 1, "name": "Alice"})]), {"id"})
    rows = stream_to_list(operator.execute())
    assert rows[0]["id"] == 1
    assert "name" not in rows[0]
    assert "Project(" in operator.explain()
    assert operator.explain_json()["parameters"]["attributes"] == ["id"]


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_project_operator_raises_for_missing_attributes():
    operator = ProjectOperator(_StaticOperator([MappingTuple({"id": 1})]), {"missing"})
    with pytest.raises(KeyError):
        list(operator.execute())


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_project_operator_returns_empty_stream_for_empty_parent(stream_to_list):
    operator = ProjectOperator(_StaticOperator([]), {"id"})
    assert stream_to_list(operator.execute()) == []
