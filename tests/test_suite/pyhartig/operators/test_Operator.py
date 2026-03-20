from __future__ import annotations

import pytest

from pyhartig.algebra.Tuple import MappingTuple
from pyhartig.expressions.Constant import Constant
from pyhartig.operators.Operator import Operator, StreamRows


class _StaticOperator(Operator):
    def __init__(self, rows):
        self.rows = rows

    def execute(self):
        return self.rows

    def explain(self, indent: int = 0, prefix: str = "") -> str:
        return "static"

    def explain_json(self):
        return {"type": "static"}


@pytest.mark.coverage_suite
def test_stream_rows_and_operator_extend_helper():
    rows = StreamRows(iter([MappingTuple({"id": 1}), MappingTuple({"id": 2})]))
    extended = _StaticOperator(rows).extend("kind", Constant("demo"))

    assert len(rows) == 2
    assert rows[1]["id"] == 2
    assert rows[:1][0]["id"] == 1
    assert extended.new_attribute == "kind"


@pytest.mark.edge_case
def test_stream_rows_supports_negative_index():
    rows = StreamRows(iter([MappingTuple({"id": 1}), MappingTuple({"id": 2})]))
    assert rows[-1]["id"] == 2
