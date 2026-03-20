from __future__ import annotations

import logging

import pytest

from pyhartig.algebra.Tuple import EPSILON, MappingTuple
from pyhartig.operators.EquiJoinOperator import EquiJoinOperator
from pyhartig.operators.Operator import Operator


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
def test_equijoin_operator_matches_on_keys(stream_to_list):
    left = _StaticOperator([MappingTuple({"id": 1, "name": "Alice"})])
    right = _StaticOperator([MappingTuple({"ref": 1, "dept": "R&D"})])
    joined = EquiJoinOperator(left, right, left_join_attributes=["id"], right_join_attributes=["ref"])
    rows = stream_to_list(joined.execute())
    assert rows[0]["dept"] == "R&D"
    assert "EqJoin" not in joined.explain_json()["type"]


@pytest.mark.edge_case
def test_equijoin_operator_handles_legacy_args_and_missing_keys(stream_to_list):
    joined = EquiJoinOperator(
        _StaticOperator([MappingTuple({"id": EPSILON})]),
        _StaticOperator([MappingTuple({"ref": EPSILON})]),
        A=["id"],
        B=["ref"],
    )
    rows = stream_to_list(joined.execute())
    assert rows[0]["id"] == EPSILON
    assert rows[0]["ref"] == EPSILON


@pytest.mark.coverage_suite
def test_equijoin_operator_helper_methods_cover_normalization_and_key_lookup():
    joined = EquiJoinOperator(_StaticOperator([]), _StaticOperator([]), left_join_attributes=["id"], right_join_attributes=["ref"])
    assert joined._normalize_join_value(True) == "true"
    assert joined._normalize_join_value(5) == "5"
    assert joined._normalize_join_value(type("V", (), {"lexical_form": "x"})()) == "x"
    assert joined._get_join_attribute_value(MappingTuple({"parent_id": 1}), "id") == 1
    assert joined._get_join_attribute_value(MappingTuple({"ID": 1}), "id") == 1
    assert joined._build_key(MappingTuple({"id": 1}), ["id"]) == ("1",)
    assert joined._satisfies_join_condition(MappingTuple({"id": 1}), MappingTuple({"ref": 1})) is True
    assert "EquiJoin(" in joined.explain()
    assert joined.explain_json()["parameters"]["join_conditions"] == [{"left": "id", "right": "ref"}]


@pytest.mark.edge_case
def test_equijoin_operator_rejects_bad_constructor_arguments():
    with pytest.raises(ValueError):
        EquiJoinOperator(_StaticOperator([]), _StaticOperator([]), left_join_attributes=["a"], right_join_attributes=["a", "b"])
    with pytest.raises(TypeError):
        EquiJoinOperator(_StaticOperator([]), _StaticOperator([]), left_join_attributes=["a"], right_join_attributes=["b"], C=["x"])


@pytest.mark.coverage_suite
def test_equijoin_operator_logs_overlap_and_skips_missing_join_keys(stream_to_list, caplog):
    left = _StaticOperator([MappingTuple({"id": 1, "shared": "x"}), MappingTuple({"name": "no-key"})])
    right = _StaticOperator([MappingTuple({"ref": 1, "shared": "x"})])
    left.attribute_mappings = {"shared": "shared"}
    right.attribute_mappings = {"shared": "shared"}
    join = EquiJoinOperator(left, right, left_join_attributes=["id"], right_join_attributes=["ref"])

    with caplog.at_level(logging.WARNING):
        rows = stream_to_list(join.execute())

    assert rows[0]["shared"] == "x"
    assert any("overlap" in record.message for record in caplog.records)


@pytest.mark.edge_case
def test_equijoin_operator_handles_internal_overlap_guard_exceptions():
    class _BrokenMappingsOperator(_StaticOperator):
        @property
        def attribute_mappings(self):
            raise RuntimeError("boom")

    join = EquiJoinOperator(
        _BrokenMappingsOperator([MappingTuple({"id": 1})]),
        _BrokenMappingsOperator([MappingTuple({"ref": 1})]),
        left_join_attributes=["id"],
        right_join_attributes=["ref"],
    )
    assert list(join._join_materialized_rows([], [])) == []
    join._warn_for_declared_attribute_overlap()
