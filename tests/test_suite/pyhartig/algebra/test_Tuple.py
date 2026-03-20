from __future__ import annotations

import pytest

from pyhartig.algebra.Tuple import EPSILON, MappingTuple


@pytest.mark.coverage_suite
def test_mapping_tuple_supports_merge_extend_and_project():
    left = MappingTuple({"id": 1, "name": "Alice"})
    right = MappingTuple({"dept": "R&D"})

    merged = left.merge(right).extend("status", EPSILON).project({"id", "status"})

    assert merged["id"] == 1
    assert merged["status"] == EPSILON
    assert "name" not in merged
    assert "id" in merged
    assert hash(merged) == hash(merged)
    assert merged == {"id": 1, "status": EPSILON}
    assert "Tuple(" in repr(merged)


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_mapping_tuple_rejects_none_values_and_conflicts():
    with pytest.raises(ValueError):
        MappingTuple({"id": None})
    with pytest.raises(ValueError):
        MappingTuple({"id": 1}).merge(MappingTuple({"id": 2}))
