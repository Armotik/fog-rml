from __future__ import annotations

import pytest

from pyhartig.algebra.Tuple import EPSILON
from pyhartig.operators.SourceOperator import SourceOperator


class _DictSource(SourceOperator):
    def _apply_iterator(self, data, query):
        return data.get("items", [])

    def _apply_extraction(self, context, query):
        return context.get(query, [])


@pytest.mark.coverage_suite
def test_source_operator_executes_cartesian_products(stream_to_list):
    source = _DictSource({"items": [{"id": [1], "name": ["Alice"]}]}, "$", {"id": "id", "name": "name"})
    rows = stream_to_list(source.execute())
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "Alice"
    assert _DictSource._normalize_row_dict({"missing": None})["missing"] == EPSILON


@pytest.mark.edge_case
def test_source_operator_strict_reference_mode_raises(monkeypatch):
    source = _DictSource({"items": [{"id": [1]}]}, "$", {"id": "id", "missing": "missing"})
    monkeypatch.setenv("PYHARTIG_STRICT_REFERENCES", "1")
    with pytest.raises(ValueError):
        list(source.execute())
    monkeypatch.delenv("PYHARTIG_STRICT_REFERENCES", raising=False)
