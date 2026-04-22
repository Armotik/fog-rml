from __future__ import annotations

import json
from pathlib import Path

import pytest

from fog_rml.operators.sources.JsonSourceOperator import JsonSourceOperator


@pytest.mark.coverage_suite
def test_json_source_operator_reads_payloads_and_files(tmp_path: Path, stream_to_list):
    source = JsonSourceOperator({"items": [{"id": 1, "name": "Alice"}]}, "$.items[*]", {"name": "$.name"})
    rows = stream_to_list(source.execute())
    assert rows[0]["name"] == "Alice"

    path = tmp_path / "items.json"
    path.write_text(json.dumps({"items": [{"id": 2, "name": "Bob"}]}), encoding="utf-8")
    file_source = JsonSourceOperator.from_json_file(path, "$.items[*]", {"name": "$.name"})
    assert stream_to_list(file_source.execute())[0]["name"] == "Bob"
    assert file_source.explain_json()["parameters"]["source_type"] == "JSON"


@pytest.mark.coverage_suite
def test_json_source_operator_helper_paths_cover_compilation_and_flattening(tmp_path: Path):
    path = tmp_path / "items.json"
    path.write_text(json.dumps([{"name": "Alice"}, {"name": "Bob"}]), encoding="utf-8")
    assert JsonSourceOperator._resolve_json_path(path) == path.resolve()

    source = JsonSourceOperator([{"name": "Alice"}], "$", {"name": "name"})
    assert source._apply_iterator(source._data, "$") == [{"name": "Alice"}]
    assert source._find_attribute_matches({"name": "Alice"}, "name")
    assert source._get_compiled_attribute_expr("$.name") is not None
    assert JsonSourceOperator._flatten_matches([type("M", (), {"value": ["a", "b"]})(), type("M", (), {"value": "c"})()]) == ["a", "b", "c"]


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_json_source_operator_rejects_unsupported_payload_types():
    assert JsonSourceOperator._prepare_source_data(object()) is None


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_json_source_operator_handles_invalid_paths_and_queries(tmp_path: Path):
    with pytest.raises(ValueError):
        JsonSourceOperator._resolve_json_path(tmp_path)
    assert JsonSourceOperator._compile_attribute_expr("$['oops'") is None
    source = JsonSourceOperator({"items": [{"name": "Alice"}]}, "$.items[*]", {"name": "$.missing"})
    assert source._apply_extraction({"name": "Alice"}, "$.missing") == []


@pytest.mark.coverage_suite
def test_json_source_operator_covers_invalid_compilation_and_fallback_attribute_lookup(monkeypatch):
    source = JsonSourceOperator({"items": [{"name": "Alice"}]}, "$['oops'", {"name": "$['oops'"})
    assert source._compiled_iterator is None
    assert source._compiled_attribute_exprs["$['oops'"] is None
    assert source._apply_iterator(source._data, "$['oops'") == []

    fallback_source = JsonSourceOperator({"name": "Alice"}, "$", {})

    class _Expr:
        def find(self, context):
            return ["hit"]

    monkeypatch.setattr(fallback_source, "_get_compiled_attribute_expr", lambda query: None)
    monkeypatch.setattr(fallback_source, "_compile_attribute_expr", lambda query: _Expr())
    assert fallback_source._find_attribute_matches({"name": "Alice"}, "name") == ["hit"]


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_json_source_operator_rejects_invalid_json_shapes_and_non_string_keys():
    with pytest.raises(ValueError):
        JsonSourceOperator._sanitize_json_value({1: "bad"})
    assert JsonSourceOperator._prepare_source_data({1: "bad"}) is None
