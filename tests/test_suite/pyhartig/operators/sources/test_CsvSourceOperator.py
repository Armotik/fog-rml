from __future__ import annotations

from pathlib import Path

import pytest

from pyhartig.operators.sources.CsvSourceOperator import CsvSourceOperator


@pytest.mark.coverage_suite
def test_csv_source_operator_reads_rows_and_dotted_values(tmp_path: Path, stream_to_list):
    path = tmp_path / "items.csv"
    path.write_text("ID,user.name\n1,Alice\n", encoding="utf-8")
    source = CsvSourceOperator(path, "$", {"id": "$.ID", "name": "$['user.name']"})
    rows = stream_to_list(source.execute())
    assert rows[0]["id"] == "1"
    assert rows[0]["name"] == "Alice"


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_csv_source_operator_returns_empty_list_on_invalid_query():
    source = CsvSourceOperator([{"ID": "1"}], "$", {"missing": "x"})
    assert source._apply_extraction({"ID": "1"}, "$['oops'") == []


@pytest.mark.coverage_suite
def test_csv_source_operator_helper_branches_cover_normalization_and_case_insensitive_access():
    source = CsvSourceOperator([{"ID": "1", "nested": {"Name": "Alice"}}], "$", {"name": "Name"})
    assert source._apply_extraction({"id": "1"}, "ID") == ["1"]
    assert source._normalize_extraction_query("$.ID") == "ID"
    assert source._normalize_extraction_query("$['user.name']") == "user.name"
    assert source._normalize_extraction_query("$[name]") == "name"
    assert source._resolve_dotted_query({"nested": {"Name": "Alice"}}, "nested.name") == ["Alice"]
    assert source._resolve_dotted_query({"nested": "Alice"}, "nested.name") == []


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_csv_source_operator_handles_invalid_context_and_source_loading_errors():
    class _BrokenRows:
        def __iter__(self):
            raise RuntimeError("boom")

    source = CsvSourceOperator(_BrokenRows(), "$", {"name": "name"})
    assert source._rows == []
    assert source._apply_extraction(None, "name") == []
    assert source._apply_extraction({"name": "Alice"}, "") == []
