from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from fog_rml.operators.sources.sql_fixture_fallback import (
    _apply_char_padding,
    _drop_sqlite_unsupported_line,
    _execute_fixture_query,
    _extract_query_table_name,
    _extract_schema_hints,
    _is_constraint_line,
    _load_fixture_artifacts,
    _normalize_binary_value,
    _normalize_boolean_value,
    _normalize_cell_value,
    _normalize_hinted_temporal_value,
    _normalize_native_typed_value,
    _normalize_select_sql,
    _normalize_sql_script,
    _normalize_string_temporal_value,
    _parse_column_hint,
    _require_mapping_dir,
    _resolve_fixture_sql,
    _rewrite_drop_table_cascade,
    _validate_strict_table_name,
    load_rows_from_sql_fixture,
    normalize_db_rows,
)


@pytest.mark.coverage_suite
def test_sql_fixture_fallback_loads_and_normalizes_rows(tmp_path: Path):
    fixture = tmp_path / "resource1.sql"
    fixture.write_text(
        "CREATE TABLE demo (flag BIT, created DATE, payload TEXT);\n"
        "INSERT INTO demo VALUES (1, '2024-01-02', 'hello');",
        encoding="utf-8",
    )
    rows = load_rows_from_sql_fixture(tmp_path, "SELECT * FROM demo", None)
    normalized = normalize_db_rows([{"flag": True, "payload": b"\x0f"}])
    assert rows[0]["created"].datatype_iri.endswith("#date")
    assert normalized[0]["payload"] == "0F"


@pytest.mark.coverage_suite
def test_sql_fixture_fallback_helper_functions_cover_normalization_paths(tmp_path: Path):
    fixture = tmp_path / "resource1.sql"
    fixture.write_text(
        "USE demo;\n"
        "DROP TABLE IF EXISTS demo CASCADE;\n"
        "CREATE TABLE demo (\n"
        "  id INTEGER,\n"
        "  code CHAR(3),\n"
        "  created DATE,\n"
        "  PRIMARY KEY (id)\n"
        ");\n"
        "INSERT INTO demo VALUES (1, 'A', '2024-01-02');\n",
        encoding="utf-8",
    )

    assert _drop_sqlite_unsupported_line("USE demo;") is True
    assert _rewrite_drop_table_cascade("  DROP TABLE IF EXISTS demo CASCADE;") == "  DROP TABLE IF EXISTS demo;"
    normalized_script = _normalize_sql_script(fixture.read_text(encoding="utf-8"))
    assert "USE demo" not in normalized_script
    assert _extract_query_table_name("SELECT * FROM demo") == "demo"
    assert _normalize_select_sql("SELECT concat_ws(code, '', created) FROM demo")
    assert _is_constraint_line("PRIMARY KEY (id)") is True
    assert _parse_column_hint("code CHAR(3)") == ("code", {"type": "char", "size": 3})
    assert "demo" in _extract_schema_hints(fixture.read_text(encoding="utf-8"))
    assert _normalize_boolean_value(1, "bit").lexical_form == "true"
    assert _apply_char_padding("A", "char", 3) == "A  "
    assert _normalize_hinted_temporal_value("2024-01-02", "date").datatype_iri.endswith("#date")
    assert _normalize_native_typed_value(Decimal("1.5")).datatype_iri.endswith("#decimal")
    assert _normalize_native_typed_value(datetime(2024, 1, 2, 3, 4, 5)).datatype_iri.endswith("#dateTime")
    assert _normalize_native_typed_value(date(2024, 1, 2)).datatype_iri.endswith("#date")
    assert _normalize_string_temporal_value("2024-01-02 03:04:05").datatype_iri.endswith("#dateTime")
    assert _normalize_binary_value(memoryview(b"\x0f")) == "0F"
    assert _normalize_binary_value("\\x0f") == "0F"
    assert _normalize_cell_value("A", {"type": "char", "size": 2}) == "A "
    assert _require_mapping_dir(tmp_path) == tmp_path
    script, normalized, hints = _load_fixture_artifacts(tmp_path)
    assert script
    assert normalized
    assert hints["demo"]["code"]["type"] == "char"
    assert _resolve_fixture_sql("SELECT * FROM demo", None, False, normalized_script) == "SELECT * FROM demo"
    assert _resolve_fixture_sql(None, "demo", False, normalized_script) == "SELECT * FROM demo"
    assert _execute_fixture_query(normalized_script, "SELECT * FROM demo", False)[0]["id"] == 1


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_sql_fixture_fallback_strict_mode_rejects_invalid_table_identifier(tmp_path: Path, monkeypatch):
    fixture = tmp_path / "resource1.sql"
    fixture.write_text("CREATE TABLE demo (id INTEGER);\nINSERT INTO demo VALUES (1);", encoding="utf-8")
    monkeypatch.setenv("FOG_RML_STRICT_REFERENCES", "1")
    with pytest.raises(ValueError):
        load_rows_from_sql_fixture(tmp_path, None, "Demo")
    monkeypatch.delenv("FOG_RML_STRICT_REFERENCES", raising=False)


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_sql_fixture_fallback_helper_edge_cases(tmp_path: Path):
    fixture = tmp_path / "resource1.sql"
    fixture.write_text("CREATE TABLE demo (id INTEGER);\nINSERT INTO demo VALUES (1);", encoding="utf-8")
    with pytest.raises(ValueError):
        _require_mapping_dir(None)
    with pytest.raises(ValueError):
        _resolve_fixture_sql(None, None, False, fixture.read_text(encoding="utf-8"))
    with pytest.raises(ValueError):
        _validate_strict_table_name("Demo", _normalize_sql_script(fixture.read_text(encoding="utf-8")))
    with pytest.raises(ValueError):
        _validate_strict_table_name("missing", _normalize_sql_script(fixture.read_text(encoding="utf-8")))
    with pytest.raises(ValueError):
        _load_fixture_artifacts(tmp_path / "missing")
    with pytest.raises(Exception):
        _execute_fixture_query("CREATE TABLE demo (id INTEGER);", "SELECT * FROM missing", True)
