from __future__ import annotations

import types

import pytest

from fog_rml.operators.sources.MysqlSourceOperator import MysqlSourceOperator


@pytest.mark.coverage_suite
def test_mysql_source_operator_normalizes_dsn_and_connection_kwargs(monkeypatch, tmp_path):
    fixture = tmp_path / "resource1.sql"
    fixture.write_text("CREATE TABLE demo (id INTEGER, name TEXT);\nINSERT INTO demo VALUES (1, 'Alice');", encoding="utf-8")

    assert MysqlSourceOperator._normalize_dsn("jdbc:mysql://host/db").startswith("mysql://")
    monkeypatch.setenv("FOG_RML_MYSQL_DSN", "mysql://user:pass@host:3306/db")
    assert MysqlSourceOperator._normalize_dsn("CONNECTIONDSN").startswith("mysql://")
    kwargs = MysqlSourceOperator._build_connection_kwargs("mysql://user:pass@host:3306/db", None, None)
    assert kwargs["database"] == "db"
    rows = MysqlSourceOperator._load_rows("", None, "demo", None, None, tmp_path)
    assert rows[0]["name"] == "Alice"

    original_load_rows = MysqlSourceOperator._load_rows
    monkeypatch.setattr(MysqlSourceOperator, "_load_rows", classmethod(lambda cls, **kwargs: [{"id": 1}]))
    operator = MysqlSourceOperator("mysql://user:pass@host:3306/db", "$", {"id": "id"})
    assert operator.explain_json()["parameters"]["source_type"] == "MYSQL"
    monkeypatch.setattr(MysqlSourceOperator, "_load_rows", original_load_rows)


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_mysql_source_operator_requires_complete_connection_info():
    with pytest.raises(ValueError):
        MysqlSourceOperator._build_connection_kwargs("mysql:///db", None, None)


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_mysql_source_operator_handles_import_and_connection_errors(monkeypatch, tmp_path):
    fixture = tmp_path / "resource1.sql"
    fixture.write_text("CREATE TABLE demo (id INTEGER);\nINSERT INTO demo VALUES (1);", encoding="utf-8")
    monkeypatch.setenv("FOG_RML_DB_SQLITE_FALLBACK", "0")
    with pytest.raises(ValueError):
        MysqlSourceOperator._load_rows("", None, "demo", None, None, tmp_path)

    monkeypatch.setenv("FOG_RML_DB_SQLITE_FALLBACK", "1")
    class _Cursor:
        def execute(self, sql):
            return None

        def fetchall(self):
            return [{"id": 1}]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    fake_cursor = _Cursor()
    fake_connection = types.SimpleNamespace(cursor=lambda: fake_cursor, close=lambda: None)
    fake_module = types.SimpleNamespace(cursors=types.SimpleNamespace(DictCursor=object()), connect=lambda **kwargs: fake_connection)
    monkeypatch.setattr("importlib.import_module", lambda name: fake_module)
    rows = MysqlSourceOperator._load_rows("mysql://user:pass@host:3306/db", "SELECT id FROM demo", None, None, None, tmp_path)
    assert rows[0]["id"].lexical_form == "1"
    monkeypatch.setattr("importlib.import_module", lambda name: (_ for _ in ()).throw(ModuleNotFoundError("missing")))
    with pytest.raises(ModuleNotFoundError):
        MysqlSourceOperator._load_rows("mysql://user:pass@host:3306/db", "SELECT id FROM demo", None, None, None, None)

