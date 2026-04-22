from __future__ import annotations

import types

import pytest

from fog_rml.operators.sources.SqlserverSourceOperator import SqlserverSourceOperator


@pytest.mark.coverage_suite
def test_sqlserver_source_operator_builds_connection_strings(monkeypatch, tmp_path):
    fixture = tmp_path / "resource1.sql"
    fixture.write_text("CREATE TABLE demo (id INTEGER, name TEXT);\nINSERT INTO demo VALUES (1, 'Alice');", encoding="utf-8")
    monkeypatch.setenv("FOG_RML_SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
    assert "SERVER=host,1433" in SqlserverSourceOperator._build_connection_string("sqlserver://host/db", None, None)
    assert SqlserverSourceOperator._normalize_dsn("jdbc:sqlserver://host;databaseName=db").startswith("DRIVER=")
    assert SqlserverSourceOperator._augment_connection_string("DRIVER={x};SERVER=host;", "user", "pwd").endswith("UID=user;PWD=pwd;")
    assert SqlserverSourceOperator._resolve_sql_query(None, "demo") == "SELECT * FROM demo"
    rows = SqlserverSourceOperator._load_rows("", None, "demo", None, None, tmp_path)
    assert rows[0]["name"] == "Alice"
    original_load_rows = SqlserverSourceOperator._load_rows
    monkeypatch.setattr(SqlserverSourceOperator, "_load_rows", classmethod(lambda cls, **kwargs: [{"id": 1}]))
    operator = SqlserverSourceOperator("sqlserver://host/db", "$", {"id": "id"})
    assert operator.explain_json()["parameters"]["source_type"] == "SQLSERVER"
    monkeypatch.setattr(SqlserverSourceOperator, "_load_rows", original_load_rows)


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_sqlserver_source_operator_rejects_invalid_url_dsn():
    with pytest.raises(ValueError):
        SqlserverSourceOperator._build_url_connection_string("postgresql://host/db", None, None)


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_sqlserver_source_operator_import_and_fetch_paths(monkeypatch, tmp_path):
    fixture = tmp_path / "resource1.sql"
    fixture.write_text("CREATE TABLE demo (id INTEGER);\nINSERT INTO demo VALUES (1);", encoding="utf-8")
    monkeypatch.setenv("FOG_RML_DB_SQLITE_FALLBACK", "0")
    with pytest.raises(ValueError):
        SqlserverSourceOperator._load_rows("", None, "demo", None, None, tmp_path)

    monkeypatch.setenv("FOG_RML_DB_SQLITE_FALLBACK", "1")
    fake_cursor = types.SimpleNamespace(
        description=[("id",)],
        execute=lambda sql: None,
        fetchall=lambda: [(1,)],
        close=lambda: None,
    )
    fake_connection = types.SimpleNamespace(cursor=lambda: fake_cursor, close=lambda: None)
    fake_pyodbc = types.SimpleNamespace(connect=lambda conn_str: fake_connection)
    monkeypatch.setattr("importlib.import_module", lambda name: fake_pyodbc)
    rows = SqlserverSourceOperator._load_rows("sqlserver://host/db", "SELECT 1", None, None, None, tmp_path)
    assert rows[0]["id"].lexical_form == "1"
    monkeypatch.setenv("FOG_RML_SQLSERVER_DSN", "sqlserver://host/db")
    assert SqlserverSourceOperator._normalize_dsn("CONNECTIONDSN") == "sqlserver://host/db"
    monkeypatch.setattr("importlib.import_module", lambda name: (_ for _ in ()).throw(ModuleNotFoundError("missing")))
    with pytest.raises(ModuleNotFoundError):
        SqlserverSourceOperator._load_rows("sqlserver://host/db", "SELECT 1", None, None, None, None)
