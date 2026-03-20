from __future__ import annotations

import types

import pytest

from pyhartig.operators.sources.PostgresqlSourceOperator import PostgresqlSourceOperator


@pytest.mark.coverage_suite
def test_postgresql_source_operator_normalizes_dsn_and_connection_kwargs(monkeypatch, tmp_path):
    fixture = tmp_path / "resource1.sql"
    fixture.write_text("CREATE TABLE demo (id INTEGER, name TEXT);\nINSERT INTO demo VALUES (1, 'Alice');", encoding="utf-8")

    assert PostgresqlSourceOperator._normalize_dsn("jdbc:postgresql://host/db").startswith("postgresql://")
    monkeypatch.setenv("PYHARTIG_POSTGRES_DSN", "postgresql://user:pass@host:5432/db")
    assert PostgresqlSourceOperator._normalize_dsn("CONNECTIONDSN").startswith("postgresql://")
    kwargs = PostgresqlSourceOperator._build_connection_kwargs("postgresql://user:pass@host:5432/db", None, None)
    assert kwargs["dbname"] == "db"
    rows = PostgresqlSourceOperator._load_rows("", None, "demo", None, None, tmp_path)
    assert rows[0]["name"] == "Alice"

    original_load_rows = PostgresqlSourceOperator._load_rows
    monkeypatch.setattr(PostgresqlSourceOperator, "_load_rows", classmethod(lambda cls, **kwargs: [{"id": 1}]))
    operator = PostgresqlSourceOperator("postgresql://user:pass@host:5432/db", "$", {"id": "id"})
    assert operator.explain_json()["parameters"]["source_type"] == "POSTGRESQL"
    monkeypatch.setattr(PostgresqlSourceOperator, "_load_rows", original_load_rows)


@pytest.mark.edge_case
def test_postgresql_source_operator_requires_complete_connection_info():
    with pytest.raises(ValueError):
        PostgresqlSourceOperator._build_connection_kwargs("postgresql:///db", None, None)


@pytest.mark.edge_case
def test_postgresql_source_operator_handles_import_and_connection_paths(monkeypatch, tmp_path):
    fixture = tmp_path / "resource1.sql"
    fixture.write_text("CREATE TABLE demo (id INTEGER);\nINSERT INTO demo VALUES (1);", encoding="utf-8")
    monkeypatch.setenv("PYHARTIG_DB_SQLITE_FALLBACK", "0")
    with pytest.raises(ValueError):
        PostgresqlSourceOperator._load_rows("", None, "demo", None, None, tmp_path)

    monkeypatch.setenv("PYHARTIG_DB_SQLITE_FALLBACK", "1")
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
    fake_connection = types.SimpleNamespace(cursor=lambda cursor_factory=None: fake_cursor, close=lambda: None)
    fake_psycopg2 = types.SimpleNamespace(connect=lambda **kwargs: fake_connection)
    fake_extras = types.SimpleNamespace(RealDictCursor=object())
    monkeypatch.setattr(
        "importlib.import_module",
        lambda name: fake_psycopg2 if name == "psycopg2" else fake_extras,
    )
    rows = PostgresqlSourceOperator._load_rows("postgresql://user:pass@host:5432/db", "SELECT id FROM demo", None, None, None, tmp_path)
    assert rows[0]["id"].lexical_form == "1"
    monkeypatch.setattr("importlib.import_module", lambda name: (_ for _ in ()).throw(ModuleNotFoundError("missing")))
    with pytest.raises(ModuleNotFoundError):
        PostgresqlSourceOperator._load_rows("postgresql://user:pass@host:5432/db", "SELECT id FROM demo", None, None, None, None)
