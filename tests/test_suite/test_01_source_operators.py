"""
Test Suite for Source Operators

This module provides comprehensive unit tests for source operators,
specifically focusing on the JsonSourceOperator implementation.
Tests validate iterator and extraction query mechanisms.
"""

import pytest
import json
import types
from pathlib import Path
from pyhartig.operators.sources.JsonSourceOperator import JsonSourceOperator
from pyhartig.operators.sources.SparqlSourceOperator import SparqlSourceOperator
from pyhartig.operators.sources.MysqlSourceOperator import MysqlSourceOperator
from pyhartig.operators.sources.PostgresqlSourceOperator import PostgresqlSourceOperator
from pyhartig.operators.sources.SqlserverSourceOperator import SqlserverSourceOperator


class TestJsonSourceOperator:
    """Test suite for JSON-based source operators."""

    @pytest.fixture
    def sample_json_data(self):
        """
        Fixture providing sample JSON data for testing.
        
        Returns:
            dict: Sample JSON structure with nested data
        """
        return {
            "project": "SPARQLLM Beta",
            "team": [
                {
                    "id": 1,
                    "name": "Alice",
                    "roles": ["Dev", "Admin"],
                    "skills": ["Python", "RDF"]
                },
                {
                    "id": 2,
                    "name": "Bob",
                    "roles": ["User"],
                    "skills": ["Java"]
                }
            ]
        }

    @pytest.fixture
    def debug_logger(self):
        """
        Fixture providing a debug logging function.
        
        Returns:
            callable: Function for structured debug output
        """

        def log(section, message):
            print(f"\n{'=' * 80}")
            print(f"[DEBUG] {section}")
            print(f"{'-' * 80}")
            print(message)
            print(f"{'=' * 80}\n")

        return log

    def test_simple_iterator_extraction(self, sample_json_data, debug_logger):
        """
        Test basic iterator functionality with simple extraction queries.
        
        Validates that the source operator correctly iterates through
        JSON arrays and extracts specified attributes.
        """
        debug_logger("Test: Simple Iterator and Extraction",
                     "Objective: Extract team member names and IDs")

        # Define source operator
        operator = JsonSourceOperator(
            source_data=sample_json_data,
            iterator_query="$.team[*]",
            attribute_mappings={
                "person_id": "$.id",
                "person_name": "$.name"
            }
        )

        debug_logger("Configuration",
                     f"Iterator: $.team[*]\n"
                     f"Mappings:\n"
                     f"  - person_id: $.id\n"
                     f"  - person_name: $.name")

        # Execute operator
        result = operator.execute()

        debug_logger("Execution Result",
                     f"Number of tuples: {len(result)}\n"
                     f"Tuples:\n" + "\n".join(f"  {i + 1}. {tuple}" for i, tuple in enumerate(result)))

        # Assertions
        assert len(result) == 2, "Should extract 2 team members"
        assert result[0]["person_name"] == "Alice"
        assert result[0]["person_id"] == 1
        assert result[1]["person_name"] == "Bob"
        assert result[1]["person_id"] == 2

        debug_logger("Validation", "✓ All assertions passed")

    def test_array_extraction(self, sample_json_data, debug_logger):
        """
        Test extraction of array-valued attributes.
        
        Validates that the operator correctly handles extraction queries
        that return multiple values, generating Cartesian products.
        """
        debug_logger("Test: Array Extraction with Cartesian Product",
                     "Objective: Extract roles and skills arrays")

        operator = JsonSourceOperator(
            source_data=sample_json_data,
            iterator_query="$.team[*]",
            attribute_mappings={
                "name": "$.name",
                "role": "$.roles[*]"
            }
        )

        debug_logger("Configuration",
                     f"Iterator: $.team[*]\n"
                     f"Mappings:\n"
                     f"  - name: $.name\n"
                     f"  - role: $.roles[*]")

        result = operator.execute()

        debug_logger("Execution Result",
                     f"Number of tuples: {len(result)}\n"
                     f"Tuples:\n" + "\n".join(f"  {i + 1}. {tuple}" for i, tuple in enumerate(result)))

        # Alice has 2 roles, Bob has 1 role
        assert len(result) == 3, "Should generate 3 tuples (2 for Alice, 1 for Bob)"

        alice_tuples = [t for t in result if t["name"] == "Alice"]
        bob_tuples = [t for t in result if t["name"] == "Bob"]

        assert len(alice_tuples) == 2
        assert len(bob_tuples) == 1

        alice_roles = {t["role"] for t in alice_tuples}
        assert alice_roles == {"Dev", "Admin"}

        debug_logger("Validation",
                     f"✓ Cartesian product correctly generated\n"
                     f"  - Alice tuples: {len(alice_tuples)}\n"
                     f"  - Bob tuples: {len(bob_tuples)}\n"
                     f"  - Alice roles: {alice_roles}")

    def test_nested_extraction(self, debug_logger):
        """
        Test extraction from nested JSON structures.
        
        Validates operator behavior with deeply nested data.
        """
        nested_data = {
            "organization": {
                "departments": [
                    {
                        "name": "Engineering",
                        "manager": {
                            "name": "Charlie",
                            "level": 5
                        }
                    },
                    {
                        "name": "Sales",
                        "manager": {
                            "name": "Diana",
                            "level": 4
                        }
                    }
                ]
            }
        }

        debug_logger("Test: Nested Extraction",
                     "Objective: Extract manager data from nested structure")

        operator = JsonSourceOperator(
            source_data=nested_data,
            iterator_query="$.organization.departments[*]",
            attribute_mappings={
                "dept": "$.name",
                "mgr_name": "$.manager.name",
                "mgr_level": "$.manager.level"
            }
        )

        debug_logger("Configuration",
                     f"Input data:\n{json.dumps(nested_data, indent=2)}\n\n"
                     f"Iterator: $.organization.departments[*]\n"
                     f"Mappings:\n"
                     f"  - dept: $.name\n"
                     f"  - mgr_name: $.manager.name\n"
                     f"  - mgr_level: $.manager.level")

        result = operator.execute()

        debug_logger("Execution Result",
                     f"Number of tuples: {len(result)}\n"
                     f"Tuples:\n" + "\n".join(f"  {i + 1}. {tuple}" for i, tuple in enumerate(result)))

        assert len(result) == 2
        assert result[0]["dept"] == "Engineering"
        assert result[0]["mgr_name"] == "Charlie"
        assert result[1]["mgr_level"] == 4

        debug_logger("Validation", "✓ Nested extraction successful")

    def test_empty_result_handling(self, debug_logger):
        """
        Test operator behavior with queries yielding no results.
        
        Validates graceful handling of empty result sets.
        """
        data = {"items": []}

        debug_logger("Test: Empty Result Handling",
                     "Objective: Validate behavior with empty iterators")

        operator = JsonSourceOperator(
            source_data=data,
            iterator_query="$.items[*]",
            attribute_mappings={"value": "$.val"}
        )

        result = operator.execute()

        debug_logger("Execution Result",
                     f"Number of tuples: {len(result)}\n"
                     f"Expected: 0 tuples for empty iterator")

        assert len(result) == 0, "Empty iterator should produce no tuples"

        debug_logger("Validation", "✓ Empty result correctly handled")

    def test_missing_attribute_extraction(self, debug_logger):
        """
        Test extraction query on non-existent attributes.
        
        Validates that missing attributes result in empty value lists,
        not errors.
        """
        data = {
            "records": [
                {"id": 1, "name": "Item1"},
                {"id": 2}  # Missing 'name' attribute
            ]
        }

        debug_logger("Test: Missing Attribute Extraction",
                     "Objective: Handle missing attributes gracefully")

        operator = JsonSourceOperator(
            source_data=data,
            iterator_query="$.records[*]",
            attribute_mappings={
                "record_id": "$.id",
                "record_name": "$.name"
            }
        )

        debug_logger("Configuration",
                     f"Input data:\n{json.dumps(data, indent=2)}\n\n"
                     f"Note: Second record lacks 'name' attribute")

        result = operator.execute()

        debug_logger("Execution Result",
                     f"Number of tuples: {len(result)}\n"
                     f"Tuples:\n" + "\n".join(f"  {i + 1}. {tuple}" for i, tuple in enumerate(result)))

        # Only the first record with both attributes should generate a tuple
        assert len(result) == 1, "Only complete records should generate tuples"
        assert result[0]["record_name"] == "Item1"

        debug_logger("Validation",
                     "✓ Missing attributes handled correctly\n"
                     "  Cartesian product with empty list yields no tuples")

    def test_cartesian_product_multiple_arrays(self, debug_logger):
        """
        Test Cartesian product with multiple array-valued extractions.
        
        Validates that the operator correctly computes the Cartesian
        product when multiple attributes have multiple values.
        """
        data = {
            "items": [
                {
                    "id": "A",
                    "colors": ["red", "blue"],
                    "sizes": ["S", "M"]
                }
            ]
        }

        debug_logger("Test: Multiple Array Cartesian Product",
                     "Objective: Generate all combinations of colors and sizes")

        operator = JsonSourceOperator(
            source_data=data,
            iterator_query="$.items[*]",
            attribute_mappings={
                "item_id": "$.id",
                "color": "$.colors[*]",
                "size": "$.sizes[*]"
            }
        )

        debug_logger("Configuration",
                     f"Input data:\n{json.dumps(data, indent=2)}\n\n"
                     f"Expected combinations: 2 colors × 2 sizes = 4 tuples")

        result = operator.execute()

        debug_logger("Execution Result",
                     f"Number of tuples: {len(result)}\n"
                     f"Tuples:\n" + "\n".join(f"  {i + 1}. {tuple}" for i, tuple in enumerate(result)))

        assert len(result) == 4, "Should generate 2×2=4 combinations"

        # Verify all combinations exist
        combinations = {(t["color"], t["size"]) for t in result}
        expected = {("red", "S"), ("red", "M"), ("blue", "S"), ("blue", "M")}
        assert combinations == expected

        debug_logger("Validation",
                     f"✓ All combinations generated correctly:\n"
                     f"  {combinations}")


class TestSparqlSourceOperator:
    def test_local_resource_emulation(self, tmp_path: Path):
        resource = tmp_path / "resource1.ttl"
        resource.write_text(
            """
            @prefix ex: <http://example.com/> .

            ex:a ex:name "Alice" .
            ex:b ex:name "Bob" .
            """,
            encoding="utf-8",
        )

        operator = SparqlSourceOperator(
            endpoint="http://localhost:PORT/ds1/sparql",
            sparql_query="""
                PREFIX ex: <http://example.com/>
                SELECT ?name WHERE { ?s ex:name ?name }
            """,
            iterator_query="$.results.bindings[*]",
            attribute_mappings={"name": "name.value"},
            mapping_dir=tmp_path,
            source_node="http://example.com/base#InputSPARQL1",
        )

        rows = operator.execute()
        names = {row["name"] for row in rows}

        assert names == {"Alice", "Bob"}


class TestMysqlSourceOperator:
    def test_mysql_query_with_mocked_driver(self, monkeypatch):
        captured = {"sql": None, "kwargs": None}

        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                return False

            def execute(self, sql):
                captured["sql"] = sql

            def fetchall(self):
                return [{"ID": 10, "Name": "Venus"}]

        class FakeConnection:
            def cursor(self):
                return FakeCursor()

            def close(self):
                return None

        fake_pymysql = types.SimpleNamespace(
            connect=lambda **kwargs: captured.update({"kwargs": kwargs}) or FakeConnection(),
            cursors=types.SimpleNamespace(DictCursor=object),
        )

        monkeypatch.setitem(__import__("sys").modules, "pymysql", fake_pymysql)

        operator = MysqlSourceOperator(
            dsn="mysql://root:secret@localhost:3306/testdb",
            query="SELECT ID, Name FROM student",
            table_name=None,
            iterator_query="$",
            attribute_mappings={"id": "ID", "name": "Name"},
        )

        rows = operator.execute()

        assert len(rows) == 1
        assert str(rows[0]["id"]) == '"10"^^http://www.w3.org/2001/XMLSchema#integer'
        assert rows[0]["name"] == "Venus"
        assert captured["sql"] == "SELECT ID, Name FROM student"
        assert captured["kwargs"]["database"] == "testdb"

    def test_mysql_sql_fixture_fallback_without_dsn(self, tmp_path: Path, monkeypatch):
        monkeypatch.delenv("PYHARTIG_MYSQL_DSN", raising=False)
        monkeypatch.setenv("PYHARTIG_DB_SQLITE_FALLBACK", "1")

        fixture = tmp_path / "resource.sql"
        fixture.write_text(
            """
            CREATE TABLE student (ID INTEGER, Name TEXT);
            INSERT INTO student (ID, Name) VALUES (10, 'Venus');
            """,
            encoding="utf-8",
        )

        operator = MysqlSourceOperator(
            dsn="CONNECTIONDSN",
            query=None,
            table_name="student",
            iterator_query="$",
            attribute_mappings={"id": "ID", "name": "Name"},
            mapping_dir=tmp_path,
        )

        rows = operator.execute()

        assert len(rows) == 1
        assert str(rows[0]["id"]) == '"10"^^http://www.w3.org/2001/XMLSchema#integer'
        assert rows[0]["name"] == "Venus"


class TestPostgresqlSourceOperator:
    def test_postgresql_query_with_mocked_driver(self, monkeypatch):
        captured = {"sql": None, "kwargs": None}

        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                return False

            def execute(self, sql):
                captured["sql"] = sql

            def fetchall(self):
                return [{"id": 7, "name": "Nadia"}]

        class FakeConnection:
            def cursor(self, cursor_factory=None):
                return FakeCursor()

            def close(self):
                return None

        fake_psycopg2 = types.SimpleNamespace(
            connect=lambda **kwargs: captured.update({"kwargs": kwargs}) or FakeConnection(),
        )
        fake_psycopg2_extras = types.SimpleNamespace(RealDictCursor=object)

        monkeypatch.setitem(__import__("sys").modules, "psycopg2", fake_psycopg2)
        monkeypatch.setitem(__import__("sys").modules, "psycopg2.extras", fake_psycopg2_extras)

        operator = PostgresqlSourceOperator(
            dsn="postgresql://postgres:secret@localhost:5432/sampledb",
            query="SELECT id, name FROM student",
            table_name=None,
            iterator_query="$",
            attribute_mappings={"id": "id", "name": "name"},
        )

        rows = operator.execute()

        assert len(rows) == 1
        assert str(rows[0]["id"]) == '"7"^^http://www.w3.org/2001/XMLSchema#integer'
        assert rows[0]["name"] == "Nadia"
        assert captured["sql"] == "SELECT id, name FROM student"
        assert captured["kwargs"]["dbname"] == "sampledb"

    def test_postgresql_sql_fixture_fallback_without_dsn(self, tmp_path: Path, monkeypatch):
        monkeypatch.delenv("PYHARTIG_POSTGRES_DSN", raising=False)
        monkeypatch.setenv("PYHARTIG_DB_SQLITE_FALLBACK", "1")

        fixture = tmp_path / "resource.sql"
        fixture.write_text(
            """
            CREATE TABLE student (id INTEGER, name TEXT);
            INSERT INTO student (id, name) VALUES (7, 'Nadia');
            """,
            encoding="utf-8",
        )

        operator = PostgresqlSourceOperator(
            dsn="CONNECTIONDSN",
            query=None,
            table_name="student",
            iterator_query="$",
            attribute_mappings={"id": "id", "name": "name"},
            mapping_dir=tmp_path,
        )

        rows = operator.execute()

        assert len(rows) == 1
        assert str(rows[0]["id"]) == '"7"^^http://www.w3.org/2001/XMLSchema#integer'
        assert rows[0]["name"] == "Nadia"


class TestSqlserverSourceOperator:
    def test_sqlserver_query_with_mocked_driver(self, monkeypatch):
        captured = {"sql": None, "conn": None}

        class FakeCursor:
            description = [("id",), ("name",)]

            def execute(self, sql):
                captured["sql"] = sql

            def fetchall(self):
                return [(3, "Lina")]

            def close(self):
                return None

        class FakeConnection:
            def cursor(self):
                return FakeCursor()

            def close(self):
                return None

        fake_pyodbc = types.SimpleNamespace(
            connect=lambda conn: captured.update({"conn": conn}) or FakeConnection(),
        )

        monkeypatch.setitem(__import__("sys").modules, "pyodbc", fake_pyodbc)

        operator = SqlserverSourceOperator(
            dsn="sqlserver://sa:secret@localhost:1433/master",
            query="SELECT id, name FROM student",
            table_name=None,
            iterator_query="$",
            attribute_mappings={"id": "id", "name": "name"},
        )

        rows = operator.execute()

        assert len(rows) == 1
        assert str(rows[0]["id"]) == '"3"^^http://www.w3.org/2001/XMLSchema#integer'
        assert rows[0]["name"] == "Lina"
        assert captured["sql"] == "SELECT id, name FROM student"
        assert "SERVER=localhost,1433" in captured["conn"]

    def test_sqlserver_sql_fixture_fallback_without_dsn(self, tmp_path: Path, monkeypatch):
        monkeypatch.delenv("PYHARTIG_SQLSERVER_DSN", raising=False)
        monkeypatch.setenv("PYHARTIG_DB_SQLITE_FALLBACK", "1")

        fixture = tmp_path / "resource.sql"
        fixture.write_text(
            """
            CREATE TABLE student (id INTEGER, name TEXT);
            INSERT INTO student (id, name) VALUES (3, 'Lina');
            """,
            encoding="utf-8",
        )

        operator = SqlserverSourceOperator(
            dsn="CONNECTIONDSN",
            query=None,
            table_name="student",
            iterator_query="$",
            attribute_mappings={"id": "id", "name": "name"},
            mapping_dir=tmp_path,
        )

        rows = operator.execute()

        assert len(rows) == 1
        assert str(rows[0]["id"]) == '"3"^^http://www.w3.org/2001/XMLSchema#integer'
        assert rows[0]["name"] == "Lina"
