import re
import sqlite3
import os
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import date, datetime
from decimal import Decimal

from pyhartig.algebra.Terms import Literal as AlgebraLiteral
from pyhartig.namespaces import (
	XSD_BOOLEAN,
	XSD_DATE,
	XSD_DATETIME,
	XSD_DECIMAL,
	XSD_DOUBLE,
	XSD_INTEGER,
)

_SQL_IDENTIFIER = r"[A-Za-z_]\w*"
_CREATE_TABLE_PATTERN = re.compile(
	rf'(?is)CREATE\s+TABLE\s+"?({_SQL_IDENTIFIER})"?\s*\((.*?)\)\s*;',
	re.ASCII,
)
_COLUMN_DEF_PATTERN = re.compile(
	rf'(?i)^"?({_SQL_IDENTIFIER})"?\s+([A-Za-z]+)(?:\s*\(\s*(\d+)\s*\))?',
	re.ASCII,
)
_FROM_TABLE_PATTERN = re.compile(rf'(?i)\bFROM\s+"?({_SQL_IDENTIFIER})"?', re.ASCII)
_CONCAT_WS_LEFT_PATTERN = re.compile(
	rf"(?i)concat_ws\s*\(\s*({_SQL_IDENTIFIER})\s*,\s*''\s*,\s*({_SQL_IDENTIFIER})\s*\)",
	re.ASCII,
)
_CONCAT_WS_RIGHT_PATTERN = re.compile(
	rf"(?i)concat_ws\s*\(\s*''\s*,\s*({_SQL_IDENTIFIER})\s*,\s*({_SQL_IDENTIFIER})\s*\)",
	re.ASCII,
)
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$")


def _drop_sqlite_unsupported_line(stripped_line: str) -> bool:
	"""
	Checks whether a SQL fixture line should be dropped for sqlite compatibility.
	:param stripped_line: SQL line stripped from leading/trailing whitespace.
	:return: True when the line should be removed.
	"""
	upper_line = stripped_line.upper()
	return (
		(upper_line.startswith("USE ") and stripped_line.endswith(";"))
		or ((upper_line.startswith("CREATE DATABASE ") or upper_line.startswith("DROP DATABASE ")) and stripped_line.endswith(";"))
		or upper_line == "GO"
		or (upper_line.startswith("SET ") and stripped_line.endswith(";"))
		or upper_line.startswith("EXEC ")
	)


def _rewrite_drop_table_cascade(line: str) -> str:
	"""
	Rewrites `DROP TABLE ... CASCADE` statements to sqlite-compatible syntax.
	:param line: Raw SQL line.
	:return: Normalized SQL line.
	"""
	stripped_line = line.strip()
	upper_line = stripped_line.upper()
	if not (upper_line.startswith("DROP TABLE IF EXISTS ") and stripped_line.endswith(";")):
		return line

	statement = stripped_line[:-1].rstrip()
	if not statement.upper().endswith(" CASCADE"):
		return line

	indent = line[:len(line) - len(line.lstrip())]
	statement = statement[:-len("CASCADE")].rstrip()
	return f"{indent}{statement};"


def _normalize_sql_script(script: str) -> str:
	"""
	Normalizes a SQL fixture script for sqlite execution.
	:param script: Raw SQL fixture script.
	:return: Sqlite-compatible SQL script.
	"""
	text = script

	# Remove database-selection/setup statements not supported by sqlite
	normalized_lines = []
	for raw_line in text.splitlines():
		stripped_line = raw_line.strip()
		if _drop_sqlite_unsupported_line(stripped_line):
			continue
		normalized_lines.append(_rewrite_drop_table_cascade(raw_line))
	text = "\n".join(normalized_lines)

	# Replace schema-qualified references commonly used in fixtures
	text = re.sub(r"(?i)\btest\.", "", text)

	# SQLServer/PostgreSQL binary literal patterns
	text = re.sub(
		r"(?i)CAST\(\s*'([0-9A-F]+)'\s+AS\s+VARBINARY(?:\s*\([^\)]*\))?\s*\)",
		r"X'\1'",
		text,
	)
	text = re.sub(r"'\\\\x([0-9A-Fa-f]+)'", r"X'\1'", text)

	# SQLServer-specific types not recognized by sqlite
	text = re.sub(r"(?i)\bDATETIME\b", "TEXT", text)
	text = re.sub(r"(?i)\bTIMESTAMP\b", "TEXT", text)
	text = re.sub(r"(?i)\bBIT\b", "INTEGER", text)

	return text


def _extract_schema_hints(script: str) -> Dict[str, Dict[str, Dict[str, Any]]]:
	"""
	Extracts column type hints from CREATE TABLE statements.
	:param script: Raw SQL fixture script.
	:return: Table-to-column schema hints.
	"""
	schema: Dict[str, Dict[str, Dict[str, Any]]] = {}
	for match in _CREATE_TABLE_PATTERN.finditer(script):
		table = match.group(1).lower()
		cols = _extract_table_column_hints(match.group(2))
		if cols:
			schema[table] = cols
	return schema


def _extract_table_column_hints(table_body: str) -> Dict[str, Dict[str, Any]]:
	"""
	Extracts column hints from the body of a CREATE TABLE statement.
	:param table_body: Body of a CREATE TABLE statement.
	:return: Column hints for the table body.
	"""
	column_hints: Dict[str, Dict[str, Any]] = {}
	for raw_line in table_body.splitlines():
		column_hint = _parse_column_hint(raw_line.strip().rstrip(","))
		if column_hint is None:
			continue
		column_name, hint = column_hint
		column_hints[column_name] = hint
	return column_hints


def _parse_column_hint(line: str) -> Optional[tuple[str, Dict[str, Any]]]:
	"""
	Parses a single column definition line into a schema hint.
	:param line: Column definition line.
	:return: Tuple of column name and hint dictionary, or None.
	"""
	if not line or _is_constraint_line(line):
		return None
	column_match = _COLUMN_DEF_PATTERN.match(line)
	if not column_match:
		return None
	column_size = int(column_match.group(3)) if column_match.group(3) else None
	return column_match.group(1).lower(), {
		"type": column_match.group(2).lower(),
		"size": column_size,
	}


def _is_constraint_line(line: str) -> bool:
	"""
	Checks whether a CREATE TABLE line defines a table constraint instead of a column.
	:param line: CREATE TABLE line.
	:return: True when the line describes a constraint.
	"""
	return bool(re.match(r"(?i)^(PRIMARY|FOREIGN|UNIQUE|CONSTRAINT|CHECK)\b", line))


def _normalize_select_sql(sql: str) -> str:
	"""
	Normalizes SELECT statements for sqlite compatibility.
	:param sql: Raw SQL query.
	:return: Normalized SQL query.
	"""
	q = sql.strip()
	if not q:
		return q

	# Normalize concat_ws(a, '', b) / concat_ws('', a, b) for sqlite compatibility
	q = _CONCAT_WS_LEFT_PATTERN.sub(r"(\1 || \2)", q)
	q = _CONCAT_WS_RIGHT_PATTERN.sub(r"(\1 || \2)", q)

	return q


def _extract_query_table_name(sql: str) -> Optional[str]:
	"""
	Extracts the main table name from a SELECT query.
	:param sql: SQL query string.
	:return: Table name, or None.
	"""
	if not sql:
		return None
	m = _FROM_TABLE_PATTERN.search(sql)
	if not m:
		return None
	return m.group(1)


def _normalize_cell_value(value: Any, hint: Optional[Dict[str, Any]] = None) -> Any:
	"""
	Normalizes a SQL cell value to the RDF-friendly representation expected by the engine.
	:param value: Raw cell value.
	:param hint: Optional schema hint for the column.
	:return: Normalized cell value.
	"""
	hint_type = (hint or {}).get("type", "") if hint else ""
	hint_size = (hint or {}).get("size") if hint else None
	boolean_value = _normalize_boolean_value(value, hint_type)
	if boolean_value is not None:
		return boolean_value

	value = _apply_char_padding(value, hint_type, hint_size)
	hinted_temporal_value = _normalize_hinted_temporal_value(value, hint_type)
	if hinted_temporal_value is not None:
		return hinted_temporal_value

	native_value = _normalize_native_typed_value(value)
	if native_value is not None:
		return native_value

	string_temporal_value = _normalize_string_temporal_value(value)
	if string_temporal_value is not None:
		return string_temporal_value

	binary_value = _normalize_binary_value(value)
	return value if binary_value is None else binary_value


def _normalize_boolean_value(value: Any, hint_type: str) -> AlgebraLiteral | None:
	"""
	Normalizes boolean-like values using schema hints when available.
	:param value: Raw cell value.
	:param hint_type: Column type hint.
	:return: Boolean AlgebraLiteral, or None.
	"""
	if hint_type not in ("boolean", "bool", "bit"):
		return None
	if isinstance(value, (int, float)):
		return AlgebraLiteral("true" if int(value) != 0 else "false", XSD_BOOLEAN.value)
	if not isinstance(value, str):
		return None

	lowered_value = value.strip().lower()
	if lowered_value in ("1", "true", "t", "yes"):
		return AlgebraLiteral("true", XSD_BOOLEAN.value)
	if lowered_value in ("0", "false", "f", "no"):
		return AlgebraLiteral("false", XSD_BOOLEAN.value)
	return None


def _apply_char_padding(value: Any, hint_type: str, hint_size: Optional[Any]) -> Any:
	"""
	Applies SQL CHAR-style padding when the schema hint requires it.
	:param value: Raw cell value.
	:param hint_type: Column type hint.
	:param hint_size: Optional column size hint.
	:return: Possibly padded value.
	"""
	if hint_type == "char" and isinstance(value, str) and hint_size:
		return value.ljust(int(hint_size))
	return value


def _normalize_hinted_temporal_value(value: Any, hint_type: str) -> AlgebraLiteral | None:
	"""
	Normalizes hinted temporal strings to typed RDF literals.
	:param value: Raw cell value.
	:param hint_type: Column type hint.
	:return: Typed temporal AlgebraLiteral, or None.
	"""
	if not isinstance(value, str):
		return None
	if hint_type == "date" and _DATE_PATTERN.match(value):
		return AlgebraLiteral(value, XSD_DATE.value)
	if hint_type in ("datetime", "timestamp") and _DATETIME_PATTERN.match(value):
		return AlgebraLiteral(value.replace(" ", "T"), XSD_DATETIME.value)
	return None


def _normalize_native_typed_value(value: Any) -> AlgebraLiteral | None:
	"""
	Normalizes native Python values to typed RDF literals.
	:param value: Raw cell value.
	:return: Typed AlgebraLiteral, or None.
	"""
	if isinstance(value, bool):
		return AlgebraLiteral("true" if value else "false", XSD_BOOLEAN.value)
	if isinstance(value, int):
		return AlgebraLiteral(str(value), XSD_INTEGER.value)
	if isinstance(value, Decimal):
		return AlgebraLiteral(str(value), XSD_DECIMAL.value)
	if isinstance(value, float):
		return AlgebraLiteral(str(value), XSD_DOUBLE.value)
	if isinstance(value, datetime):
		return AlgebraLiteral(value.isoformat(), XSD_DATETIME.value)
	if isinstance(value, date):
		return AlgebraLiteral(value.isoformat(), XSD_DATE.value)
	return None


def _normalize_string_temporal_value(value: Any) -> AlgebraLiteral | None:
	"""
	Normalizes unhinted temporal strings to typed RDF literals.
	:param value: Raw cell value.
	:return: Typed temporal AlgebraLiteral, or None.
	"""
	if not isinstance(value, str):
		return None
	if _DATE_PATTERN.match(value):
		return AlgebraLiteral(value, XSD_DATE.value)
	if _DATETIME_PATTERN.match(value):
		return AlgebraLiteral(value.replace(" ", "T"), XSD_DATETIME.value)
	return None


def _normalize_binary_value(value: Any) -> Any:
	"""
	Normalizes binary-like values to uppercase hexadecimal strings.
	:param value: Raw cell value.
	:return: Uppercase hexadecimal representation, or None.
	"""
	if isinstance(value, (bytes, bytearray, memoryview)):
		return bytes(value).hex().upper()
	if isinstance(value, str) and value.startswith("\\x"):
		return value[2:].upper()
	return None


def normalize_db_rows(rows: List[Dict[str, Any]], column_hints: Optional[Dict[str, Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
	"""
	Normalizes a list of database rows using optional schema hints.
	:param rows: Database rows to normalize.
	:param column_hints: Optional column hints indexed by lowercase column name.
	:return: Normalized database rows.
	"""
	normalized: List[Dict[str, Any]] = []
	for row in rows:
		normalized.append({
			k: _normalize_cell_value(v, (column_hints or {}).get(str(k).lower()))
			for k, v in row.items()
		})
	return normalized


def load_rows_from_sql_fixture(
	mapping_dir: Optional[Path],
	query: Optional[str],
	table_name: Optional[str],
) -> List[Dict[str, Any]]:
	"""
	Loads rows from a local SQL fixture using sqlite as a compatibility backend.
	:param mapping_dir: Mapping directory containing `resource*.sql` fixtures.
	:param query: Optional SQL query.
	:param table_name: Optional table name used when no explicit query is provided.
	:return: Normalized database rows.
	"""
	strict_mode = ("1" == os.getenv("PYHARTIG_STRICT_REFERENCES", "0"))
	mapping_dir = _require_mapping_dir(mapping_dir)
	_, normalized_script, schema_hints = _load_fixture_artifacts(mapping_dir)
	sql = _resolve_fixture_sql(query, table_name, strict_mode, normalized_script)
	query_table = (_extract_query_table_name(sql) or table_name or "").lower()
	column_hints = schema_hints.get(query_table, {})
	row_dicts = _execute_fixture_query(normalized_script, sql, strict_mode)
	return normalize_db_rows(row_dicts, column_hints=column_hints)


def _require_mapping_dir(mapping_dir: Optional[Path]) -> Path:
	"""
	Validates the mapping directory used for SQL fixture fallback.
	:param mapping_dir: Optional mapping directory.
	:return: Validated mapping directory path.
	"""
	if mapping_dir is None:
		raise ValueError("Local SQL fixture fallback requires mapping_dir.")
	return mapping_dir


def _load_fixture_artifacts(mapping_dir: Path):
	"""
	Loads the SQL fixture script and its derived normalization artifacts.
	:param mapping_dir: Mapping directory containing `resource*.sql` fixtures.
	:return: Tuple of raw script, normalized script, and schema hints.
	"""
	fixture_files = sorted(mapping_dir.glob("resource*.sql"))
	if not fixture_files:
		raise ValueError(f"No local SQL fixture found in {mapping_dir}.")

	fixture_path = fixture_files[0]
	script = fixture_path.read_text(encoding="utf-8", errors="ignore")
	return script, _normalize_sql_script(script), _extract_schema_hints(script)


def _resolve_fixture_sql(
	query: Optional[str],
	table_name: Optional[str],
	strict_mode: bool,
	normalized_script: str,
) -> str:
	"""
	Resolves the SQL statement executed against the sqlite fixture.
	:param query: Optional SQL query.
	:param table_name: Optional table name used when no explicit query is provided.
	:param strict_mode: Whether strict reference behavior is enabled.
	:param normalized_script: Sqlite-compatible SQL fixture script.
	:return: Normalized SQL query string.
	"""
	sql = (query or "").strip()
	if sql:
		return _normalize_select_sql(sql)
	if not table_name:
		raise ValueError("Logical source requires rml:query or rr:tableName.")

	if strict_mode:
		_validate_strict_table_name(table_name, normalized_script)
	return _normalize_select_sql(f"SELECT * FROM {table_name}")


def _validate_strict_table_name(table_name: str, normalized_script: str) -> None:
	"""
	Validates strict-mode table-name expectations against the sqlite fixture.
	:param table_name: Table name requested by the logical source.
	:param normalized_script: Sqlite-compatible SQL fixture script.
	:return: None
	"""
	if any(ch.isupper() for ch in str(table_name)):
		raise ValueError(f"Invalid table identifier in strict mode: {table_name}")
	with sqlite3.connect(":memory:") as strict_conn:
		strict_conn.executescript(normalized_script)
		rows = strict_conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
		table_names = {row[0] for row in rows}
		if table_name not in table_names:
			raise ValueError(f"Table not found (strict mode): {table_name}")


def _execute_fixture_query(normalized_script: str, sql: str, strict_mode: bool) -> List[Dict[str, Any]]:
	"""
	Executes a SQL query against the sqlite fixture backend.
	:param normalized_script: Sqlite-compatible SQL fixture script.
	:param sql: SQL query string.
	:param strict_mode: Whether strict reference behavior is enabled.
	:return: List of row dictionaries.
	"""
	connection = sqlite3.connect(":memory:")
	connection.row_factory = sqlite3.Row
	try:
		connection.executescript(normalized_script)
		cursor = connection.cursor()
		try:
			try:
				cursor.execute(sql)
			except sqlite3.OperationalError:
				if strict_mode:
					raise
				return []
			rows = cursor.fetchall() or []
		finally:
			cursor.close()
	finally:
		connection.close()
	return [dict(row) for row in rows]
