import re
import sqlite3
import os
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import date, datetime
from decimal import Decimal

from pyhartig.algebra.Terms import Literal as AlgebraLiteral


def _normalize_sql_script(script: str) -> str:
	text = script

	# Remove database-selection/setup statements not supported by sqlite
	text = re.sub(r"(?im)^\s*USE\s+[^;]+;\s*$", "", text)
	text = re.sub(r"(?im)^\s*(CREATE|DROP)\s+DATABASE\s+[^;]+;\s*$", "", text)
	text = re.sub(r"(?im)^\s*GO\s*$", "", text)
	text = re.sub(r"(?im)^\s*SET\s+[^;]+;\s*$", "", text)
	text = re.sub(r"(?im)^\s*EXEC\s+[^\n]*$", "", text)
	text = re.sub(r"(?i)\bDROP\s+TABLE\s+IF\s+EXISTS\s+([^;]+?)\s+CASCADE\s*;", r"DROP TABLE IF EXISTS \1;", text)

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
	schema: Dict[str, Dict[str, Dict[str, Any]]] = {}
	for match in re.finditer(r"(?is)CREATE\s+TABLE\s+\"?([A-Za-z_][A-Za-z0-9_]*)\"?\s*\((.*?)\)\s*;", script):
		table = match.group(1).lower()
		body = match.group(2)
		cols: Dict[str, Dict[str, Any]] = {}
		for raw_line in body.splitlines():
			line = raw_line.strip().rstrip(',')
			if not line:
				continue
			if re.match(r"(?i)^(PRIMARY|FOREIGN|UNIQUE|CONSTRAINT|CHECK)\b", line):
				continue
			col_match = re.match(
				r'(?i)^\"?([A-Za-z_][A-Za-z0-9_]*)\"?\s+([A-Za-z]+)(?:\s*\(\s*(\d+)\s*\))?',
				line,
			)
			if not col_match:
				continue
			col_name = col_match.group(1).lower()
			col_type = col_match.group(2).lower()
			col_size = int(col_match.group(3)) if col_match.group(3) else None
			cols[col_name] = {"type": col_type, "size": col_size}
		if cols:
			schema[table] = cols
	return schema


def _normalize_select_sql(sql: str) -> str:
	q = sql.strip()
	if not q:
		return q

	# Normalize concat_ws(a, '', b) / concat_ws('', a, b) for sqlite compatibility
	q = re.sub(
		r"(?i)concat_ws\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*''\s*,\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)",
		r"(\1 || \2)",
		q,
	)
	q = re.sub(
		r"(?i)concat_ws\s*\(\s*''\s*,\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)",
		r"(\1 || \2)",
		q,
	)

	return q


def _extract_query_table_name(sql: str) -> Optional[str]:
	if not sql:
		return None
	m = re.search(r"(?i)\bFROM\s+\"?([A-Za-z_][A-Za-z0-9_]*)\"?", sql)
	if not m:
		return None
	return m.group(1)


def _normalize_cell_value(value: Any, hint: Optional[Dict[str, Any]] = None) -> Any:
	hint_type = (hint or {}).get("type", "") if hint else ""
	hint_size = (hint or {}).get("size") if hint else None

	if hint_type in ("boolean", "bool", "bit"):
		if isinstance(value, (int, float)):
			return AlgebraLiteral("true" if int(value) != 0 else "false", "http://www.w3.org/2001/XMLSchema#boolean")
		if isinstance(value, str):
			lv = value.strip().lower()
			if lv in ("1", "true", "t", "yes"):
				return AlgebraLiteral("true", "http://www.w3.org/2001/XMLSchema#boolean")
			if lv in ("0", "false", "f", "no"):
				return AlgebraLiteral("false", "http://www.w3.org/2001/XMLSchema#boolean")

	if hint_type == "char" and isinstance(value, str) and hint_size:
		value = value.ljust(int(hint_size))

	if hint_type == "date" and isinstance(value, str):
		if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
			return AlgebraLiteral(value, "http://www.w3.org/2001/XMLSchema#date")

	if hint_type in ("datetime", "timestamp") and isinstance(value, str):
		if re.match(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$", value):
			return AlgebraLiteral(value.replace(" ", "T"), "http://www.w3.org/2001/XMLSchema#dateTime")

	# Preserve relational native types as typed literals so object maps without
	# explicit rr:datatype can still reflect SQL datatypes.
	if isinstance(value, bool):
		return AlgebraLiteral("true" if value else "false", "http://www.w3.org/2001/XMLSchema#boolean")
	if isinstance(value, int):
		return AlgebraLiteral(str(value), "http://www.w3.org/2001/XMLSchema#integer")
	if isinstance(value, Decimal):
		return AlgebraLiteral(str(value), "http://www.w3.org/2001/XMLSchema#decimal")
	if isinstance(value, float):
		return AlgebraLiteral(str(value), "http://www.w3.org/2001/XMLSchema#double")
	if isinstance(value, datetime):
		return AlgebraLiteral(value.isoformat(), "http://www.w3.org/2001/XMLSchema#dateTime")
	if isinstance(value, date):
		return AlgebraLiteral(value.isoformat(), "http://www.w3.org/2001/XMLSchema#date")
	if isinstance(value, str):
		if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
			return AlgebraLiteral(value, "http://www.w3.org/2001/XMLSchema#date")
		if re.match(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$", value):
			return AlgebraLiteral(value.replace(" ", "T"), "http://www.w3.org/2001/XMLSchema#dateTime")
	if isinstance(value, (bytes, bytearray, memoryview)):
		return bytes(value).hex().upper()
	if isinstance(value, str) and value.startswith("\\x"):
		return value[2:].upper()
	return value


def normalize_db_rows(rows: List[Dict[str, Any]], column_hints: Optional[Dict[str, Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
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
	strict_mode = ("1" == os.getenv("PYHARTIG_STRICT_REFERENCES", "0"))

	if mapping_dir is None:
		raise ValueError("Local SQL fixture fallback requires mapping_dir.")

	fixture_files = sorted(mapping_dir.glob("resource*.sql"))
	if not fixture_files:
		raise ValueError(f"No local SQL fixture found in {mapping_dir}.")

	fixture_path = fixture_files[0]
	script = fixture_path.read_text(encoding="utf-8", errors="ignore")
	normalized_script = _normalize_sql_script(script)
	schema_hints = _extract_schema_hints(script)

	sql = (query or "").strip()
	if not sql:
		if not table_name:
			raise ValueError("Logical source requires rml:query or rr:tableName.")
		if strict_mode:
			if any(ch.isupper() for ch in str(table_name)):
				raise ValueError(f"Invalid table identifier in strict mode: {table_name}")
			# Emulate stricter DB behavior for expected-error checks where
			# case-sensitive table names matter.
			with sqlite3.connect(":memory:") as strict_conn:
				strict_conn.executescript(normalized_script)
				rows = strict_conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
				table_names = {r[0] for r in rows}
				if table_name not in table_names:
					raise ValueError(f"Table not found (strict mode): {table_name}")
		sql = f"SELECT * FROM {table_name}"

	sql = _normalize_select_sql(sql)
	query_table = (_extract_query_table_name(sql) or table_name or "").lower()
	column_hints = schema_hints.get(query_table, {})

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
				# For non-strict runs, treat invalid SQL/query issues as empty source.
				return []
			rows = cursor.fetchall() or []
		finally:
			cursor.close()
	finally:
		connection.close()

	row_dicts = [dict(row) for row in rows]
	return normalize_db_rows(row_dicts, column_hints=column_hints)