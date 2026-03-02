import os
import importlib
from typing import Dict, Any, Optional
from urllib.parse import urlparse
from pathlib import Path

from pyhartig.operators.sources.CsvSourceOperator import CsvSourceOperator
from pyhartig.operators.sources.sql_fixture_fallback import load_rows_from_sql_fixture, normalize_db_rows


class MysqlSourceOperator(CsvSourceOperator):
	def __init__(
		self,
		dsn: str,
		iterator_query: str,
		attribute_mappings: Dict[str, str],
		query: Optional[str] = None,
		table_name: Optional[str] = None,
		username: Optional[str] = None,
		password: Optional[str] = None,
		mapping_dir: Optional[Path] = None,
	):
		rows = self._load_rows(
			dsn=dsn,
			query=query,
			table_name=table_name,
			username=username,
			password=password,
			mapping_dir=mapping_dir,
		)
		super().__init__(source_data=rows, iterator_query=iterator_query, attribute_mappings=attribute_mappings)
		self._dsn = dsn
		self._query = query
		self._table_name = table_name

	@staticmethod
	def _normalize_dsn(dsn: str) -> str:
		dsn_str = (dsn or "").strip()
		if not dsn_str:
			return ""

		if dsn_str == "CONNECTIONDSN":
			env_dsn = os.getenv("PYHARTIG_MYSQL_DSN") or os.getenv("MYSQL_DSN")
			return (env_dsn or "").strip()

		if dsn_str.startswith("jdbc:mysql://"):
			return "mysql://" + dsn_str[len("jdbc:mysql://"):]

		return dsn_str

	@staticmethod
	def _build_connection_kwargs(dsn: str, username: Optional[str], password: Optional[str]) -> Dict[str, Any]:
		parsed = urlparse(dsn)
		if parsed.scheme != "mysql":
			raise ValueError(
				"Unsupported MySQL DSN. Use mysql://user:pass@host:3306/database or jdbc:mysql://..."
			)

		db_user = username or parsed.username
		db_password = password if password is not None else (parsed.password or "")
		db_name = parsed.path.lstrip("/") if parsed.path else ""

		if not parsed.hostname or not db_user or not db_name:
			raise ValueError(
				"Incomplete MySQL connection information. Required: host, username, database."
			)

		return {
			"host": parsed.hostname,
			"port": parsed.port or 3306,
			"user": db_user,
			"password": db_password,
			"database": db_name,
			"charset": "utf8mb4",
			"autocommit": True,
		}

	@classmethod
	def _load_rows(
		cls,
		dsn: str,
		query: Optional[str],
		table_name: Optional[str],
		username: Optional[str],
		password: Optional[str],
		mapping_dir: Optional[Path],
	) -> list:
		normalized_dsn = cls._normalize_dsn(dsn)

		allow_fixture_fallback = os.getenv("PYHARTIG_DB_SQLITE_FALLBACK", "1") != "0"
		if not normalized_dsn:
			if allow_fixture_fallback and mapping_dir is not None:
				return load_rows_from_sql_fixture(mapping_dir=mapping_dir, query=query, table_name=table_name)
			raise ValueError("MySQL DSN is missing. Set rml:source DSN or environment variable MYSQL_DSN.")

		sql = (query or "").strip()
		if not sql:
			if table_name:
				sql = f"SELECT * FROM {table_name}"
			else:
				raise ValueError("MySQL logical source requires rml:query or rr:tableName.")

		try:
			pymysql = importlib.import_module("pymysql")
		except Exception as exc:
			raise ModuleNotFoundError(
				"pymysql is required for MysqlSourceOperator. Install it with 'pip install pymysql'."
			) from exc

		conn_kwargs = cls._build_connection_kwargs(normalized_dsn, username=username, password=password)
		conn_kwargs["cursorclass"] = pymysql.cursors.DictCursor

		try:
			connection = pymysql.connect(**conn_kwargs)
			try:
				with connection.cursor() as cursor:
					cursor.execute(sql)
					rows = cursor.fetchall() or []
			finally:
				connection.close()
		except Exception:
			if allow_fixture_fallback and mapping_dir is not None:
				return load_rows_from_sql_fixture(mapping_dir=mapping_dir, query=query, table_name=table_name)
			raise

		return normalize_db_rows([dict(r) for r in rows])

	def explain_json(self) -> Dict[str, Any]:
		base = super().explain_json()
		base["parameters"]["source_type"] = "MYSQL"
		base["parameters"]["dsn"] = self._dsn
		base["parameters"]["query"] = self._query
		base["parameters"]["table_name"] = self._table_name
		return base
