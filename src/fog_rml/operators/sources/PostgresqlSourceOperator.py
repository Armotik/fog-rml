import os
import importlib
from typing import Dict, Any, Optional
from urllib.parse import urlparse
from pathlib import Path

from fog_rml.operators.sources.CsvSourceOperator import CsvSourceOperator
from fog_rml.operators.sources.sql_fixture_fallback import load_rows_from_sql_fixture, normalize_db_rows


class PostgresqlSourceOperator(CsvSourceOperator):
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
			env_dsn = os.getenv("FOG_RML_POSTGRES_DSN") or os.getenv("FOG_RML_POSTGRES_DSN") or os.getenv("POSTGRES_DSN")
			return (env_dsn or "").strip()

		if dsn_str.startswith("jdbc:postgresql://"):
			return "postgresql://" + dsn_str[len("jdbc:postgresql://"):]

		return dsn_str

	@staticmethod
	def _build_connection_kwargs(dsn: str, username: Optional[str], password: Optional[str]) -> Dict[str, Any]:
		parsed = urlparse(dsn)
		if parsed.scheme != "postgresql":
			raise ValueError(
				"Unsupported PostgreSQL DSN. Use postgresql://user:pass@host:5432/database or jdbc:postgresql://..."
			)

		db_user = username or parsed.username
		db_password = password if password is not None else (parsed.password or "")
		db_name = parsed.path.lstrip("/") if parsed.path else ""

		if not parsed.hostname or not db_user or not db_name:
			raise ValueError(
				"Incomplete PostgreSQL connection information. Required: host, username, database."
			)

		return {
			"host": parsed.hostname,
			"port": parsed.port or 5432,
			"user": db_user,
			"password": db_password,
			"dbname": db_name,
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
		allow_fixture_fallback = cls._allow_fixture_fallback()
		if not normalized_dsn:
			return cls._load_fixture_or_raise_missing_dsn(
				allow_fixture_fallback,
				mapping_dir,
				query,
				table_name,
			)

		sql = cls._resolve_sql_query(query, table_name)
		psycopg2, extras = cls._import_psycopg2()
		conn_kwargs = cls._build_connection_kwargs(normalized_dsn, username=username, password=password)

		try:
			rows = cls._fetch_rows(psycopg2, extras, conn_kwargs, sql)
		except Exception:
			if allow_fixture_fallback and mapping_dir is not None:
				return load_rows_from_sql_fixture(mapping_dir=mapping_dir, query=query, table_name=table_name)
			raise

		return normalize_db_rows([dict(r) for r in rows])

	@staticmethod
	def _allow_fixture_fallback() -> bool:
		"""
		Checks whether local SQL fixture fallback is enabled.
		:return: True when the SQLite fixture fallback can be used.
		"""
		return os.getenv("FOG_RML_DB_SQLITE_FALLBACK", "1") != "0"

	@staticmethod
	def _load_fixture_or_raise_missing_dsn(
		allow_fixture_fallback: bool,
		mapping_dir: Optional[Path],
		query: Optional[str],
		table_name: Optional[str],
	) -> list:
		"""
		Loads fallback fixture rows when no DSN is configured, otherwise raises a DSN error.
		:param allow_fixture_fallback: Whether fixture fallback is enabled.
		:param mapping_dir: Optional mapping directory used for SQL fixture fallback.
		:param query: Optional SQL query.
		:param table_name: Optional table name.
		:return: Rows loaded from the SQL fixture fallback.
		"""
		if allow_fixture_fallback and mapping_dir is not None:
			return load_rows_from_sql_fixture(mapping_dir=mapping_dir, query=query, table_name=table_name)
		raise ValueError("PostgreSQL DSN is missing. Set rml:source DSN or environment variable POSTGRES_DSN.")

	@staticmethod
	def _resolve_sql_query(query: Optional[str], table_name: Optional[str]) -> str:
		"""
		Resolves the SQL query to execute for the logical source.
		:param query: Optional SQL query.
		:param table_name: Optional table name used when no explicit query is provided.
		:return: SQL query string.
		"""
		sql = (query or "").strip()
		if sql:
			return sql
		if table_name:
			return f"SELECT * FROM {table_name}"
		raise ValueError("PostgreSQL logical source requires rml:query or rr:tableName.")

	@staticmethod
	def _import_psycopg2():
		"""
		Imports the `psycopg2` modules required for PostgreSQL access.
		:return: Imported `psycopg2` and `psycopg2.extras` modules.
		"""
		try:
			psycopg2 = importlib.import_module("psycopg2")
			extras = importlib.import_module("psycopg2.extras")
			return psycopg2, extras
		except Exception as exc:
			raise ModuleNotFoundError(
				"psycopg2 is required for PostgresqlSourceOperator. Install it with 'pip install psycopg2-binary'."
			) from exc

	@staticmethod
	def _fetch_rows(psycopg2, extras, conn_kwargs: Dict[str, Any], sql: str) -> list:
		"""
		Executes a SQL query through psycopg2 and returns row dictionaries.
		:param psycopg2: Imported `psycopg2` module.
		:param extras: Imported `psycopg2.extras` module.
		:param conn_kwargs: psycopg2 connection keyword arguments.
		:param sql: SQL query string.
		:return: List of row dictionaries.
		"""
		connection = psycopg2.connect(**conn_kwargs)
		try:
			with connection.cursor(cursor_factory=extras.RealDictCursor) as cursor:
				cursor.execute(sql)
				return cursor.fetchall() or []
		finally:
			connection.close()

	def explain_json(self) -> Dict[str, Any]:
		return super().explain_json()
