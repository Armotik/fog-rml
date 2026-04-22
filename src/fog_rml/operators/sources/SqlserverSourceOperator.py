import os
import importlib
from typing import Dict, Any, Optional
from urllib.parse import urlparse
from pathlib import Path

from fog_rml.operators.sources.CsvSourceOperator import CsvSourceOperator
from fog_rml.operators.sources.sql_fixture_fallback import load_rows_from_sql_fixture, normalize_db_rows


class SqlserverSourceOperator(CsvSourceOperator):
	"""
	Source operator specialized for SQL Server logical sources.
	"""

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
		"""
		Initializes the SQL Server source operator.
		:param dsn: SQL Server DSN or connection string.
		:param iterator_query: Iterator query used by the source operator interface.
		:param attribute_mappings: Mapping of output attributes to extraction queries.
		:param query: Optional SQL query.
		:param table_name: Optional table name used when no explicit query is provided.
		:param username: Optional SQL Server username.
		:param password: Optional SQL Server password.
		:param mapping_dir: Optional mapping directory used for SQL fixture fallback.
		:return: None
		"""
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
		"""
		Normalizes a SQL Server DSN or JDBC URL to a connection string-like value.
		:param dsn: Raw DSN value.
		:return: Normalized DSN string.
		"""
		dsn_str = (dsn or "").strip()
		if not dsn_str:
			return ""

		if dsn_str == "CONNECTIONDSN":
			env_dsn = os.getenv("FOG_RML_SQLSERVER_DSN") or os.getenv("FOG_RML_SQLSERVER_DSN") or os.getenv("SQLSERVER_DSN")
			return (env_dsn or "").strip()

		if dsn_str.startswith("jdbc:sqlserver://"):
			raw = dsn_str[len("jdbc:sqlserver://"):]
			host_port, *params = raw.split(';')
			host = host_port
			database = None
			for p in params:
				if p.lower().startswith("databasename="):
					database = p.split('=', 1)[1]
					break
			driver = os.getenv("FOG_RML_SQLSERVER_DRIVER") or os.getenv("FOG_RML_SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
			conn = f"DRIVER={{{driver}}};SERVER={host};"
			if database:
				conn += f"DATABASE={database};"
			return conn

		return dsn_str

	@staticmethod
	def _build_connection_string(dsn: str, username: Optional[str], password: Optional[str]) -> str:
		"""
		Builds a pyodbc-compatible SQL Server connection string.
		:param dsn: Normalized SQL Server DSN.
		:param username: Optional SQL Server username.
		:param password: Optional SQL Server password.
		:return: SQL Server connection string.
		"""
		dsn_str = dsn.strip()
		if "=" in dsn_str and ";" in dsn_str:
			return SqlserverSourceOperator._augment_connection_string(dsn_str, username, password)
		return SqlserverSourceOperator._build_url_connection_string(dsn_str, username, password)

	@staticmethod
	def _augment_connection_string(dsn: str, username: Optional[str], password: Optional[str]) -> str:
		"""
		Adds credentials to an existing SQL Server connection string when missing.
		:param dsn: Existing SQL Server connection string.
		:param username: Optional SQL Server username.
		:param password: Optional SQL Server password.
		:return: Augmented SQL Server connection string.
		"""
		connection_string = dsn
		if username and "UID=" not in connection_string.upper():
			connection_string += f"UID={username};"
		if password is not None and "PWD=" not in connection_string.upper():
			connection_string += f"PWD={password};"
		return connection_string

	@staticmethod
	def _build_url_connection_string(dsn: str, username: Optional[str], password: Optional[str]) -> str:
		"""
		Builds a SQL Server connection string from a `sqlserver://` URL.
		:param dsn: SQL Server URL.
		:param username: Optional SQL Server username override.
		:param password: Optional SQL Server password override.
		:return: SQL Server connection string.
		"""
		parsed = urlparse(dsn)
		if parsed.scheme != "sqlserver":
			raise ValueError(
				"Unsupported SQLServer DSN. Use sqlserver://user:pass@host:1433/database, JDBC SQLServer DSN, or ODBC connection string."
			)

		host = parsed.hostname
		port = parsed.port or 1433
		database = parsed.path.lstrip("/") if parsed.path else ""
		user = username or parsed.username
		pwd = password if password is not None else (parsed.password or "")
		if not host or not database:
			raise ValueError("Incomplete SQLServer connection information. Required: host and database.")

		driver = os.getenv("FOG_RML_SQLSERVER_DRIVER") or os.getenv("FOG_RML_SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
		connection_string = f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};"
		if user:
			return f"{connection_string}UID={user};PWD={pwd};"
		return f"{connection_string}Trusted_Connection=yes;"

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
		"""
		Loads rows from SQL Server or from a local SQL fixture fallback.
		:param dsn: SQL Server DSN or connection string.
		:param query: Optional SQL query.
		:param table_name: Optional table name used when no explicit query is provided.
		:param username: Optional SQL Server username.
		:param password: Optional SQL Server password.
		:param mapping_dir: Optional mapping directory used for SQL fixture fallback.
		:return: List of normalized database rows.
		"""
		normalized_dsn = cls._normalize_dsn(dsn)
		allow_fixture_fallback = (os.getenv("FOG_RML_DB_SQLITE_FALLBACK") or os.getenv("FOG_RML_DB_SQLITE_FALLBACK", "1")) != "0"
		if not normalized_dsn:
			if allow_fixture_fallback and mapping_dir is not None:
				return load_rows_from_sql_fixture(mapping_dir=mapping_dir, query=query, table_name=table_name)
			raise ValueError("SQLServer DSN is missing. Set rml:source DSN or environment variable SQLSERVER_DSN.")

		sql = cls._resolve_sql_query(query, table_name)
		pyodbc = cls._import_pyodbc()
		connection_string = cls._build_connection_string(normalized_dsn, username=username, password=password)

		try:
			rows = cls._fetch_rows(pyodbc, connection_string, sql)
		except Exception:
			if allow_fixture_fallback and mapping_dir is not None:
				return load_rows_from_sql_fixture(mapping_dir=mapping_dir, query=query, table_name=table_name)
			raise

		return normalize_db_rows(rows)

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
		raise ValueError("SQLServer logical source requires rml:query or rr:tableName.")

	@staticmethod
	def _import_pyodbc():
		"""
		Imports the `pyodbc` module required for SQL Server access.
		:return: Imported `pyodbc` module.
		"""
		try:
			return importlib.import_module("pyodbc")
		except Exception as exc:
			raise ModuleNotFoundError(
				"pyodbc is required for SqlserverSourceOperator. Install it with 'pip install pyodbc'."
			) from exc

	@staticmethod
	def _fetch_rows(pyodbc, connection_string: str, sql: str) -> list[Dict[str, Any]]:
		"""
		Executes a SQL query through pyodbc and returns row dictionaries.
		:param pyodbc: Imported `pyodbc` module.
		:param connection_string: SQL Server connection string.
		:param sql: SQL query string.
		:return: List of row dictionaries.
		"""
		connection = pyodbc.connect(connection_string)
		try:
			cursor = connection.cursor()
			try:
				cursor.execute(sql)
				columns = [c[0] for c in cursor.description] if cursor.description else []
				rows = cursor.fetchall() or []
			finally:
				cursor.close()
		finally:
			connection.close()
		return [dict(zip(columns, row)) for row in rows]

	def explain_json(self) -> Dict[str, Any]:
		"""
		Generate a JSON-serializable explanation of the SQL Server source operator.
		:return: Dictionary representing the operator tree structure.
		"""
		return super().explain_json()

