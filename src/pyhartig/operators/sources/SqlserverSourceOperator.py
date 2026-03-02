import os
import importlib
from typing import Dict, Any, Optional
from urllib.parse import urlparse
from pathlib import Path

from pyhartig.operators.sources.CsvSourceOperator import CsvSourceOperator
from pyhartig.operators.sources.sql_fixture_fallback import load_rows_from_sql_fixture, normalize_db_rows


class SqlserverSourceOperator(CsvSourceOperator):
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
			env_dsn = os.getenv("PYHARTIG_SQLSERVER_DSN") or os.getenv("SQLSERVER_DSN")
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
			driver = os.getenv("PYHARTIG_SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
			conn = f"DRIVER={{{driver}}};SERVER={host};"
			if database:
				conn += f"DATABASE={database};"
			return conn

		return dsn_str

	@staticmethod
	def _build_connection_string(dsn: str, username: Optional[str], password: Optional[str]) -> str:
		dsn_str = dsn.strip()
		if "=" in dsn_str and ";" in dsn_str:
			conn = dsn_str
			if username and "UID=" not in conn.upper():
				conn += f"UID={username};"
			if password is not None and "PWD=" not in conn.upper():
				conn += f"PWD={password};"
			return conn

		parsed = urlparse(dsn_str)
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

		driver = os.getenv("PYHARTIG_SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
		conn = f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};"
		if user:
			conn += f"UID={user};PWD={pwd};"
		else:
			conn += "Trusted_Connection=yes;"
		return conn

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
			raise ValueError("SQLServer DSN is missing. Set rml:source DSN or environment variable SQLSERVER_DSN.")

		sql = (query or "").strip()
		if not sql:
			if table_name:
				sql = f"SELECT * FROM {table_name}"
			else:
				raise ValueError("SQLServer logical source requires rml:query or rr:tableName.")

		try:
			pyodbc = importlib.import_module("pyodbc")
		except Exception as exc:
			raise ModuleNotFoundError(
				"pyodbc is required for SqlserverSourceOperator. Install it with 'pip install pyodbc'."
			) from exc

		connection_string = cls._build_connection_string(normalized_dsn, username=username, password=password)

		try:
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
		except Exception:
			if allow_fixture_fallback and mapping_dir is not None:
				return load_rows_from_sql_fixture(mapping_dir=mapping_dir, query=query, table_name=table_name)
			raise

		return normalize_db_rows([dict(zip(columns, row)) for row in rows])

	def explain_json(self) -> Dict[str, Any]:
		base = super().explain_json()
		base["parameters"]["source_type"] = "SQLSERVER"
		base["parameters"]["dsn"] = self._dsn
		base["parameters"]["query"] = self._query
		base["parameters"]["table_name"] = self._table_name
		return base
