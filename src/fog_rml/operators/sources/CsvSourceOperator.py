import csv
from pathlib import Path
from typing import Any, Dict, Iterator, List

from fog_rml.operators.SourceOperator import SourceOperator


class CsvSourceOperator(SourceOperator):
    """
    Source operator specialized for CSV rows.
    """

    def __init__(self, source_data: Any, iterator_query: str, attribute_mappings: Dict[str, str]):
        """
        Initializes the CSV source operator.
        :param source_data: CSV rows or a path to a CSV file.
        :param iterator_query: Iterator query used by the source operator interface.
        :param attribute_mappings: Mapping of output attributes to CSV extraction queries.
        :return: None
        """
        super().__init__(source_data, iterator_query, attribute_mappings)

        # Keep iterables lazy. File-backed sources are opened only when execution starts.
        self._rows = None
        self._source_path = None
        try:
            if isinstance(source_data, (str, Path)):
                self._source_path = Path(source_data)
            elif source_data is not None:
                iter(source_data)
                self._rows = source_data
            else:
                self._rows = []
        except Exception:
            self._rows = []

    def _apply_iterator(self, data: Any, query: str) -> Iterator[Any]:
        """
        Returns the CSV rows selected by the iterator.
        :param data: CSV data source.
        :param query: Iterator query.
        :return: Iterator of CSV row contexts.
        """
        if self._source_path is not None:
            return self._iter_rows_from_path()
        if self._rows is None:
            return iter(())
        return iter(self._rows)

    def _iter_rows_from_path(self) -> Iterator[Dict[str, Any]]:
        """
        Lazily reads CSV rows from the configured file path.
        :return: Iterator of CSV row dictionaries.
        """
        if self._source_path is None:
            return iter(())

        def _gen():
            with self._source_path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield row

        return _gen()

    def _apply_extraction(self, context: Any, query: str) -> List[Any]:
        """
        Extracts a CSV cell value from the current row context.
        :param context: CSV row context.
        :param query: Extraction query interpreted as a column name.
        :return: List of extracted values.
        """
        if context is None:
            return []

        normalized_query = self._normalize_extraction_query(query)
        if not normalized_query:
            return []

        try:
            value = self._lookup_key(context, normalized_query)
            return [value] if value is not None else []
        except Exception:
            pass

        return self._resolve_dotted_query(context, normalized_query)

    @staticmethod
    def _normalize_extraction_query(query: str) -> str:
        """
        Normalizes a CSV extraction query to a direct column lookup key.
        :param query: Raw extraction query.
        :return: Normalized column lookup key.
        """
        if not query:
            return ""
        if query.startswith("$."):
            return query[2:]
        if query.startswith("$") and len(query) > 1 and query[1] == ".":
            return query[2:]
        if not query.startswith("$["):
            return query

        try:
            end = query.index("]")
        except ValueError:
            return query

        inner = query[2:end]
        if (inner.startswith("'") and inner.endswith("'")) or (inner.startswith('"') and inner.endswith('"')):
            return inner[1:-1]
        return inner

    @staticmethod
    def _lookup_key(row_context: Dict[str, Any], key: str):
        """
        Looks up a CSV column by exact name or case-insensitive fallback.
        :param row_context: CSV row context.
        :param key: Column name to resolve.
        :return: Matching cell value.
        """
        if key in row_context:
            return row_context[key]

        lower_key = key.lower()
        for row_key, value in row_context.items():
            if isinstance(row_key, str) and row_key.lower() == lower_key:
                return value
        raise KeyError(key)

    def _resolve_dotted_query(self, context: Any, query: str) -> List[Any]:
        """
        Resolves a dotted extraction query through nested mapping objects.
        :param context: Current row context.
        :param query: Dotted extraction query.
        :return: List containing the resolved value, or an empty list.
        """
        current = context
        for part in query.split("."):
            if not isinstance(current, dict):
                return []
            try:
                current = self._lookup_key(current, part)
            except Exception:
                return []
        return [current]

    def explain_json(self) -> Dict[str, Any]:
        """
        Generate a JSON-serializable explanation of the CSV source operator.
        :return: Dictionary representing the operator tree structure.
        """
        return super().explain_json()
