from typing import Any, List, Dict
from pathlib import Path

from pyhartig.operators.SourceOperator import SourceOperator


class CsvSourceOperator(SourceOperator):

    def __init__(self, source_data: Any, iterator_query: str, attribute_mappings: Dict[str, str]):
        super().__init__(source_data, iterator_query, attribute_mappings)

        # source_data is expected to be a list of dicts (rows) or a path
        self._rows = None
        try:
            if isinstance(source_data, (str, Path)):
                import csv
                p = Path(source_data)
                with p.open('r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    self._rows = list(reader)
            else:
                self._rows = list(source_data) if source_data is not None else []
        except Exception:
            self._rows = []

    def _apply_iterator(self, data: Any, query: str) -> List[Any]:
        # For CSV, iterator selects all rows; query is ignored
        return self._rows if self._rows is not None else []

    def _apply_extraction(self, context: Any, query: str) -> List[Any]:
        # Query is treated as column name; support nested keys using dot notation
        if context is None:
            return []

        if not query:
            return []

        # If query exactly matches a column, return that value
        if query in context:
            val = context.get(query)
            return [val] if val is not None else []

        # Support dotted keys like 'user.name'
        parts = query.split('.')
        cur = context
        for p in parts:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                return []

        return [cur]

    def explain_json(self) -> Dict[str, Any]:
        base = super().explain_json()
        base['parameters']['source_type'] = 'CSV'
        return base
