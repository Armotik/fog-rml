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

        # Some mappings use JSONPath-like queries (e.g. '$.ID') even for CSV;
        # normalize by stripping a leading '$.' so column lookups succeed.
        # Also support bracket notation like $['Column Name'] or $("Column Name")
        q = query
        if q.startswith("$."):
            q = q[2:]
        elif q.startswith("$") and len(q) > 1 and q[1] == '.':
            q = q[2:]
        else:
            # handle bracket notation $['col'] or $("col")
            if q.startswith("$["):
                # find the closing ]
                try:
                    end = q.index(']')
                    inner = q[2:end]
                    # strip surrounding quotes if present
                    if (inner.startswith("'") and inner.endswith("'")) or (inner.startswith('"') and inner.endswith('"')):
                        inner = inner[1:-1]
                    q = inner
                except Exception:
                    # leave q as-is if parsing fails
                    q = query

        def _lookup_key(d: Dict[str, Any], key: str):
            if key in d:
                return d[key]
            low = key.lower()
            for dk, dv in d.items():
                if isinstance(dk, str) and dk.lower() == low:
                    return dv
            raise KeyError(key)

        # If query exactly matches a column (or case-insensitive equivalent), return that value
        try:
            val = _lookup_key(context, q)
            return [val] if val is not None else []
        except Exception:
            pass

        # Support dotted keys like 'user.name'
        parts = query.split('.')
        cur = context
        for p in parts:
            if isinstance(cur, dict):
                try:
                    cur = _lookup_key(cur, p)
                    continue
                except Exception:
                    return []
            if not isinstance(cur, dict):
                return []

        return [cur]

    def explain_json(self) -> Dict[str, Any]:
        base = super().explain_json()
        base['parameters']['source_type'] = 'CSV'
        return base
