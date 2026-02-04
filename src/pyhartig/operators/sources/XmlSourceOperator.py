from typing import Any, List, Dict
from pathlib import Path

from pyhartig.operators.SourceOperator import SourceOperator


class XmlSourceOperator(SourceOperator):

    def __init__(self, source_data: Any, iterator_query: str, attribute_mappings: Dict[str, str]):
        super().__init__(source_data, iterator_query, attribute_mappings)

        # source_data is expected to be an ElementTree Element (root) or a path
        self._root = None
        try:
            if isinstance(source_data, (str, Path)):
                import xml.etree.ElementTree as ET
                p = Path(source_data)
                tree = ET.parse(p)
                self._root = tree.getroot()
            else:
                self._root = source_data
        except Exception:
            self._root = None

    def _apply_iterator(self, data: Any, query: str) -> List[Any]:
        # Use simple XPath supported by ElementTree
        if self._root is None:
            return []

        try:
            elems = self._root.findall(query)
            return elems
        except Exception:
            # Fallback: return root children
            return list(self._root)

    def _apply_extraction(self, context: Any, query: str) -> List[Any]:
        # If query starts with '@' treat as attribute, else tag or subpath
        if context is None:
            return []

        try:
            if query.startswith('@'):
                attr = query[1:]
                val = context.get(attr)
                return [val] if val is not None else []
            else:
                elems = context.findall(query)
                results = []
                for e in elems:
                    if e.text:
                        results.append(e.text)
                return results
        except Exception:
            return []

    def explain_json(self) -> Dict[str, Any]:
        base = super().explain_json()
        base['parameters']['source_type'] = 'XML'
        return base
