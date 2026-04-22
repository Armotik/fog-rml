from pathlib import Path
from typing import Any, Dict, Iterator, List

from fog_rml.operators.SourceOperator import SourceOperator


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

    def _apply_iterator(self, data: Any, query: str) -> Iterator[Any]:
        # Use simple XPath supported by ElementTree.
        if self._root is None:
            return iter(())

        try:
            def _first_or_none(iterator):
                try:
                    first = next(iterator)
                except StopIteration:
                    return None

                def _gen():
                    yield first
                    yield from iterator

                return _gen()

            # ElementTree.find* does not accept leading '/' for absolute paths.
            # Try several fallbacks to resolve common absolute XPath forms used
            # in RML test-cases (e.g. '/countries/country').
            q = query if isinstance(query, str) else ""

            # 1) try as-is without leading slash
            if q.startswith('/'):
                q1 = q.lstrip('/')
                elems = _first_or_none(self._root.iterfind(q1))
                if elems is not None:
                    return elems

                # 2) try descendant search (find anywhere under root)
                q2 = './/' + q1
                elems = _first_or_none(self._root.iterfind(q2))
                if elems is not None:
                    return elems

                # 3) fallback: try last path segment (e.g., 'country')
                last = q1.split('/')[-1]
                elems = _first_or_none(self._root.iterfind('.//' + last))
                if elems is not None:
                    return elems
            else:
                elems = _first_or_none(self._root.iterfind(q))
                if elems is not None:
                    return elems

            # As a final fallback, return empty iterator so caller can handle it.
            return iter(())
        except Exception:
            # Fallback: return root children.
            return iter(self._root)

    def _apply_extraction(self, context: Any, query: str) -> List[Any]:
        # If query starts with '@' treat as attribute, else tag or subpath
        if context is None:
            return []

        try:
            q = query
            # Accept JSONPath-style references produced by the mapping extractor
            # (e.g. '$.Name') and normalize them to the XML tag name 'Name'
            if isinstance(q, str) and q.startswith('$.'):
                q = q[2:]

            if q.startswith('@'):
                attr = q[1:]
                val = context.get(attr)
                return [val] if val is not None else []
            else:
                elems = context.findall(q)
                results = []
                for e in elems:
                    if e.text:
                        results.append(e.text)
                return results
        except Exception:
            return []

    def explain_json(self) -> Dict[str, Any]:
        return super().explain_json()
