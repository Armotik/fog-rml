from typing import Any, List, Dict
from pathlib import Path
from jsonpath_ng import parse

from pyhartig.operators.SourceOperator import SourceOperator


class JsonSourceOperator(SourceOperator):

    def __init__(self, source_data: Any, iterator_query: str, attribute_mappings: Dict[str, str]):
        super().__init__(source_data, iterator_query, attribute_mappings)

        # Accept either a parsed JSON structure or a path to a JSON file/string
        self._data = None
        try:
            if isinstance(source_data, (str, Path)):
                p = Path(source_data)
                with p.open("r", encoding="utf-8") as f:
                    import json

                    self._data = json.load(f)
            else:
                # assume it's already parsed JSON-like
                self._data = source_data
        except Exception:
            # keep None and let _apply_iterator raise/handle when used
            self._data = source_data

        # Pre-compile JSONPath expressions to avoid reparsing the same query many times.
        # Keep a compiled iterator expression and a cache for attribute extraction expressions.
        try:
            self._compiled_iterator = parse(self.iterator_query)
        except Exception:
            # Fallback: store None and parse lazily in case of errors
            self._compiled_iterator = None

        # Map query string -> compiled expression (shared across attributes if identical)
        self._compiled_attribute_exprs: Dict[str, Any] = {}
        for q in set(self.attribute_mappings.values()):
            try:
                self._compiled_attribute_exprs[q] = parse(q)
            except Exception:
                # Defer parsing until first use
                self._compiled_attribute_exprs[q] = None

    def _apply_iterator(self, data: Any, query: str) -> List[Any]:
        """
        Apply the iterator query on the data source (function eval(D, q))
        :param data: JSON data source
        :param query: Iterator query
        :return: List of context
        """
        jsonpath_expr = self._compiled_iterator
        if jsonpath_expr is None:
            jsonpath_expr = parse(query)
            self._compiled_iterator = jsonpath_expr

        source = self._data if self._data is not None else data
        return [match.value for match in jsonpath_expr.find(source)]

    def _apply_extraction(self, context: Any, query: str) -> List[Any]:
        """
        Apply the extraction query on a context object (function eval'(D, d, q'))
        :param context: Context object
        :param query: Extraction query
        :return: List of extracted values for the attribute
        """
        jsonpath_expr = self._compiled_attribute_exprs.get(query)
        if jsonpath_expr is None:
            # Compile and cache on first use
            jsonpath_expr = parse(query)
            self._compiled_attribute_exprs[query] = jsonpath_expr

        matches = jsonpath_expr.find(context)

        # If no matches found, return empty list
        if not matches:
            return []

        # Flatten the results
        results: List[Any] = []
        for match in matches:
            # If the match value is a list, extend the results; otherwise, append the single value
            if isinstance(match.value, list):
                results.extend(match.value)
            else:
                results.append(match.value)
        return results

    def explain_json(self) -> Dict[str, Any]:
        """
        Generate a JSON-serializable explanation of the JsonSource operator
        :return: Dictionary representing the operator tree structure
        """
        base = super().explain_json()
        base["parameters"]["source_type"] = "JSON"
        base["parameters"]["jsonpath_iterator"] = self.iterator_query
        return base
