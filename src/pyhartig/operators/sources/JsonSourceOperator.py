import json
from typing import Any, List, Dict, TypeAlias
from pathlib import Path
from jsonpath_ng import parse

from pyhartig.operators.SourceOperator import SourceOperator

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


class JsonSourceOperator(SourceOperator):

    def __init__(self, source_data: JsonValue | str | Path, iterator_query: str, attribute_mappings: Dict[str, str]):
        prepared_source = self._prepare_source_data(source_data)
        super().__init__(prepared_source, iterator_query, attribute_mappings)

        # Keep a sanitized JSON payload only; invalid inputs degrade to None.
        self._data: JsonValue | None = prepared_source

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
                # Try to parse the provided query as-is. Many mappings use
                # bare attribute names (e.g. "ID") which are valid as
                # relative JSONPath expressions, but to be robust also
                # accept plain names by compiling a `$.name` fallback.
                self._compiled_attribute_exprs[q] = parse(q)
            except Exception:
                # Defer parsing until first use
                self._compiled_attribute_exprs[q] = None

    @classmethod
    def from_json_file(cls, source_path: str | Path, iterator_query: str,
                       attribute_mappings: Dict[str, str]) -> "JsonSourceOperator":
        """
        Build an operator from a validated local JSON file.
        """
        sanitized_data = cls._load_json_file(source_path)
        return cls(sanitized_data, iterator_query, attribute_mappings)

    @classmethod
    def _prepare_source_data(cls, source_data: JsonValue | str | Path) -> JsonValue | None:
        try:
            if isinstance(source_data, (str, Path)):
                return cls._load_json_file(source_data)
            return cls._sanitize_json_value(source_data)
        except Exception:
            return None

    @staticmethod
    def _resolve_json_path(source_path: str | Path) -> Path:
        resolved_path = Path(source_path).expanduser().resolve(strict=True)
        if not resolved_path.is_file():
            raise ValueError(f"JSON source is not a file: {resolved_path}")
        return resolved_path

    @classmethod
    def _load_json_file(cls, source_path: str | Path) -> JsonValue:
        json_path = cls._resolve_json_path(source_path)
        with json_path.open("r", encoding="utf-8") as f:
            loaded_data = json.load(f)
        return cls._sanitize_json_value(loaded_data)

    @classmethod
    def _sanitize_json_value(cls, value: Any) -> JsonValue:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value

        if isinstance(value, list):
            return [cls._sanitize_json_value(item) for item in value]

        if isinstance(value, dict):
            sanitized_object: dict[str, JsonValue] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise ValueError(f"JSON object keys must be strings, received {type(key)!r}")
                sanitized_object[key] = cls._sanitize_json_value(item)
            return sanitized_object

        raise ValueError(f"Unsupported JSON payload type: {type(value)!r}")

    def _apply_iterator(self, data: Any, query: str) -> List[Any]:
        """
        Apply the iterator query on the data source (function eval(D, q))
        :param data: JSON data source
        :param query: Iterator query
        :return: List of context
        """
        jsonpath_expr = self._compiled_iterator
        if jsonpath_expr is None:
            try:
                jsonpath_expr = parse(query)
                self._compiled_iterator = jsonpath_expr
            except Exception:
                return []

        source = self._data if self._data is not None else data
        matches = [match.value for match in jsonpath_expr.find(source)]
        # If the iterator expression returned a single list (root array),
        # treat its elements as the iteration contexts (common case when
        # rml:iterator is '$' and the JSON source is an array).
        if len(matches) == 1 and isinstance(matches[0], list):
            return matches[0]
        return matches

    def _apply_extraction(self, context: Any, query: str) -> List[Any]:
        """
        Apply the extraction query on a context object (function eval'(D, d, q'))
        :param context: Context object
        :param query: Extraction query
        :return: List of extracted values for the attribute
        """
        jsonpath_expr = self._compiled_attribute_exprs.get(query)
        if jsonpath_expr is None:
            # Compile and cache on first use. Be tolerant: if the provided
            # query is a bare attribute name (which causes jsonpath_ng.parse
            # to raise), try compiling a `$.<name>` alternative before
            # letting an exception propagate.
            try:
                jsonpath_expr = parse(query)
            except Exception:
                if not query.startswith('$'):
                    alt_q = f'$.{query}'
                    try:
                        jsonpath_expr = parse(alt_q)
                        # cache the alternative under the original query key
                        self._compiled_attribute_exprs[query] = jsonpath_expr
                    except Exception:
                        # leave as None so caller handles missing matches
                        jsonpath_expr = None
                else:
                    jsonpath_expr = None
            else:
                self._compiled_attribute_exprs[query] = jsonpath_expr

        # If we couldn't compile an expression, treat as no matches and
        # fall through to the alternative-fallback below.
        if jsonpath_expr is None:
            matches = []
        else:
            matches = jsonpath_expr.find(context)

        # If no matches found, return empty list
        if not matches:
            # Fallback: if we didn't get any matches, and the original query
            # isn't a JSONPath expression, try `$.<name>` as a last resort.
            if not query.startswith('$'):
                try:
                    alt_q = f'$.{query}'
                    alt_expr = parse(alt_q)
                    # cache the alternative compiled expression for future use
                    self._compiled_attribute_exprs[query] = alt_expr
                    matches = alt_expr.find(context)
                except Exception:
                    pass
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
