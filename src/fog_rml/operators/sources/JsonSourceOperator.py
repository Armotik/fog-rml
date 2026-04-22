import json
from typing import Any, Dict, Iterator, List, TypeAlias
from pathlib import Path
from jsonpath_ng import parse

from fog_rml.operators.SourceOperator import SourceOperator

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


class JsonSourceOperator(SourceOperator):
    """
    Source operator specialized for JSON documents queried through JSONPath.
    """

    def __init__(self, source_data: JsonValue | str | Path, iterator_query: str, attribute_mappings: Dict[str, str]):
        """
        Initializes the JSON source operator.
        :param source_data: JSON payload or path to a JSON file.
        :param iterator_query: JSONPath iterator query.
        :param attribute_mappings: Mapping of output attributes to JSONPath extraction queries.
        :return: None
        """
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
        :param source_path: Path to the JSON file.
        :param iterator_query: JSONPath iterator query.
        :param attribute_mappings: Mapping of output attributes to JSONPath extraction queries.
        :return: JsonSourceOperator built from the file.
        """
        sanitized_data = cls._load_json_file(source_path)
        return cls(sanitized_data, iterator_query, attribute_mappings)

    @classmethod
    def _prepare_source_data(cls, source_data: JsonValue | str | Path) -> JsonValue | None:
        """
        Validates and sanitizes the JSON source payload.
        :param source_data: JSON payload or path to a JSON file.
        :return: Sanitized JSON payload, or None on failure.
        """
        try:
            if isinstance(source_data, (str, Path)):
                return cls._load_json_file(source_data)
            return cls._sanitize_json_value(source_data)
        except Exception:
            return None

    @staticmethod
    def _resolve_json_path(source_path: str | Path) -> Path:
        """
        Resolves a JSON file path to a validated local file.
        :param source_path: JSON source path.
        :return: Resolved JSON file path.
        """
        resolved_path = Path(source_path).expanduser().resolve(strict=True)
        if not resolved_path.is_file():
            raise ValueError(f"JSON source is not a file: {resolved_path}")
        return resolved_path

    @classmethod
    def _load_json_file(cls, source_path: str | Path) -> JsonValue:
        """
        Loads and sanitizes a JSON file.
        :param source_path: JSON source path.
        :return: Sanitized JSON payload.
        """
        json_path = cls._resolve_json_path(source_path)
        with json_path.open("r", encoding="utf-8") as f:
            loaded_data = json.load(f)
        return cls._sanitize_json_value(loaded_data)

    @classmethod
    def _sanitize_json_value(cls, value: Any) -> JsonValue:
        """
        Recursively sanitizes a JSON value to supported JSON scalar/container types.
        :param value: JSON value to sanitize.
        :return: Sanitized JSON value.
        """
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

    def _apply_iterator(self, data: Any, query: str) -> Iterator[Any]:
        """
        Apply the iterator query on the data source (function eval(D, q)).
        :param data: JSON data source.
        :param query: Iterator query.
        :return: Iterator of context objects.
        """
        jsonpath_expr = self._compiled_iterator
        if jsonpath_expr is None:
            try:
                jsonpath_expr = parse(query)
                self._compiled_iterator = jsonpath_expr
            except Exception:
                return iter(())

        source = self._data if self._data is not None else data
        if source is None:
            return iter(())

        def _gen():
            try:
                matches = iter(jsonpath_expr.find(source))
                try:
                    first = next(matches)
                except StopIteration:
                    return

                try:
                    second = next(matches)
                except StopIteration:
                    if isinstance(first.value, list):
                        yield from first.value
                    else:
                        yield first.value
                    return

                yield first.value
                yield second.value
                for match in matches:
                    yield match.value
            except Exception:
                return

        return _gen()

    def _apply_extraction(self, context: Any, query: str) -> List[Any]:
        """
        Apply the extraction query on a context object (function eval'(D, d, q')).
        :param context: Context object.
        :param query: Extraction query.
        :return: List of extracted values for the attribute.
        """
        matches = self._find_attribute_matches(context, query)
        if not matches:
            return []
        return self._flatten_matches(matches)

    def _find_attribute_matches(self, context: Any, query: str):
        """
        Finds JSONPath matches for an attribute query, including tolerant fallback compilation.
        :param context: JSON context object.
        :param query: Attribute extraction query.
        :return: JSONPath matches for the query.
        """
        jsonpath_expr = self._get_compiled_attribute_expr(query)
        matches = [] if jsonpath_expr is None else jsonpath_expr.find(context)
        if matches or query.startswith("$"):
            return matches

        alternative_expr = self._compile_attribute_expr(f"$.{query}")
        if alternative_expr is None:
            return matches

        self._compiled_attribute_exprs[query] = alternative_expr
        return alternative_expr.find(context)

    def _get_compiled_attribute_expr(self, query: str):
        """
        Returns a cached compiled JSONPath expression for an attribute query.
        :param query: Attribute extraction query.
        :return: Compiled JSONPath expression, or None.
        """
        jsonpath_expr = self._compiled_attribute_exprs.get(query)
        if jsonpath_expr is not None:
            return jsonpath_expr

        jsonpath_expr = self._compile_attribute_expr(query)
        self._compiled_attribute_exprs[query] = jsonpath_expr
        return jsonpath_expr

    @staticmethod
    def _compile_attribute_expr(query: str):
        """
        Compiles a JSONPath expression, returning None on failure.
        :param query: Attribute extraction query.
        :return: Compiled JSONPath expression, or None.
        """
        try:
            return parse(query)
        except Exception:
            return None

    @staticmethod
    def _flatten_matches(matches) -> List[Any]:
        """
        Flattens JSONPath matches to a plain list of extracted values.
        :param matches: JSONPath matches.
        :return: Flattened extracted values.
        """
        results: List[Any] = []
        for match in matches:
            if isinstance(match.value, list):
                results.extend(match.value)
            else:
                results.append(match.value)
        return results

    def explain_json(self) -> Dict[str, Any]:
        """
        Generate a JSON-serializable explanation of the JsonSource operator.
        :return: Dictionary representing the operator tree structure.
        """
        return super().explain_json()

