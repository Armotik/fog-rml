from abc import abstractmethod
from typing import Any, Dict, Iterable, Iterator
from itertools import product

from fog_rml.algebra.Tuple import MappingTuple, EPSILON
from fog_rml.operators.Operator import Operator


class SourceOperator(Operator):
    """
    Abstract class defining the algebraic logic of the Source operator
    """

    def __init__(self, source_data: Any, iterator_query: str, attribute_mappings: Dict[str, str]):
        """
        Initializes the source operator.
        :param source_data: Data source.
        :param iterator_query: Iterative query that selects a set of context objects from s.
        :param attribute_mappings: Mapping that associates an attribute a with an extraction query q'.
        :return: None
        """
        super().__init__()
        self.source_data = source_data
        self.iterator_query = iterator_query
        self.attribute_mappings = attribute_mappings

    @abstractmethod
    def _apply_iterator(self, data: Any, query: str) -> Any:
        """
        Apply the iterator query on the data source (function eval(D, q)).
        :param data: Data source.
        :param query: Iterator query.
        :return: Iterable of context objects.
        """
        pass

    @abstractmethod
    def _apply_extraction(self, context: Any, query: str) -> Any:
        """
        Apply the extraction query on a context object (function eval'(D, d, q')).
        :param context: Context object.
        :param query: Extraction query.
        :return: Iterable of extracted values for the attribute.
        """
        pass

    def explain(self, indent: int = 0, prefix: str = "") -> str:
        """
        Generate a human-readable explanation of the Source operator.
        :param indent: Current indentation level.
        :param prefix: Prefix for tree structure (e.g., "â”œâ”€", "â””â”€").
        :return: String representation of the Source operator.
        """
        return super().explain(indent=indent, prefix=prefix)

    def explain_json(self) -> Dict[str, Any]:
        """
        Generate a JSON-serializable explanation of the Source operator.
        :return: Dictionary representing the Source operator.
        """
        return super().explain_json()

    def execute(self) -> Iterator[MappingTuple]:
        """
        Executes the source operator by iterating contexts and expanding extracted value combinations.
        :return: Iterable of MappingTuple rows produced by the source.
        """
        return super().execute()

    def _extract_context_values(self, context: Any, attr_has_value: Dict[str, bool]) -> Dict[str, Any]:
        """
        Extracts attribute values for a single iterator context.
        :param context: Current iterator context.
        :param attr_has_value: Mutable attribute-presence tracker.
        :return: Mapping of attributes to extracted value lists.
        """
        extracted_values = {}
        for attr_name, extraction_query in self.attribute_mappings.items():
            values = self._apply_extraction(context, extraction_query)
            if values:
                attr_has_value[attr_name] = True
            extracted_values[attr_name] = values
        return extracted_values

    def _yield_rows_from_extractions(self, extracted_values: Dict[str, Any]):
        """
        Expands extracted attribute values into MappingTuple combinations.
        :param extracted_values: Mapping of attributes to extracted value lists.
        :return: Iterable of MappingTuple rows for the context.
        """
        keys = list(extracted_values.keys())
        values_lists = list(extracted_values.values())
        for combination in product(*values_lists):
            yield MappingTuple(self._normalize_row_dict(dict(zip(keys, combination))))

    @staticmethod
    def _normalize_row_dict(row_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Replaces None values by EPSILON in a row dictionary.
        :param row_dict: Row dictionary to normalize.
        :return: Normalized row dictionary.
        """
        normalized = dict(row_dict)
        for key, value in list(normalized.items()):
            if value is None:
                normalized[key] = EPSILON
        return normalized

    @staticmethod
    def _raise_for_missing_references(attr_has_value: Dict[str, bool]) -> None:
        """
        Raises when strict reference mode detects attributes with no extracted values.
        :param attr_has_value: Attribute-presence tracker.
        :return: None
        """
        missing_attrs = [name for name, has_value in attr_has_value.items() if not has_value]
        if missing_attrs:
            missing = ", ".join(sorted(missing_attrs))
            raise ValueError(f"Undefined logical reference(s): {missing}")

