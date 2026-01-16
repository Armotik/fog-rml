from abc import abstractmethod
from typing import Any, Dict, List
from itertools import product

from pyhartig.algebra.Tuple import MappingTuple, EPSILON
from pyhartig.operators.Operator import Operator


class SourceOperator(Operator):
    """
    Abstract class defining the algebraic logic of the Source operator
    """

    def __init__(self, source_data: Any, iterator_query: str, attribute_mappings: Dict[str, str]):
        """
        Constructor
        :param source_data: Data source
        :param iterator_query: Iterative query that selects a set of context objects from s
        :param attribute_mappings: Mapping that associates an attribute a with an extraction query q'
        """
        super().__init__()
        self.source_data = source_data
        self.iterator_query = iterator_query
        self.attribute_mappings = attribute_mappings

    @abstractmethod
    def _apply_iterator(self, data: Any, query: str) -> List[Any]:
        """
        Apply the iterator query on the data source (function eval(D, q))
        :param data: Data source
        :param query: Iterator query
        :return: List of context
        """
        pass

    @abstractmethod
    def _apply_extraction(self, context: Any, query: str) -> List[Any]:
        """
        Apply the extraction query on a context object (function eval'(D, d, q'))
        :param context: Context object
        :param query: Extraction query
        :return: List of extracted values for the attribute
        """
        pass

    def explain(self, indent: int = 0, prefix: str = "") -> str:
        """
        Generate a human-readable explanation of the Source operator
        :param indent: Current indentation level
        :param prefix: Prefix for tree structure (e.g., "├─", "└─")
        :return: String representation of the Source operator
        """
        indent_str = "  " * indent
        lines = [f"{indent_str}{prefix}Source(", f"{indent_str}  iterator: {self.iterator_query}",
                 f"{indent_str}  mappings: {list(self.attribute_mappings.keys())}", f"{indent_str})"]

        return "\n".join(lines)

    def explain_json(self) -> Dict[str, Any]:
        """
        Generate a JSON-serializable explanation of the Source operator
        :return: Dictionary representing the Source operator
        """
        return {
            "type": "Source",
            "operator_class": self.__class__.__name__,
            "parameters": {
                "iterator": self.iterator_query,
                "attribute_mappings": self.attribute_mappings
            }
        }

    def execute(self) -> List[MappingTuple]:
        """
        Execute the Source operator logic
        :return: List of rows resulting from the Source operator
        """

        result = []

        # Apply the iterator to get context objects
        contexts = self._apply_iterator(self.source_data, self.iterator_query)

        # For each context, apply the extraction queries for each attribute
        for context in contexts:

            extracted_values = {}

            # Extract values for each attribute
            for attr_name, extraction_query in self.attribute_mappings.items():
                values = self._apply_extraction(context, extraction_query)
                extracted_values[attr_name] = values

            keys = list(extracted_values.keys())
            values_lists = list(extracted_values.values())

            # Generate all combinations of extracted values
            for combination in product(*values_lists):
                row_dict = dict(zip(keys, combination))
                # Convert None values to EPSILON (per algebra semantics)
                row_dict = {k: (EPSILON if v is None else v) for k, v in row_dict.items()}
                row = MappingTuple(row_dict)
                result.append(row)

        return result
