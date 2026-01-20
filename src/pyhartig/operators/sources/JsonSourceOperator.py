from typing import Any, List, Dict
from jsonpath_ng import parse

from pyhartig.algebra.Terms import Literal
from pyhartig.operators.SourceOperator import SourceOperator
from pyhartig.namespaces import get_xsd_from_python_type

class JsonSourceOperator(SourceOperator):

    def _apply_iterator(self, data: Any, query: str) -> List[Any]:
        """
        Apply the iterator query on the data source (function eval(D, q))
        :param data: JSON data source
        :param query: Iterator query
        :return: List of context
        """
        jsonpath_expr = parse(query)
        return [match.value for match in jsonpath_expr.find(data)]

    def _apply_extraction(self, context: Any, query: str) -> List[Literal]:
        """
        Apply the extraction query on a context object (function eval'(D, d, q'))
        :param context: Context object
        :param query: Extraction query
        :return: List of extracted values for the attribute
        """
        jsonpath_expr = parse(query)
        matches = jsonpath_expr.find(context)

        # If no matches found, return empty list
        if not matches:
            return []

        # Flatten the results
        results = []
        for match in matches:
            val = match.value

            # If the match value is a list, extend the results; otherwise, append the single value
            if isinstance(match.value, list):
                # Apply conversion to each item in the list
                results.extend([self._to_rdf_term(v) for v in val])
            else:
                # Single value conversion
                results.append(self._to_rdf_term(val))
        return results

    def _to_rdf_term(self, value: Any) -> Literal:
        """
        Helper method to convert Python primitives to RDF Literals.
        :param value: Raw Python value (str, int, float, bool...)
        :return: Typed RDF Literal
        """
        datatype_iri = get_xsd_from_python_type(value)

        if isinstance(value, bool):
            # Python 'True' -> XSD 'true' (lowercase)
            lexical_form = str(value).lower()
        elif value is None:
            # Handle JSON null if necessary, though usually filtered out before.
            lexical_form = ""
        else:
            lexical_form = str(value)

        return Literal(lexical_form, datatype_iri.value)

    def explain_json(self) -> Dict[str, Any]:
        """
        Generate a JSON-serializable explanation of the JsonSource operator
        :return: Dictionary representing the operator tree structure
        """
        base = super().explain_json()
        base["parameters"]["source_type"] = "JSON"
        base["parameters"]["jsonpath_iterator"] = self.iterator_query
        return base



