from typing import Iterable, Dict, Any
from fog_rml.operators.Operator import Operator
from fog_rml.algebra.Tuple import MappingTuple
from fog_rml.expressions.Expression import Expression


class ExtendOperator(Operator):
    """
    Implements the Extend operator.
    Extends a mapping relation with a new attribute derived from an expression phi.
    """

    def __init__(self, parent_operator: Operator, new_attribute: str, expression: Expression):
        """
        Initializes the Extend operator.
        :param parent_operator: The operator that provides the input relation (r)
        :param new_attribute: The name of the new attribute to add (a)
        :param expression: The expression to evaluate (phi)
        :return: None
        """
        super().__init__()
        self.parent_operator = parent_operator
        self.new_attribute = new_attribute
        self.expression = expression

    def execute(self) -> Iterable[MappingTuple]:
        """
        Executes the Extend logic.
        r' = { t U {a -> eval(phi, t)} | t in r }
        :return: Iterable of extended MappingTuples.
        """
        return super().execute()

    def explain(self, indent: int = 0, prefix: str = "") -> str:
        """
        Generate a human-readable explanation of the Extend operator.
        :param indent: Current indentation level
        :param prefix: Prefix for tree structure (e.g., "â”œâ”€", "â””â”€")
        :return: String representation of the operator tree
        """
        return super().explain(indent=indent, prefix=prefix)

    def explain_json(self) -> Dict[str, Any]:
        """
        Generate a JSON-serializable explanation of the Extend operator.
        :return: Dictionary representing the operator tree structure
        """
        return super().explain_json()

    def _explain_expression(self, expr) -> str:
        """
        Helper to explain expressions recursively.
        :param expr: The expression to explain
        :return: String representation of the expression
        """
        from fog_rml.expressions.Constant import Constant
        from fog_rml.expressions.Reference import Reference
        from fog_rml.expressions.FunctionCall import FunctionCall

        if isinstance(expr, Constant):
            return f"Const({repr(expr.value)})"

        elif isinstance(expr, Reference):
            return f"Ref({expr.attribute_name})"

        elif isinstance(expr, FunctionCall):
            func_name = getattr(expr.function, "__name__", str(expr.function))
            args = [self._explain_expression(arg) for arg in expr.arguments]
            args_str = ", ".join(args)
            return f"{func_name}({args_str})"

        else:
            return str(expr)

    def _expression_to_json(self, expr) -> Dict[str, Any]:
        """
        Helper to convert expressions to JSON-serializable format.
        :param expr: The expression to convert
        :return: Dictionary representation of the expression
        """
        from fog_rml.expressions.Constant import Constant
        from fog_rml.expressions.Reference import Reference
        from fog_rml.expressions.FunctionCall import FunctionCall
        from fog_rml.algebra.Terms import IRI, Literal, BlankNode

        if isinstance(expr, Constant):
            value = expr.value

            # Handle RDF terms
            if isinstance(value, IRI):
                return {
                    "type": "Constant",
                    "value_type": "IRI",
                    "value": value.value
                }
            elif isinstance(value, Literal):
                return {
                    "type": "Constant",
                    "value_type": "Literal",
                    "value": value.lexical_form,
                    "datatype": value.datatype_iri
                }
            elif isinstance(value, BlankNode):
                return {
                    "type": "Constant",
                    "value_type": "BlankNode",
                    "value": value.identifier
                }
            else:
                return {
                    "type": "Constant",
                    "value_type": type(value).__name__,
                    "value": str(value)
                }

        elif isinstance(expr, Reference):
            return {
                "type": "Reference",
                "attribute": expr.attribute_name
            }

        elif isinstance(expr, FunctionCall):
            func_name = getattr(expr.function, "__name__", str(expr.function))
            return {
                "type": "FunctionCall",
                "function": func_name,
                "arguments": [self._expression_to_json(arg) for arg in expr.arguments]
            }

        else:
            return {
                "type": "Unknown",
                "value": str(expr)
            }

