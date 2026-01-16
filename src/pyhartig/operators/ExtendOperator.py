from typing import List, Dict, Any
from pyhartig.operators.Operator import Operator
from pyhartig.algebra.Tuple import MappingTuple
from pyhartig.expressions.Expression import Expression


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

    def execute(self) -> List[MappingTuple]:
        """
        Executes the Extend logic.
        r' = { t U {a -> eval(phi, t)} | t in r }
        :return: A list of extended MappingTuples.
        """

        # Get input tuples from parent
        parent_rows = self.parent_operator.execute()

        new_rows = []
        for row in parent_rows:
            # Calculate the new value using the Expression system
            computed_value = self.expression.evaluate(row)

            # Create a new immutable tuple with the extended attribute
            new_row = row.extend(self.new_attribute, computed_value)

            new_rows.append(new_row)

        return new_rows

    def explain(self, indent: int = 0, prefix: str = "") -> str:
        """
        Generate a human-readable explanation of the Extend operator.
        :param indent: Current indentation level
        :param prefix: Prefix for tree structure (e.g., "├─", "└─")
        :return: String representation of the operator tree
        """
        indent_str = "  " * indent
        lines = [f"{indent_str}{prefix}Extend(", f"{indent_str}  attribute: {self.new_attribute}",
                 f"{indent_str}  expression: {self._explain_expression(self.expression, indent + 2)}",
                 f"{indent_str}  parent:"]

        # Current operator

        # Recursive call to parent
        parent_explanation = self.parent_operator.explain(indent + 2, "└─ ")
        lines.append(parent_explanation)

        lines.append(f"{indent_str})")

        return "\n".join(lines)

    def explain_json(self) -> Dict[str, Any]:
        """
        Generate a JSON-serializable explanation of the Extend operator.
        :return: Dictionary representing the operator tree structure
        """
        return {
            "type": "Extend",
            "parameters": {
                "new_attribute": self.new_attribute,
                "expression": self._expression_to_json(self.expression)
            },
            "parent": self.parent_operator.explain_json()
        }

    def _explain_expression(self, expr, indent: int) -> str:
        """
        Helper to explain expressions recursively.
        :param expr: The expression to explain
        :param indent: Current indentation level
        :return: String representation of the expression
        """
        from pyhartig.expressions.Constant import Constant
        from pyhartig.expressions.Reference import Reference
        from pyhartig.expressions.FunctionCall import FunctionCall

        if isinstance(expr, Constant):
            return f"Const({repr(expr.value)})"

        elif isinstance(expr, Reference):
            return f"Ref({expr.attribute_name})"

        elif isinstance(expr, FunctionCall):
            func_name = getattr(expr.function, "__name__", str(expr.function))
            args = [self._explain_expression(arg, 0) for arg in expr.arguments]
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
        from pyhartig.expressions.Constant import Constant
        from pyhartig.expressions.Reference import Reference
        from pyhartig.expressions.FunctionCall import FunctionCall
        from pyhartig.algebra.Terms import IRI, Literal, BlankNode

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
