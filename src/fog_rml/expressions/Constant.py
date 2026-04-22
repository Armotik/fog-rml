from typing import Any
from fog_rml.expressions.Expression import Expression
from fog_rml.algebra.Tuple import MappingTuple
from fog_rml.algebra.Terms import IRI, Literal, BlankNode
from typing import Union

RdfTerm = Union[IRI, Literal, BlankNode]
class Constant(Expression):
    """
    Represents a constant value in an expression.
    If the expression is a fixed value (e.g., rdf:type or â€œhttp://example.org/â€), it always returns that value, regardless of the tuple.
    """

    def __init__(self, value: RdfTerm):
        """
        Initialize the Constant with a specific value (RDF Term or fixed attribute).
        :param value: The constant value
        """
        self.value = value

    def evaluate(self, tuple_data: MappingTuple) -> Any:
        """
        Evaluate the constant expression, which simply returns its value.
        :param tuple_data: Mapping tuple to evaluate against
        :return: The constant value
        """
        return self.value

    def __repr__(self):
        """
        String representation of the Constant expression.
        :return: String representation
        """
        return f"Const({self.value})"

