from typing import Any
from pyhartig.expressions.Expression import Expression
from pyhartig.algebra.Tuple import MappingTuple, EPSILON

class Reference(Expression):
    """
    Represents a reference to an attribute of the tuple
    """

    def __init__(self, attribute_name: str):
        """
        Initializes a Reference expression.
        :param attribute_name: The name of the attribute to reference.
        """
        self.attribute_name = attribute_name

    def evaluate(self, tuple_data: MappingTuple) -> Any:
        """
        Evaluates the reference against the provided tuple data.
        :param tuple_data: The tuple data to evaluate against.
        :return: The value of the referenced attribute, or EPSILON if not found.
        """
        # Check if the attribute exists in the tuple data : if yes, return its value
        return tuple_data.get(self.attribute_name, EPSILON)

    def __repr__(self):
        """
        Returns a string representation of the Reference expression.
        :return: A string representing the Reference expression.
        """
        return f"Ref({self.attribute_name})"