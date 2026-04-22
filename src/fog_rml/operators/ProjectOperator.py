from typing import Iterable, Dict, Any, Set

from fog_rml.algebra.Tuple import MappingTuple
from fog_rml.operators.Operator import Operator


class ProjectOperator(Operator):
    """
    Restricts a mapping relation to a specified subset of attributes.

    Project^P(r) : (A, I) -> (P, I')
    - r = (A, I) : Source mapping relation with attributes A and instance I
    - P âŠ† A : Non-empty subset of attributes to retain
    - Result : New mapping relation (P, I') where I' = { t[P] | t âˆˆ I }

    For each tuple t in the input relation, the projection t[P] creates a new tuple
    where dom(t[P]) = P and for each attribute a âˆˆ P: t[P](a) = t(a)
    """

    def __init__(self, operator: Operator, attributes: Set[str]):
        """
        Initializes the Project operator.

        :param operator: The child operator whose results will be projected (provides relation r)
        :param attributes: A set of attribute names P to retain in the output tuples (P âŠ† A)
        :return: None
        """
        super().__init__()
        self.operator = operator
        self.attributes = set(attributes)

    def execute(self) -> Iterable[MappingTuple]:
        """
        Executes the Project logic.

        I' = { t[P] | t âˆˆ I }

        For each tuple t in the input relation, creates a new tuple t[P] containing
        only the attributes in P with their original values.

        Strict mode: Raises an exception if any attribute in P is not present in a tuple.
        This ensures conformance with classical relational algebra where P âŠ† A.

        :return: Iterable of MappingTuples with only the specified attributes P.
        :raises KeyError: If an attribute in P is not found in a tuple (strict mode).
        """
        return super().execute()

    def explain(self, indent: int = 0, prefix: str = "") -> str:
        """
        Generate a human-readable explanation of the Project operator.

        :param indent: Current indentation level
        :param prefix: Prefix for tree structure (e.g., "â”œâ”€", "â””â”€")
        :return: String representation of the operator tree
        """
        return super().explain(indent=indent, prefix=prefix)

    def explain_json(self) -> Dict[str, Any]:
        """
        Generate a JSON-serializable explanation of the Project operator.

        :return: Dictionary representing the operator tree structure
        """
        return super().explain_json()

