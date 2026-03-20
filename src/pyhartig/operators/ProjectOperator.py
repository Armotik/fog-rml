from typing import Iterable, Dict, Any, Set

from pyhartig.algebra.Tuple import MappingTuple
from pyhartig.operators.Operator import Operator


class ProjectOperator(Operator):
    """
    Restricts a mapping relation to a specified subset of attributes.

    Project^P(r) : (A, I) -> (P, I')
    - r = (A, I) : Source mapping relation with attributes A and instance I
    - P ⊆ A : Non-empty subset of attributes to retain
    - Result : New mapping relation (P, I') where I' = { t[P] | t ∈ I }

    For each tuple t in the input relation, the projection t[P] creates a new tuple
    where dom(t[P]) = P and for each attribute a ∈ P: t[P](a) = t(a)
    """

    def __init__(self, operator: Operator, attributes: Set[str]):
        """
        Initializes the Project operator.

        :param operator: The child operator whose results will be projected (provides relation r)
        :param attributes: A set of attribute names P to retain in the output tuples (P ⊆ A)
        :return: None
        """
        super().__init__()
        self.operator = operator
        self.attributes = set(attributes)

    def execute(self) -> Iterable[MappingTuple]:
        """
        Executes the Project logic.

        I' = { t[P] | t ∈ I }

        For each tuple t in the input relation, creates a new tuple t[P] containing
        only the attributes in P with their original values.

        Strict mode: Raises an exception if any attribute in P is not present in a tuple.
        This ensures conformance with classical relational algebra where P ⊆ A.

        :return: Iterable of MappingTuples with only the specified attributes P.
        :raises KeyError: If an attribute in P is not found in a tuple (strict mode).
        """
        from pyhartig.operators.Operator import StreamRows

        parent_rows = self.operator.execute()
        parent_iter = iter(parent_rows)

        # Try to peek the first tuple to determine the input attribute set A
        try:
            first = next(parent_iter)
        except StopIteration:
            # Empty input relation => empty result
            return StreamRows(iter(()))

        A = set(first.keys())
        missing_attrs = self.attributes - A
        if missing_attrs:
            raise KeyError(
                f"ProjectOperator: Attribute(s) {missing_attrs} not found in tuple. Available attributes: {A}."
            )

        def _gen():
            # Yield projection for the first tuple
            yield MappingTuple({attr: first[attr] for attr in self.attributes})
            # Continue with the rest of the parent iterator
            for row in parent_iter:
                yield MappingTuple({attr: row[attr] for attr in self.attributes})

        return StreamRows(_gen())

    def explain(self, indent: int = 0, prefix: str = "") -> str:
        """
        Generate a human-readable explanation of the Project operator.

        :param indent: Current indentation level
        :param prefix: Prefix for tree structure (e.g., "├─", "└─")
        :return: String representation of the operator tree
        """
        indent_str = "  " * indent
        sorted_attributes = sorted(self.attributes)

        lines = [
            f"{indent_str}{prefix}Project(",
            f"{indent_str}  attributes: {sorted_attributes}",
            f"{indent_str}  parent:"
        ]

        # Recursive call to parent operator
        parent_explanation = self.operator.explain(indent + 2, "└─ ")
        lines.append(parent_explanation)

        lines.append(f"{indent_str})")

        return "\n".join(lines)

    def explain_json(self) -> Dict[str, Any]:
        """
        Generate a JSON-serializable explanation of the Project operator.

        :return: Dictionary representing the operator tree structure
        """
        return {
            "type": "Project",
            "parameters": {
                "attributes": sorted(self.attributes)
            },
            "parent": self.operator.explain_json()
        }
