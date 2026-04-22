from typing import Iterable, Dict, Any

from fog_rml.algebra.Tuple import MappingTuple
from fog_rml.operators.Operator import Operator


class UnionOperator(Operator):
    """
    Implements the Union operator.
    Merges the results of multiple operators into a single relation.
    """

    def __init__(self, operators: list[Operator], distinct: bool = False):
        """
        Initializes the Union operator.
        :param operators: A list of operators whose results will be merged.
        :return: None
        """
        super().__init__()
        self.operators = operators
        # If True, ensure output contains no duplicate tuples (set semantics)
        # Default False => bag semantics (fast, may contain duplicates)
        self.distinct = bool(distinct)

    def execute(self) -> Iterable[MappingTuple]:
        """
        Executes all child operators and merges their results.
        Union(r1, r2, ..., rn) = new MpaaingRelation (A_1, I_union)
        I_union = I_1 U I_2 U ... U I_n
        :return:
        """
        return super().execute()

    def _iter_bag_rows(self):
        for operator in self.operators:
            yield from operator.execute()

    def _iter_distinct_rows(self):
        seen = set()
        for operator in self.operators:
            for row in operator.execute():
                key = self._build_distinct_key(row)
                if key in seen:
                    continue
                seen.add(key)
                yield row

    @staticmethod
    def _build_distinct_key(row: MappingTuple):
        try:
            return tuple(sorted(row.items()))
        except Exception:
            return str(row)

    def explain(self, indent: int = 0, prefix: str = "") -> str:
        """
        Generate a human-readable explanation of the Union operator.
        :param indent: Current indentation level
        :param prefix: Prefix for tree structure (e.g., "â”œâ”€", "â””â”€")
        :return: String representation of the operator tree
        """
        return super().explain(indent=indent, prefix=prefix)

    def explain_json(self) -> Dict[str, Any]:
        """
        Generate a JSON-serializable explanation of the Union operator.
        :return: Dictionary representing the operator tree structure
        """
        return super().explain_json()

