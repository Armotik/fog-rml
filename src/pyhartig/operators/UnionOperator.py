from typing import Iterator, Dict, Any

from pyhartig.algebra.Tuple import MappingTuple
from pyhartig.operators.Operator import Operator


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

    def execute(self) -> Iterator[MappingTuple]:
        """
        Executes all child operators and merges their results.
        Union(r1, r2, ..., rn) = new MpaaingRelation (A_1, I_union)
        I_union = I_1 U I_2 U ... U I_n
        :return:
        """
        from pyhartig.operators.Operator import StreamRows

        def _gen():
            # Stream results from each child operator in order
            if not self.distinct:
                for op in self.operators:
                    for row in op.execute():
                        yield row
                return

            # distinct=True: track seen tuples and yield each unique row once
            seen = set()
            for op in self.operators:
                for row in op.execute():
                    try:
                        key = tuple(sorted(row.items()))
                    except Exception:
                        key = str(row)

                    if key in seen:
                        continue
                    seen.add(key)
                    yield row

        return StreamRows(_gen())

    def explain(self, indent: int = 0, prefix: str = "") -> str:
        """
        Generate a human-readable explanation of the Union operator.
        :param indent: Current indentation level
        :param prefix: Prefix for tree structure (e.g., "├─", "└─")
        :return: String representation of the operator tree
        """
        indent_str = "  " * indent
        lines = [f"{indent_str}{prefix}Union(", f"{indent_str}  operators: {len(self.operators)}"]

        for i, op in enumerate(self.operators):
            is_last = (i == len(self.operators) - 1)
            child_prefix = "└─ " if is_last else "├─ "
            lines.append(f"{indent_str}  {child_prefix}[{i}]:")
            lines.append(op.explain(indent + 2, ""))

        lines.append(f"{indent_str})")

        return "\n".join(lines)

    def explain_json(self) -> Dict[str, Any]:
        """
        Generate a JSON-serializable explanation of the Union operator.
        :return: Dictionary representing the operator tree structure
        """
        return {
            "type": "Union",
            "parameters": {
                "operator_count": len(self.operators),
                "distinct": self.distinct
            },
            "children": [op.explain_json() for op in self.operators]
        }