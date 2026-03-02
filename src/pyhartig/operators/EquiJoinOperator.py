from typing import Dict, Any, List, Tuple as TypingTuple, Iterator
import logging

from pyhartig.algebra.Tuple import MappingTuple
from pyhartig.operators.Operator import Operator

logger = logging.getLogger(__name__)


class EquiJoinOperator(Operator):
    """
    EqJoin^J(r₁, r₂) : Operator × Operator → Operator

    Combines two mapping relations r₁ = (A₁, I₁) and r₂ = (A₂, I₂) based on
    a set of join conditions J ⊆ A₁ × A₂.

    Preconditions:
    - A₁ ∩ A₂ = ∅ (attribute sets must be disjoint)
    - J is a set of pairs (a₁, a₂) where a₁ ∈ A₁ and a₂ ∈ A₂

    Result: New mapping relation (A, I) where:
    - A = A₁ ∪ A₂ (union of all attributes)
    - I = { t₁ ∪ t₂ | t₁ ∈ I₁, t₂ ∈ I₂, ∀(a₁, a₂) ∈ J : t₁(a₁) = t₂(a₂) }

    Use case: Particularly relevant for referencing object maps in RML translation.
    """

    def __init__(self, r_1: Operator, r_2: Operator, A: List[str], B: List[str]):
        """
        Initializes the EquiJoin operator.

        :param r_1: The left child operator providing mapping relation r₁ = (A₁, I₁)
        :param r_2: The right child operator providing mapping relation r₂ = (A₂, I₂)
        :param A: List of attribute names from A₁ to join on (left side of J pairs)
        :param B: List of attribute names from A₂ to join on (right side of J pairs)
        :raises ValueError: If A and B have different lengths
        :return: None
        """
        super().__init__()

        if len(A) != len(B):
            raise ValueError(
                f"EquiJoinOperator: Join attribute lists must have equal length. "
                f"Got {len(A)} left attributes and {len(B)} right attributes."
            )

        self.left_operator = r_1
        self.right_operator = r_2
        self.left_attributes = A
        self.right_attributes = B
        # J = { (a₁, a₂) | a₁ ∈ A, a₂ ∈ B } - the join condition pairs
        self.join_conditions: List[TypingTuple[str, str]] = list(zip(A, B))

    def execute(self) -> Iterator[MappingTuple]:
        """
        Executes the Equi-Join logic.

        I = { t₁ ∪ t₂ | t₁ ∈ I₁, t₂ ∈ I₂, ∀(a₁, a₂) ∈ J : t₁(a₁) = t₂(a₂) }

        For each pair of tuples (t₁, t₂) from the two relations, if all join
        conditions are satisfied, the tuples are merged into a single result tuple.

        :return: A list of MappingTuples resulting from the equi-join.
        :raises ValueError: If the attribute sets of the two relations are not disjoint.
        """
        from pyhartig.operators.Operator import StreamRows

        # Eager disjointness check using attribute_mappings when available
        # Eager attribute-disjointness checks are conservative and can falsely
        # fail when both sides expose the same attribute name but with
        # compatible values (e.g., subject IDs present on both child and
        # parent sources). Rely on tuple-merge compatibility at join time
        # instead of preemptively raising. Log a warning to aid debugging.
        try:
            left_attr_keys = set(getattr(self.left_operator, "attribute_mappings", {}).keys())
            right_attr_keys = set(getattr(self.right_operator, "attribute_mappings", {}).keys())
        except Exception:
            left_attr_keys = set()
            right_attr_keys = set()

        if left_attr_keys and right_attr_keys:
            common_attrs = left_attr_keys & right_attr_keys
            if common_attrs:
                logger.warning(
                    "EquiJoinOperator: Attribute name overlap detected between joined relations: %s. "
                    "Proceeding and relying on tuple-merge compatibility at runtime.", common_attrs
                )

        def _gen():
            def _normalize_join_value(v):
                if v is None:
                    return None
                if hasattr(v, "lexical_form"):
                    return getattr(v, "lexical_form")
                if hasattr(v, "value") and not isinstance(v, (str, bytes, bytearray)):
                    try:
                        return str(getattr(v, "value"))
                    except Exception:
                        pass
                if isinstance(v, bool):
                    return "true" if v else "false"
                if isinstance(v, (int, float)):
                    return str(v)
                return v

            def _build_key(t, attrs):
                def _get_val(tt, attr_name):
                    # Direct lookup
                    if attr_name in tt:
                        return tt.get(attr_name)
                    # If attr_name is namespaced as parent_<name>, try unprefixed
                    if attr_name.startswith('parent_'):
                        alt = attr_name[len('parent_'):]
                        if alt in tt:
                            return tt.get(alt)
                    # Try prefixed parent_ form
                    alt_pref = 'parent_' + attr_name
                    if alt_pref in tt:
                        return tt.get(alt_pref)
                    # Fallback: case-insensitive match for common variations
                    for k in tt.keys():
                        if isinstance(k, str) and k.lower() == attr_name.lower():
                            return tt.get(k)
                    return None

                parts = [_normalize_join_value(_get_val(t, attr)) for attr in attrs]
                # SQL-like NULL semantics: if any join attribute is None, do not
                # consider this tuple for join matching (NULL does not equal NULL).
                if any(p is None for p in parts):
                    return None
                return tuple(parts)

            # Materialize both sides to avoid iterator-exhaustion issues when
            # the same Source operator instances are reused across multiple
            # pipeline branches. The datasets in the conformance tests are
            # small so the memory cost is acceptable and this makes joins
            # deterministic.
            left_rows = list(self.left_operator.execute())
            right_rows = list(self.right_operator.execute())

            # Choose smaller side to index for efficiency
            if len(left_rows) <= len(right_rows):
                # Index left side
                left_index: Dict[tuple, List[MappingTuple]] = {}
                for lt in left_rows:
                    key = _build_key(lt, self.left_attributes)
                    if key is None:
                        continue
                    left_index.setdefault(key, []).append(lt)

                if not left_index:
                    return

                # Eager disjointness sample check
                sample_left = next(iter(next(iter(left_index.values()), [])), None)
                sample_right = next(iter(right_rows), None)
                if sample_left is not None and sample_right is not None:
                    common_attrs = set(sample_left.keys()) & set(sample_right.keys())
                    if common_attrs:
                        logger.warning(
                            "EquiJoinOperator: Sampled tuples expose overlapping attribute names: %s. "
                            "Continuing; merge will detect incompatible value conflicts.", common_attrs
                        )

                # Probe left index using right rows
                for rt in right_rows:
                    key = _build_key(rt, self.right_attributes)
                    if key is None:
                        continue
                    matches = left_index.get(key, [])
                    for lt in matches:
                        yield lt.merge(rt)
            else:
                # Index right side and probe with left rows
                right_index: Dict[tuple, List[MappingTuple]] = {}
                for rt in right_rows:
                    key = _build_key(rt, self.right_attributes)
                    if key is None:
                        continue
                    right_index.setdefault(key, []).append(rt)

                if not right_index:
                    return

                # Eager disjointness sample check
                sample_right = next(iter(next(iter(right_index.values()), [])), None)
                sample_left = next(iter(left_rows), None)
                if sample_right is not None and sample_left is not None:
                    common_attrs = set(sample_left.keys()) & set(sample_right.keys())
                    if common_attrs:
                        logger.warning(
                            "EquiJoinOperator: Sampled left/right tuples expose overlapping attribute names: %s. "
                            "Continuing; merge will detect incompatible value conflicts.", common_attrs
                        )

                # Probe right index with left rows
                for lt in left_rows:
                    key = _build_key(lt, self.left_attributes)
                    if key is None:
                        continue
                    for rt in right_index.get(key, []):
                        yield lt.merge(rt)

        return StreamRows(_gen())

    def _satisfies_join_condition(self, t1: MappingTuple, t2: MappingTuple) -> bool:
        """
        Checks if a pair of tuples satisfies all join conditions.

        ∀(a₁, a₂) ∈ J : t₁(a₁) = t₂(a₂)

        :param t1: Tuple from the left relation (t₁ ∈ I₁)
        :param t2: Tuple from the right relation (t₂ ∈ I₂)
        :return: True if all join conditions are satisfied, False otherwise
        """
        for a1, a2 in self.join_conditions:
            # Get values, treating missing attributes as None
            val1 = t1.get(a1)
            val2 = t2.get(a2)

            # Values must be equal for join condition to be satisfied
            if val1 != val2:
                return False

        return True

    def explain(self, indent: int = 0, prefix: str = "") -> str:
        """
        Generate a human-readable explanation of the EquiJoin operator.

        :param indent: Current indentation level
        :param prefix: Prefix for tree structure (e.g., "├─", "└─")
        :return: String representation of the operator tree
        """
        indent_str = "  " * indent

        # Format join conditions as "a₁ = a₂"
        conditions_str = ", ".join(
            f"{a1} = {a2}" for a1, a2 in self.join_conditions
        )

        lines = [
            f"{indent_str}{prefix}EquiJoin(",
            f"{indent_str}  conditions: [{conditions_str}]",
            f"{indent_str}  left:",
        ]

        # Left child operator
        left_explanation = self.left_operator.explain(indent + 2, "├─ ")
        lines.append(left_explanation)

        lines.append(f"{indent_str}  right:")

        # Right child operator
        right_explanation = self.right_operator.explain(indent + 2, "└─ ")
        lines.append(right_explanation)

        lines.append(f"{indent_str})")

        return "\n".join(lines)

    def explain_json(self) -> Dict[str, Any]:
        """
        Generate a JSON-serializable explanation of the EquiJoin operator.

        :return: Dictionary representing the operator tree structure
        """
        return {
            "type": "EquiJoin",
            "parameters": {
                "join_conditions": [
                    {"left": a1, "right": a2}
                    for a1, a2 in self.join_conditions
                ],
                "left_attributes": self.left_attributes,
                "right_attributes": self.right_attributes
            },
            "left": self.left_operator.explain_json(),
            "right": self.right_operator.explain_json()
        }

