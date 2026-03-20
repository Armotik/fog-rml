from typing import Dict, Any, List, Tuple as TypingTuple, Iterable
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

    def __init__(
            self,
            r_1: Operator,
            r_2: Operator,
            left_join_attributes: List[str] | None = None,
            right_join_attributes: List[str] | None = None,
            **legacy_kwargs,
    ):
        """
        Initializes the EquiJoin operator.

        :param r_1: The left child operator providing mapping relation r₁ = (A₁, I₁)
        :param r_2: The right child operator providing mapping relation r₂ = (A₂, I₂)
        :param left_join_attributes: List of attribute names from A₁ to join on (left side of J pairs)
        :param right_join_attributes: List of attribute names from A₂ to join on (right side of J pairs)
        :raises ValueError: If both join-attribute lists have different lengths
        :return: None
        """
        super().__init__()

        left_join_attributes, right_join_attributes = self._resolve_join_attribute_args(
            left_join_attributes,
            right_join_attributes,
            legacy_kwargs,
        )

        if len(left_join_attributes) != len(right_join_attributes):
            raise ValueError(
                f"EquiJoinOperator: Join attribute lists must have equal length. "
                f"Got {len(left_join_attributes)} left attributes and {len(right_join_attributes)} right attributes."
            )

        self.left_operator = r_1
        self.right_operator = r_2
        self.left_attributes = left_join_attributes
        self.right_attributes = right_join_attributes
        # J = { (a₁, a₂) | a₁ ∈ A, a₂ ∈ B } - the join condition pairs
        self.join_conditions: List[TypingTuple[str, str]] = list(
            zip(left_join_attributes, right_join_attributes)
        )

    @staticmethod
    def _resolve_join_attribute_args(
            left_join_attributes: List[str] | None,
            right_join_attributes: List[str] | None,
            legacy_kwargs,
    ) -> TypingTuple[List[str], List[str]]:
        """
        Resolves legacy `A`/`B` keyword arguments to the normalized join-attribute names.
        :param left_join_attributes: Explicit left-side join attributes.
        :param right_join_attributes: Explicit right-side join attributes.
        :param legacy_kwargs: Additional legacy keyword arguments.
        :return: Tuple of resolved left and right join-attribute lists.
        """
        resolved_left = left_join_attributes if left_join_attributes is not None else legacy_kwargs.pop("A", None)
        resolved_right = right_join_attributes if right_join_attributes is not None else legacy_kwargs.pop("B", None)
        if legacy_kwargs:
            unexpected = ", ".join(sorted(legacy_kwargs))
            raise TypeError(f"Unexpected keyword argument(s): {unexpected}")
        if resolved_left is None or resolved_right is None:
            raise TypeError("EquiJoinOperator requires both left and right join attribute lists.")
        return list(resolved_left), list(resolved_right)

    def execute(self) -> Iterable[MappingTuple]:
        """
        Executes the Equi-Join logic.

        I = { t₁ ∪ t₂ | t₁ ∈ I₁, t₂ ∈ I₂, ∀(a₁, a₂) ∈ J : t₁(a₁) = t₂(a₂) }

        For each pair of tuples (t₁, t₂) from the two relations, if all join
        conditions are satisfied, the tuples are merged into a single result tuple.

        :return: Iterable of MappingTuples resulting from the equi-join.
        :raises ValueError: If the attribute sets of the two relations are not disjoint.
        """
        from pyhartig.operators.Operator import StreamRows

        self._warn_for_declared_attribute_overlap()

        def _gen():
            """Yields the joined MappingTuple rows for the materialized child relations."""
            left_rows, right_rows = self._materialize_rows()
            yield from self._join_materialized_rows(left_rows, right_rows)

        return StreamRows(_gen())

    def _warn_for_declared_attribute_overlap(self) -> None:
        """
        Logs a warning when both child operators declare overlapping attribute names.
        :return: None
        """
        try:
            left_attr_keys = set(getattr(self.left_operator, "attribute_mappings", {}).keys())
            right_attr_keys = set(getattr(self.right_operator, "attribute_mappings", {}).keys())
        except Exception:
            left_attr_keys = set()
            right_attr_keys = set()

        if not left_attr_keys or not right_attr_keys:
            return

        common_attrs = left_attr_keys & right_attr_keys
        if common_attrs:
            logger.warning(
                "EquiJoinOperator: Attribute name overlap detected between joined relations: %s. "
                "Proceeding and relying on tuple-merge compatibility at runtime.", common_attrs
            )

    def _materialize_rows(self) -> TypingTuple[List[MappingTuple], List[MappingTuple]]:
        """
        Materializes both child operator outputs to stable row lists.
        :return: Tuple of left and right MappingTuple lists.
        """
        return list(self.left_operator.execute()), list(self.right_operator.execute())

    def _join_materialized_rows(
            self,
            left_rows: List[MappingTuple],
            right_rows: List[MappingTuple],
    ):
        """
        Joins already materialized child rows by indexing the smaller side.
        :param left_rows: Materialized left-side rows.
        :param right_rows: Materialized right-side rows.
        :return: Iterable of joined MappingTuple rows.
        """
        left_is_indexed = len(left_rows) <= len(right_rows)
        indexed_rows = left_rows if left_is_indexed else right_rows
        probe_rows = right_rows if left_is_indexed else left_rows
        indexed_attrs = self.left_attributes if left_is_indexed else self.right_attributes
        probe_attrs = self.right_attributes if left_is_indexed else self.left_attributes

        join_index = self._build_join_index(indexed_rows, indexed_attrs)
        if not join_index:
            return

        self._warn_for_runtime_overlap(indexed_rows, probe_rows)
        yield from self._probe_join_index(join_index, probe_rows, probe_attrs, left_is_indexed)

    def _build_join_index(
            self,
            rows: List[MappingTuple],
            attributes: List[str],
    ) -> Dict[tuple, List[MappingTuple]]:
        """
        Builds an index of rows keyed by the normalized join attribute values.
        :param rows: Rows to index.
        :param attributes: Join attributes used to build the key.
        :return: Join index keyed by normalized tuples.
        """
        join_index: Dict[tuple, List[MappingTuple]] = {}
        for row in rows:
            join_key = self._build_key(row, attributes)
            if join_key is None:
                continue
            join_index.setdefault(join_key, []).append(row)
        return join_index

    def _probe_join_index(
            self,
            join_index: Dict[tuple, List[MappingTuple]],
            probe_rows: List[MappingTuple],
            probe_attributes: List[str],
            left_is_indexed: bool,
    ):
        """
        Probes a join index with rows from the non-indexed side.
        :param join_index: Join index built from the indexed side.
        :param probe_rows: Rows used to probe the index.
        :param probe_attributes: Join attributes used for probing.
        :param left_is_indexed: Whether the indexed side is the left relation.
        :return: Iterable of joined MappingTuple rows.
        """
        for probe_row in probe_rows:
            join_key = self._build_key(probe_row, probe_attributes)
            if join_key is None:
                continue
            for indexed_row in join_index.get(join_key, []):
                yield indexed_row.merge(probe_row) if left_is_indexed else probe_row.merge(indexed_row)

    def _warn_for_runtime_overlap(
            self,
            indexed_rows: List[MappingTuple],
            probe_rows: List[MappingTuple],
    ) -> None:
        """
        Logs a warning when sampled rows from both sides expose overlapping attributes.
        :param indexed_rows: Rows from the indexed side.
        :param probe_rows: Rows from the probed side.
        :return: None
        """
        sample_indexed = indexed_rows[0] if indexed_rows else None
        sample_probe = probe_rows[0] if probe_rows else None
        if sample_indexed is None or sample_probe is None:
            return

        common_attrs = set(sample_indexed.keys()) & set(sample_probe.keys())
        if common_attrs:
            logger.warning(
                "EquiJoinOperator: Sampled tuples expose overlapping attribute names: %s. "
                "Continuing; merge will detect incompatible value conflicts.", common_attrs
            )

    @staticmethod
    def _normalize_join_value(value):
        """
        Normalizes a join value to a comparable scalar representation.
        :param value: Raw join value.
        :return: Normalized comparable value.
        """
        if value is None:
            return None
        if hasattr(value, "lexical_form"):
            return getattr(value, "lexical_form")
        if hasattr(value, "value") and not isinstance(value, (str, bytes, bytearray)):
            try:
                return str(getattr(value, "value"))
            except Exception:
                pass
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        return value

    @staticmethod
    def _get_join_attribute_value(mapping_tuple: MappingTuple, attribute_name: str):
        """
        Reads a join attribute value with support for parent-prefixed and case-insensitive variants.
        :param mapping_tuple: MappingTuple to inspect.
        :param attribute_name: Join attribute name to resolve.
        :return: Matching join attribute value, or None.
        """
        if attribute_name in mapping_tuple:
            return mapping_tuple.get(attribute_name)
        if attribute_name.startswith("parent_"):
            alternate_name = attribute_name[len("parent_"):]
            if alternate_name in mapping_tuple:
                return mapping_tuple.get(alternate_name)
        prefixed_name = f"parent_{attribute_name}"
        if prefixed_name in mapping_tuple:
            return mapping_tuple.get(prefixed_name)
        for key in mapping_tuple.keys():
            if isinstance(key, str) and key.lower() == attribute_name.lower():
                return mapping_tuple.get(key)
        return None

    def _build_key(self, mapping_tuple: MappingTuple, attributes: List[str]):
        """
        Builds the normalized join key for a tuple and a list of join attributes.
        :param mapping_tuple: MappingTuple to normalize.
        :param attributes: Join attributes used to build the key.
        :return: Join key tuple, or None when at least one part is undefined.
        """
        parts = [
            self._normalize_join_value(self._get_join_attribute_value(mapping_tuple, attribute_name))
            for attribute_name in attributes
        ]
        if any(part is None for part in parts):
            return None
        return tuple(parts)

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
