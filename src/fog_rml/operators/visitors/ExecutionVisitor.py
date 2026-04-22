from __future__ import annotations

import os
from typing import Any, Iterable

from fog_rml.algebra.Tuple import MappingTuple


class ExecutionVisitor:
    """
    Visitor that executes operator trees.
    """

    def visit(self, operator: Any) -> Iterable[MappingTuple]:
        from fog_rml.operators.EquiJoinOperator import EquiJoinOperator
        from fog_rml.operators.ExtendOperator import ExtendOperator
        from fog_rml.operators.ProjectOperator import ProjectOperator
        from fog_rml.operators.SourceOperator import SourceOperator
        from fog_rml.operators.UnionOperator import UnionOperator

        if isinstance(operator, SourceOperator):
            return self._visit_source(operator)
        if isinstance(operator, ExtendOperator):
            return self._visit_extend(operator)
        if isinstance(operator, ProjectOperator):
            return self._visit_project(operator)
        if isinstance(operator, UnionOperator):
            return self._visit_union(operator)
        if isinstance(operator, EquiJoinOperator):
            return self._visit_equijoin(operator)

        raise TypeError(f"Unsupported operator for execution: {type(operator).__name__}")

    def _visit_source(self, operator: Any):
        strict_flag = os.getenv("FOG_RML_STRICT_REFERENCES")
        if strict_flag is None:
            strict_flag = os.getenv("FOG_RML_STRICT_REFERENCES", "0")
        strict_references = strict_flag == "1"

        def _gen():
            contexts = operator._apply_iterator(operator.source_data, operator.iterator_query)
            context_count = 0
            attr_has_value = dict.fromkeys(operator.attribute_mappings, False)

            for context in contexts:
                context_count += 1
                extracted_values = operator._extract_context_values(context, attr_has_value)
                yield from operator._yield_rows_from_extractions(extracted_values)

            if strict_references and context_count > 0:
                operator._raise_for_missing_references(attr_has_value)

        return _gen()

    def _visit_extend(self, operator: Any):
        from fog_rml.algebra.Tuple import MappingTuple

        def _gen():
            for row in self._execute_child(operator.parent_operator):
                computed_value = operator.expression.evaluate(row)
                try:
                    new_row = row.extend(operator.new_attribute, computed_value)
                except Exception:
                    new_row = MappingTuple({**dict(row), operator.new_attribute: computed_value})
                yield new_row

        return _gen()

    def _visit_project(self, operator: Any):
        from fog_rml.algebra.Tuple import MappingTuple

        parent_rows = self._execute_child(operator.operator)
        parent_iter = iter(parent_rows)

        def _gen():
            try:
                first = next(parent_iter)
            except StopIteration:
                return

            available_attributes = set(first.keys())
            missing_attrs = operator.attributes - available_attributes
            if missing_attrs:
                raise KeyError(
                    f"ProjectOperator: Attribute(s) {missing_attrs} not found in tuple. Available attributes: {available_attributes}."
                )

            yield MappingTuple({attr: first[attr] for attr in operator.attributes})
            for row in parent_iter:
                yield MappingTuple({attr: row[attr] for attr in operator.attributes})

        return _gen()

    def _visit_union(self, operator: Any):
        def _gen():
            row_iterator = operator._iter_distinct_rows() if operator.distinct else operator._iter_bag_rows()
            yield from row_iterator

        return _gen()

    def _visit_equijoin(self, operator: Any):
        operator._warn_for_declared_attribute_overlap()

        def _gen():
            left_rows, right_rows = self._materialize_children(operator.left_operator, operator.right_operator)
            yield from operator._join_materialized_rows(left_rows, right_rows)

        return _gen()

    def _execute_child(self, child: Any):
        from fog_rml.operators.EquiJoinOperator import EquiJoinOperator
        from fog_rml.operators.ExtendOperator import ExtendOperator
        from fog_rml.operators.ProjectOperator import ProjectOperator
        from fog_rml.operators.SourceOperator import SourceOperator
        from fog_rml.operators.UnionOperator import UnionOperator

        if isinstance(child, (SourceOperator, ExtendOperator, ProjectOperator, UnionOperator, EquiJoinOperator)):
            return self.visit(child)

        execute = getattr(child, "execute", None)
        if callable(execute):
            return execute()

        raise TypeError(f"Unsupported child operator for execution: {type(child).__name__}")

    def _materialize_children(self, left_child: Any, right_child: Any):
        return list(self._execute_child(left_child)), list(self._execute_child(right_child))
