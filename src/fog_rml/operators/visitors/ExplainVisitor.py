from __future__ import annotations

from typing import Any


class ExplainVisitor:
    """
    Visitor that renders operator trees as human-readable text.
    """

    def __init__(self, indent: int = 0, prefix: str = ""):
        self.indent = indent
        self.prefix = prefix

    def visit(self, operator: Any) -> str:
        from fog_rml.operators.EquiJoinOperator import EquiJoinOperator
        from fog_rml.operators.ExtendOperator import ExtendOperator
        from fog_rml.operators.ProjectOperator import ProjectOperator
        from fog_rml.operators.SourceOperator import SourceOperator
        from fog_rml.operators.UnionOperator import UnionOperator

        if isinstance(operator, ProjectOperator):
            return self._visit_project(operator)
        if isinstance(operator, ExtendOperator):
            return self._visit_extend(operator)
        if isinstance(operator, UnionOperator):
            return self._visit_union(operator)
        if isinstance(operator, EquiJoinOperator):
            return self._visit_equijoin(operator)
        if isinstance(operator, SourceOperator):
            return self._visit_source(operator)
        return self._fallback(operator)

    def _visit_source(self, operator: Any) -> str:
        indent_str = "  " * self.indent
        mappings = list(getattr(operator, "attribute_mappings", {}).keys())
        lines = [
            f"{indent_str}{self.prefix}Source(",
            f"{indent_str}  iterator: {getattr(operator, 'iterator_query', None)}",
            f"{indent_str}  mappings: {mappings}",
            f"{indent_str})",
        ]
        return "\n".join(lines)

    def _visit_extend(self, operator: Any) -> str:
        indent_str = "  " * self.indent
        lines = [
            f"{indent_str}{self.prefix}Extend(",
            f"{indent_str}  attribute: {operator.new_attribute}",
            f"{indent_str}  expression: {self._explain_expression(operator.expression)}",
            f"{indent_str}  parent:",
        ]
        lines.append(self._render_child(operator.parent_operator, self.indent + 2, "\\-- "))
        lines.append(f"{indent_str})")
        return "\n".join(lines)

    def _visit_project(self, operator: Any) -> str:
        indent_str = "  " * self.indent
        sorted_attributes = sorted(operator.attributes)
        lines = [
            f"{indent_str}{self.prefix}Project(",
            f"{indent_str}  attributes: {sorted_attributes}",
            f"{indent_str}  parent:",
        ]
        lines.append(self._render_child(operator.operator, self.indent + 2, "\\-- "))
        lines.append(f"{indent_str})")
        return "\n".join(lines)

    def _visit_union(self, operator: Any) -> str:
        indent_str = "  " * self.indent
        lines = [
            f"{indent_str}{self.prefix}Union(",
            f"{indent_str}  operators: {len(operator.operators)}",
        ]

        for index, child in enumerate(operator.operators):
            child_prefix = "\\-- " if index == len(operator.operators) - 1 else "|-- "
            lines.append(f"{indent_str}  {child_prefix}[{index}]:")
            lines.append(self._render_child(child, self.indent + 2, ""))

        lines.append(f"{indent_str})")
        return "\n".join(lines)

    def _visit_equijoin(self, operator: Any) -> str:
        indent_str = "  " * self.indent
        conditions_str = ", ".join(f"{left} = {right}" for left, right in operator.join_conditions)
        lines = [
            f"{indent_str}{self.prefix}EquiJoin(",
            f"{indent_str}  conditions: [{conditions_str}]",
            f"{indent_str}  left:",
        ]
        lines.append(self._render_child(operator.left_operator, self.indent + 2, "|-- "))
        lines.append(f"{indent_str}  right:")
        lines.append(self._render_child(operator.right_operator, self.indent + 2, "\\-- "))
        lines.append(f"{indent_str})")
        return "\n".join(lines)

    def _explain_expression(self, expr: Any) -> str:
        from fog_rml.expressions.Constant import Constant
        from fog_rml.expressions.FunctionCall import FunctionCall
        from fog_rml.expressions.Reference import Reference

        if isinstance(expr, Constant):
            return f"Const({repr(expr.value)})"
        if isinstance(expr, Reference):
            return f"Ref({expr.attribute_name})"
        if isinstance(expr, FunctionCall):
            func_name = getattr(expr.function, "__name__", str(expr.function))
            args = ", ".join(self._explain_expression(arg) for arg in expr.arguments)
            return f"{func_name}({args})"
        return str(expr)

    def _fallback(self, operator: Any) -> str:
        from fog_rml.operators.Operator import Operator as BaseOperator

        method = getattr(type(operator), "explain", None)
        if method is not None and method is not BaseOperator.explain:
            return operator.explain(self.indent, self.prefix)
        raise TypeError(f"Unsupported operator for explanation: {type(operator).__name__}")

    def _render_child(self, child: Any, indent: int, prefix: str) -> str:
        from fog_rml.operators.EquiJoinOperator import EquiJoinOperator
        from fog_rml.operators.ExtendOperator import ExtendOperator
        from fog_rml.operators.ProjectOperator import ProjectOperator
        from fog_rml.operators.SourceOperator import SourceOperator
        from fog_rml.operators.UnionOperator import UnionOperator

        if isinstance(child, (SourceOperator, ExtendOperator, ProjectOperator, UnionOperator, EquiJoinOperator)):
            return ExplainVisitor(indent, prefix).visit(child)

        method = getattr(child, "explain", None)
        if callable(method):
            try:
                return method(indent, prefix)
            except TypeError:
                return method()

        raise TypeError(f"Unsupported operator for explanation: {type(child).__name__}")
