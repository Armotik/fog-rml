from __future__ import annotations

from typing import Any, Dict


class JsonExplainVisitor:
    """
    Visitor that renders operator trees as JSON-serializable dictionaries.
    """

    def visit(self, operator: Any) -> Dict[str, Any]:
        from fog_rml.operators.EquiJoinOperator import EquiJoinOperator
        from fog_rml.operators.ExtendOperator import ExtendOperator
        from fog_rml.operators.ProjectOperator import ProjectOperator
        from fog_rml.operators.SourceOperator import SourceOperator
        from fog_rml.operators.UnionOperator import UnionOperator
        from fog_rml.operators.sources.CsvSourceOperator import CsvSourceOperator
        from fog_rml.operators.sources.JsonSourceOperator import JsonSourceOperator
        from fog_rml.operators.sources.MysqlSourceOperator import MysqlSourceOperator
        from fog_rml.operators.sources.PostgresqlSourceOperator import PostgresqlSourceOperator
        from fog_rml.operators.sources.SparqlSourceOperator import SparqlSourceOperator
        from fog_rml.operators.sources.SqlserverSourceOperator import SqlserverSourceOperator
        from fog_rml.operators.sources.XmlSourceOperator import XmlSourceOperator

        if isinstance(operator, ProjectOperator):
            return self._visit_project(operator)
        if isinstance(operator, ExtendOperator):
            return self._visit_extend(operator)
        if isinstance(operator, UnionOperator):
            return self._visit_union(operator)
        if isinstance(operator, EquiJoinOperator):
            return self._visit_equijoin(operator)
        if isinstance(operator, SparqlSourceOperator):
            return self._visit_sparql_source(operator)
        if isinstance(operator, MysqlSourceOperator):
            return self._visit_database_source(operator, "MYSQL")
        if isinstance(operator, PostgresqlSourceOperator):
            return self._visit_database_source(operator, "POSTGRESQL")
        if isinstance(operator, SqlserverSourceOperator):
            return self._visit_database_source(operator, "SQLSERVER")
        if isinstance(operator, JsonSourceOperator):
            return self._visit_json_source(operator)
        if isinstance(operator, CsvSourceOperator):
            return self._visit_simple_source(operator, "CSV")
        if isinstance(operator, XmlSourceOperator):
            return self._visit_simple_source(operator, "XML")
        if isinstance(operator, SourceOperator):
            return self._visit_source(operator)
        return self._fallback(operator)

    def _visit_source(self, operator: Any) -> Dict[str, Any]:
        return {
            "type": "Source",
            "operator_class": operator.__class__.__name__,
            "parameters": {
                "iterator": getattr(operator, "iterator_query", None),
                "attribute_mappings": dict(getattr(operator, "attribute_mappings", {})),
            },
        }

    def _visit_simple_source(self, operator: Any, source_type: str) -> Dict[str, Any]:
        base = self._visit_source(operator)
        base["parameters"]["source_type"] = source_type
        return base

    def _visit_json_source(self, operator: Any) -> Dict[str, Any]:
        base = self._visit_simple_source(operator, "JSON")
        base["parameters"]["jsonpath_iterator"] = getattr(operator, "iterator_query", None)
        return base

    def _visit_sparql_source(self, operator: Any) -> Dict[str, Any]:
        base = self._visit_json_source(operator)
        base["parameters"]["source_type"] = "SPARQL"
        return base

    def _visit_database_source(self, operator: Any, source_type: str) -> Dict[str, Any]:
        base = self._visit_simple_source(operator, source_type)
        base["parameters"]["dsn"] = getattr(operator, "_dsn", None)
        base["parameters"]["query"] = getattr(operator, "_query", None)
        base["parameters"]["table_name"] = getattr(operator, "_table_name", None)
        return base

    def _visit_extend(self, operator: Any) -> Dict[str, Any]:
        return {
            "type": "Extend",
            "parameters": {
                "new_attribute": operator.new_attribute,
                "expression": self._expression_to_json(operator.expression),
            },
            "parent": self._render_child_json(operator.parent_operator),
        }

    def _visit_project(self, operator: Any) -> Dict[str, Any]:
        return {
            "type": "Project",
            "parameters": {
                "attributes": sorted(operator.attributes),
            },
            "parent": self._render_child_json(operator.operator),
        }

    def _visit_union(self, operator: Any) -> Dict[str, Any]:
        return {
            "type": "Union",
            "parameters": {
                "operator_count": len(operator.operators),
                "distinct": operator.distinct,
            },
            "children": [self._render_child_json(child) for child in operator.operators],
        }

    def _visit_equijoin(self, operator: Any) -> Dict[str, Any]:
        return {
            "type": "EquiJoin",
            "parameters": {
                "join_conditions": [
                    {"left": left, "right": right}
                    for left, right in operator.join_conditions
                ],
                "left_attributes": operator.left_attributes,
                "right_attributes": operator.right_attributes,
            },
            "left": self._render_child_json(operator.left_operator),
            "right": self._render_child_json(operator.right_operator),
        }

    def _expression_to_json(self, expr: Any) -> Dict[str, Any]:
        from fog_rml.algebra.Terms import BlankNode, IRI, Literal
        from fog_rml.expressions.Constant import Constant
        from fog_rml.expressions.FunctionCall import FunctionCall
        from fog_rml.expressions.Reference import Reference

        if isinstance(expr, Constant):
            value = expr.value
            if isinstance(value, IRI):
                return {
                    "type": "Constant",
                    "value_type": "IRI",
                    "value": value.value,
                }
            if isinstance(value, Literal):
                return {
                    "type": "Constant",
                    "value_type": "Literal",
                    "value": value.lexical_form,
                    "datatype": value.datatype_iri,
                }
            if isinstance(value, BlankNode):
                return {
                    "type": "Constant",
                    "value_type": "BlankNode",
                    "value": value.identifier,
                }
            return {
                "type": "Constant",
                "value_type": type(value).__name__,
                "value": str(value),
            }

        if isinstance(expr, Reference):
            return {
                "type": "Reference",
                "attribute": expr.attribute_name,
            }

        if isinstance(expr, FunctionCall):
            func_name = getattr(expr.function, "__name__", str(expr.function))
            return {
                "type": "FunctionCall",
                "function": func_name,
                "arguments": [self._expression_to_json(arg) for arg in expr.arguments],
            }

        return {
            "type": "Unknown",
            "value": str(expr),
        }

    def _fallback(self, operator: Any) -> Dict[str, Any]:
        from fog_rml.operators.Operator import Operator as BaseOperator

        method = getattr(type(operator), "explain_json", None)
        if method is not None and method is not BaseOperator.explain_json:
            return operator.explain_json()
        raise TypeError(f"Unsupported operator for JSON explanation: {type(operator).__name__}")

    def _render_child_json(self, child: Any) -> Dict[str, Any]:
        from fog_rml.operators.EquiJoinOperator import EquiJoinOperator
        from fog_rml.operators.ExtendOperator import ExtendOperator
        from fog_rml.operators.ProjectOperator import ProjectOperator
        from fog_rml.operators.SourceOperator import SourceOperator
        from fog_rml.operators.UnionOperator import UnionOperator

        if isinstance(child, (SourceOperator, ExtendOperator, ProjectOperator, UnionOperator, EquiJoinOperator)):
            return self.visit(child)

        method = getattr(child, "explain_json", None)
        if callable(method):
            return method()

        raise TypeError(f"Unsupported operator for JSON explanation: {type(child).__name__}")
