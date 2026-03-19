"""
Test Suite for explain_json() functionality

Tests validate JSON explanation generation for all operators.
"""

import pytest
import json
from pyhartig.operators.sources.JsonSourceOperator import JsonSourceOperator
from pyhartig.operators.ExtendOperator import ExtendOperator
from pyhartig.operators.UnionOperator import UnionOperator
from pyhartig.expressions.Constant import Constant
from pyhartig.expressions.Reference import Reference
from pyhartig.expressions.FunctionCall import FunctionCall
from pyhartig.functions.builtins import to_iri
from pyhartig.algebra.Terms import IRI
from pyhartig.namespaces import FOAF_PERSON


class TestExplainJson:
    """Test suite for JSON explanation functionality."""

    @pytest.fixture
    def sample_source(self):
        """Fixture providing a simple source operator."""
        data = {"items": [{"id": 1, "name": "Test"}]}
        return JsonSourceOperator(
            source_data=data,
            iterator_query="$.items[*]",
            attribute_mappings={"item_id": "$.id", "item_name": "$.name"}
        )

    def test_source_explain_json(self, sample_source):
        """Test JSON explanation for Source operator."""
        explanation = sample_source.explain_json()

        assert explanation["type"] == "Source"
        assert explanation["operator_class"] == "JsonSourceOperator"
        assert "parameters" in explanation
        assert explanation["parameters"]["iterator"] == "$.items[*]"
        assert "item_id" in explanation["parameters"]["attribute_mappings"]

        # Verify it's valid JSON
        json_str = json.dumps(explanation)
        parsed = json.loads(json_str)
        assert parsed["type"] == "Source"

    def test_extend_with_constant_explain_json(self, sample_source):
        """Test JSON explanation for Extend with constant."""
        extend = ExtendOperator(sample_source, "type", Constant("Item"))
        explanation = extend.explain_json()

        assert explanation["type"] == "Extend"
        assert explanation["parameters"]["new_attribute"] == "type"

        expr = explanation["parameters"]["expression"]
        assert expr["type"] == "Constant"
        assert expr["value"] == "Item"

        # Parent should be included
        assert "parent" in explanation
        assert explanation["parent"]["type"] == "Source"

    def test_extend_with_reference_explain_json(self, sample_source):
        """Test JSON explanation for Extend with reference."""
        extend = ExtendOperator(sample_source, "copy", Reference("item_name"))
        explanation = extend.explain_json()

        expr = explanation["parameters"]["expression"]
        assert expr["type"] == "Reference"
        assert expr["attribute"] == "item_name"

    def test_extend_with_function_explain_json(self, sample_source):
        """Test JSON explanation for Extend with function call."""
        uri_expr = FunctionCall(
            to_iri,
            [Reference("item_id"), Constant("http://example.org/")]
        )
        extend = ExtendOperator(sample_source, "subject", uri_expr)
        explanation = extend.explain_json()

        expr = explanation["parameters"]["expression"]
        assert expr["type"] == "FunctionCall"
        assert expr["function"] == "to_iri"
        assert len(expr["arguments"]) == 2

        # First argument is Reference
        assert expr["arguments"][0]["type"] == "Reference"
        assert expr["arguments"][0]["attribute"] == "item_id"

        # Second argument is Constant
        assert expr["arguments"][1]["type"] == "Constant"
        assert expr["arguments"][1]["value"] == "http://example.org/"

    def test_extend_with_iri_constant_explain_json(self, sample_source):
        """Test JSON explanation for Extend with IRI constant."""
        iri = FOAF_PERSON
        extend = ExtendOperator(sample_source, "rdf_type", Constant(iri))
        explanation = extend.explain_json()

        expr = explanation["parameters"]["expression"]
        assert expr["type"] == "Constant"
        assert expr["value_type"] == "IRI"
        assert expr["value"] == FOAF_PERSON.value

    def test_nested_extend_explain_json(self, sample_source):
        """Test JSON explanation for nested Extend operators."""
        extend1 = ExtendOperator(sample_source, "type", Constant("Item"))
        extend2 = ExtendOperator(extend1, "category", Constant("Product"))

        explanation = extend2.explain_json()

        assert explanation["type"] == "Extend"
        assert explanation["parameters"]["new_attribute"] == "category"

        # Parent should be another Extend
        parent = explanation["parent"]
        assert parent["type"] == "Extend"
        assert parent["parameters"]["new_attribute"] == "type"

        # Grandparent should be Source
        grandparent = parent["parent"]
        assert grandparent["type"] == "Source"

    def test_union_explain_json(self, sample_source):
        """Test JSON explanation for Union operator."""
        data2 = {"items": [{"id": 2, "name": "Test2"}]}
        source2 = JsonSourceOperator(
            source_data=data2,
            iterator_query="$.items[*]",
            attribute_mappings={"item_id": "$.id", "item_name": "$.name"}
        )

        union = UnionOperator([sample_source, source2])
        explanation = union.explain_json()

        assert explanation["type"] == "Union"
        assert explanation["parameters"]["operator_count"] == 2
        assert "children" in explanation
        assert len(explanation["children"]) == 2

        # Both children should be Sources
        assert all(child["type"] == "Source" for child in explanation["children"])

    def test_union_with_extends_explain_json(self, sample_source):
        """Test JSON explanation for Union with extended sources."""
        data2 = {"items": [{"id": 2, "name": "Test2"}]}
        source2 = JsonSourceOperator(
            source_data=data2,
            iterator_query="$.items[*]",
            attribute_mappings={"item_id": "$.id", "item_name": "$.name"}
        )

        extend1 = ExtendOperator(sample_source, "source", Constant("A"))
        extend2 = ExtendOperator(source2, "source", Constant("B"))

        union = UnionOperator([extend1, extend2])
        explanation = union.explain_json()

        assert explanation["type"] == "Union"
        assert len(explanation["children"]) == 2

        # Both children should be Extends
        assert all(child["type"] == "Extend" for child in explanation["children"])

        # Check source constants
        assert explanation["children"][0]["parameters"]["expression"]["value"] == "A"
        assert explanation["children"][1]["parameters"]["expression"]["value"] == "B"

    def test_complex_nested_pipeline_explain_json(self, sample_source):
        """Test JSON explanation for complex nested pipeline."""
        # Build: Union(Extend(Source), Extend(Extend(Source)))
        extend1 = ExtendOperator(sample_source, "type", Constant("TypeA"))

        data2 = {"items": [{"id": 2}]}
        source2 = JsonSourceOperator(
            source_data=data2,
            iterator_query="$.items[*]",
            attribute_mappings={"item_id": "$.id"}
        )
        extend2a = ExtendOperator(source2, "type", Constant("TypeB"))
        extend2b = ExtendOperator(extend2a, "category", Constant("Cat"))

        union = UnionOperator([extend1, extend2b])
        explanation = union.explain_json()

        # Verify structure
        assert explanation["type"] == "Union"
        assert len(explanation["children"]) == 2

        # First child: Extend -> Source
        child1 = explanation["children"][0]
        assert child1["type"] == "Extend"
        assert child1["parent"]["type"] == "Source"

        # Second child: Extend -> Extend -> Source
        child2 = explanation["children"][1]
        assert child2["type"] == "Extend"
        assert child2["parameters"]["new_attribute"] == "category"
        assert child2["parent"]["type"] == "Extend"
        assert child2["parent"]["parameters"]["new_attribute"] == "type"
        assert child2["parent"]["parent"]["type"] == "Source"

    def test_json_serialization_validity(self, sample_source):
        """Test that all explanations produce valid JSON."""
        extend = ExtendOperator(sample_source, "test", Constant("value"))
        union = UnionOperator([sample_source, extend])

        # Test all operator types
        for op in [sample_source, extend, union]:
            explanation = op.explain_json()

            # Should be serializable to JSON
            json_str = json.dumps(explanation, indent=2)

            # Should be parseable back
            parsed = json.loads(json_str)

            # Type should be preserved
            assert parsed["type"] == explanation["type"]
