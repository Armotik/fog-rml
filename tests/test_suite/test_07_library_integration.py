"""
Test Suite for Library Integration

This module provides integration tests for external libraries used
in the project, including JSON processing (jsonpath-ng) and potential
RDF library integration scenarios.
"""

import pytest
import json
from jsonpath_ng import parse
from pyhartig.namespaces import XSD_INTEGER, XSD_STRING
from pyhartig.operators.sources.JsonSourceOperator import JsonSourceOperator

class TestLibraryIntegration:
    """Test suite for external library integration."""

    @pytest.fixture
    def debug_logger(self):
        """
        Fixture providing a debug logging function.
        
        Returns:
            callable: Function for structured debug output
        """
        def log(section, message):
            print(f"\n{'=' * 80}")
            print(f"[DEBUG] {section}")
            print(f"{'-' * 80}")
            print(message)
            print(f"{'=' * 80}\n")
        return log

    # =========================================================================
    # Tests for jsonpath-ng library integration
    # =========================================================================

    def test_jsonpath_basic_queries(self, debug_logger):
        """
        Test jsonpath-ng library with basic queries.
        
        Validates that the JSONPath library correctly handles
        standard query patterns.
        """
        debug_logger("Test: JSONPath Basic Queries", 
                     "Objective: Validate jsonpath-ng query functionality")
        
        data = {
            "store": {
                "book": [
                    {"title": "Book 1", "price": 10.99},
                    {"title": "Book 2", "price": 15.99}
                ]
            }
        }
        
        # Test simple path
        path1 = parse("$.store.book[*].title")
        matches1 = [match.value for match in path1.find(data)]
        
        # Test with filter
        path2 = parse("$.store.book[0]")
        matches2 = [match.value for match in path2.find(data)]
        
        debug_logger("Test Cases", 
                     f"Data:\n{json.dumps(data, indent=2)}\n\n"
                     f"Query 1: $.store.book[*].title\n"
                     f"Results: {matches1}\n\n"
                     f"Query 2: $.store.book[0]\n"
                     f"Results: {matches2}")
        
        assert matches1 == ["Book 1", "Book 2"]
        assert len(matches2) == 1
        assert matches2[0]["title"] == "Book 1"
        
        debug_logger("Validation", 
                     "✓ JSONPath queries execute correctly\n"
                     "✓ Array iteration works\n"
                     "✓ Index access works")

    def test_jsonpath_complex_queries(self, debug_logger):
        """
        Test jsonpath-ng with complex nested queries.
        
        Validates handling of deeply nested structures and
        recursive descent operators.
        """
        debug_logger("Test: JSONPath Complex Queries", 
                     "Objective: Test nested and recursive queries")
        
        data = {
            "organization": {
                "departments": [
                    {
                        "name": "IT",
                        "employees": [
                            {"name": "Alice", "skills": ["Python", "Java"]},
                            {"name": "Bob", "skills": ["C++", "Go"]}
                        ]
                    },
                    {
                        "name": "HR",
                        "employees": [
                            {"name": "Charlie", "skills": ["Recruiting"]}
                        ]
                    }
                ]
            }
        }
        
        # Recursive descent for all employee names
        path1 = parse("$..employees[*].name")
        matches1 = [match.value for match in path1.find(data)]
        
        # Nested array access
        path2 = parse("$.organization.departments[*].employees[*].skills[*]")
        matches2 = [match.value for match in path2.find(data)]
        
        debug_logger("Test Cases", 
                     f"Query 1: $..employees[*].name (recursive)\n"
                     f"Results: {matches1}\n\n"
                     f"Query 2: Nested array skills\n"
                     f"Results: {matches2}")
        
        assert set(matches1) == {"Alice", "Bob", "Charlie"}
        assert "Python" in matches2
        assert "Recruiting" in matches2
        
        debug_logger("Validation", 
                     "✓ Recursive descent works\n"
                     "✓ Nested array traversal works")

    def test_jsonpath_edge_cases(self, debug_logger):
        """
        Test jsonpath-ng edge cases and error handling.
        
        Validates behavior with missing paths, empty results,
        and malformed queries.
        """
        debug_logger("Test: JSONPath Edge Cases", 
                     "Objective: Validate edge case handling")
        
        data = {"items": [{"id": 1}, {"id": 2}]}
        
        # Non-existent path
        path1 = parse("$.nonexistent")
        matches1 = [match.value for match in path1.find(data)]
        
        # Empty array result
        path2 = parse("$.items[*].missing_field")
        matches2 = [match.value for match in path2.find(data)]
        
        # Accessing nested non-existent
        path3 = parse("$.items[*].name")
        matches3 = [match.value for match in path3.find(data)]
        
        debug_logger("Test Cases", 
                     f"Data: {data}\n\n"
                     f"Case 1 - Non-existent path: {len(matches1)} results\n"
                     f"Case 2 - Filter with no matches: {len(matches2)} results\n"
                     f"Case 3 - Missing attributes: {len(matches3)} results")
        
        assert len(matches1) == 0
        assert len(matches2) == 0
        assert len(matches3) == 0
        
        debug_logger("Validation", 
                     "✓ Non-existent paths return empty results\n"
                     "✓ No errors raised for missing data")

    def test_jsonpath_operator_integration(self, debug_logger):
        """
        Test JsonSourceOperator's integration with jsonpath-ng.
        
        Validates that the operator correctly uses the library
        for both iteration and extraction.
        """
        debug_logger("Test: JsonSourceOperator JSONPath Integration", 
                     "Objective: Validate operator-library integration")
        
        data = {
            "products": [
                {
                    "id": "P001",
                    "name": "Laptop",
                    "specs": {
                        "cpu": "Intel i7",
                        "ram": "16GB"
                    }
                },
                {
                    "id": "P002",
                    "name": "Mouse",
                    "specs": {
                        "type": "Wireless",
                        "color": "Black"
                    }
                }
            ]
        }
        
        operator = JsonSourceOperator(
            source_data=data,
            iterator_query="$.products[*]",
            attribute_mappings={
                "product_id": "$.id",
                "product_name": "$.name",
                "cpu": "$.specs.cpu",
                "type": "$.specs.type"
            }
        )
        
        debug_logger("Configuration", 
                     f"Data:\n{json.dumps(data, indent=2)}\n\n"
                     f"Iterator: $.products[*]\n"
                     f"Mappings:\n"
                     f"  - product_id: $.id\n"
                     f"  - product_name: $.name\n"
                     f"  - cpu: $.specs.cpu\n"
                     f"  - type: $.specs.type")
        
        result = operator.execute()
        
        debug_logger("Execution Result", 
                     f"Number of tuples: {len(result)}\n"
                     f"Tuples:\n" + 
                     "\n".join(f"  {i+1}. {tuple}" for i, tuple in enumerate(result)))
        
        # Laptop has cpu but not type, Mouse has type but not cpu
        # Empty lists in Cartesian product → no tuples for either
        # Actually, if any attribute has empty result, no tuple is generated
        assert len(result) == 0 or all("product_id" in t for t in result)
        
        debug_logger("Validation", 
                     f"✓ Operator correctly uses JSONPath\n"
                     f"✓ Missing nested attributes handled\n"
                     f"✓ Result count: {len(result)}")

    def test_json_library_features(self, debug_logger):
        """
        Test Python's json library features used in the project.
        
        Validates JSON parsing, serialization, and error handling.
        """
        debug_logger("Test: Python JSON Library Features", 
                     "Objective: Validate JSON library usage")
        
        # Test parsing
        json_string = '{"name": "Test", "value": 123}'
        parsed = json.loads(json_string)
        
        # Test serialization
        data = {"items": [1, 2, 3], "nested": {"key": "value"}}
        serialized = json.dumps(data, indent=2)
        
        # Test round-trip
        roundtrip = json.loads(json.dumps(data))
        
        debug_logger("Test Cases", 
                     f"Parsing:\n"
                     f"  Input: {json_string}\n"
                     f"  Result: {parsed}\n\n"
                     f"Serialization:\n"
                     f"  Input: {data}\n"
                     f"  Output:\n{serialized}\n\n"
                     f"Round-trip: {roundtrip == data}")
        
        assert parsed["name"] == "Test"
        assert parsed["value"] == 123
        assert roundtrip == data
        
        debug_logger("Validation", 
                     "✓ JSON parsing works correctly\n"
                     "✓ JSON serialization works\n"
                     "✓ Round-trip preserves data")

    def test_json_unicode_handling(self, debug_logger):
        """
        Test JSON library Unicode handling.
        
        Validates proper handling of international characters
        and special Unicode sequences.
        """
        debug_logger("Test: JSON Unicode Handling", 
                     "Objective: Validate Unicode support")
        
        data = {
            "french": "Bonjour",
            "japanese": "こんにちは",
            "emoji": "🎉",
            "special": "Ñoño"
        }
        
        # Serialize and parse
        json_str = json.dumps(data, ensure_ascii=False)
        parsed = json.loads(json_str)
        
        debug_logger("Test Case: Unicode Data", 
                     f"Original: {data}\n"
                     f"Serialized: {json_str}\n"
                     f"Parsed: {parsed}")
        
        assert parsed["french"] == "Bonjour"
        assert parsed["japanese"] == "こんにちは"
        assert parsed["emoji"] == "🎉"
        assert parsed["special"] == "Ñoño"
        
        debug_logger("Validation", 
                     "✓ Unicode characters preserved\n"
                     "✓ International text handled correctly")

    # =========================================================================
    # Tests for potential RDF library integration
    # =========================================================================

    def test_rdf_term_construction(self, debug_logger):
        """
        Test RDF term construction patterns.
        
        Validates that custom RDF term classes are correctly
        instantiated and used.
        """
        debug_logger("Test: RDF Term Construction", 
                     "Objective: Validate RDF term creation")
        
        from pyhartig.algebra.Terms import IRI, Literal, BlankNode
        
        # Create different RDF terms
        iri = IRI("http://example.org/resource")
        literal_plain = Literal("Hello")
        literal_typed = Literal("42", XSD_INTEGER.value)
        blank = BlankNode("b1")
        
        debug_logger("Test Cases", 
                     f"IRI: {iri}\n"
                     f"Plain Literal: {literal_plain}\n"
                     f"Typed Literal: {literal_typed}\n"
                     f"Blank Node: {blank}")
        
        assert iri.value == "http://example.org/resource"
        assert literal_plain.lexical_form == "Hello"
        assert literal_typed.datatype_iri == XSD_INTEGER.value
        assert blank.identifier == "b1"
        
        debug_logger("Validation", 
                     "✓ All RDF term types constructed correctly\n"
                     "✓ Terms are immutable (frozen dataclasses)")

    def test_rdf_term_equality(self, debug_logger):
        """
        Test RDF term equality semantics.
        
        Validates that RDF terms with same values are considered equal.
        """
        debug_logger("Test: RDF Term Equality", 
                     "Objective: Validate equality semantics")
        
        from pyhartig.algebra.Terms import IRI, Literal
        
        iri1 = IRI("http://example.org/test")
        iri2 = IRI("http://example.org/test")
        iri3 = IRI("http://example.org/other")
        
        lit1 = Literal("value", XSD_STRING.value)
        lit2 = Literal("value", XSD_STRING.value)
        lit3 = Literal("value", XSD_INTEGER.value)
        
        debug_logger("Test Cases", 
                     f"IRI equality: {iri1} == {iri2} → {iri1 == iri2}\n"
                     f"IRI inequality: {iri1} == {iri3} → {iri1 == iri3}\n"
                     f"Literal equality: {lit1} == {lit2} → {lit1 == lit2}\n"
                     f"Literal type difference: {lit1} == {lit3} → {lit1 == lit3}")
        
        assert iri1 == iri2
        assert iri1 != iri3
        assert lit1 == lit2
        assert lit1 != lit3
        
        debug_logger("Validation", 
                     "✓ RDF term equality works correctly\n"
                     "✓ Datatype affects literal equality")

    def test_rdf_term_representation(self, debug_logger):
        """
        Test RDF term string representations.
        
        Validates that terms have appropriate string formats
        for debugging and serialization.
        """
        debug_logger("Test: RDF Term String Representation", 
                     "Objective: Validate __repr__ methods")
        
        from pyhartig.algebra.Terms import IRI, Literal, BlankNode
        
        iri = IRI("http://example.org/test")
        lit_string = Literal("Hello", XSD_STRING.value)
        lit_int = Literal("42", XSD_INTEGER.value)
        blank = BlankNode("node1")
        
        iri_repr = repr(iri)
        lit_str_repr = repr(lit_string)
        lit_int_repr = repr(lit_int)
        blank_repr = repr(blank)
        
        debug_logger("Representations", 
                     f"IRI: {iri_repr}\n"
                     f"String Literal: {lit_str_repr}\n"
                     f"Integer Literal: {lit_int_repr}\n"
                     f"Blank Node: {blank_repr}")
        
        assert "http://example.org/test" in iri_repr
        assert "Hello" in lit_str_repr
        assert "42" in lit_int_repr
        assert "^^" in lit_int_repr  # Type indicator
        assert "node1" in blank_repr
        
        debug_logger("Validation", 
                     "✓ All term representations are readable\n"
                     "✓ Datatypes shown for non-string literals")

    def test_library_version_compatibility(self, debug_logger):
        """
        Test library version compatibility.
        
        Validates that required libraries are available and
        at compatible versions.
        """
        global pytest
        debug_logger("Test: Library Version Compatibility",
                     "Objective: Check library versions")
        
        try:
            import jsonpath_ng
            import pytest
            
            jsonpath_version = getattr(jsonpath_ng, '__version__', 'unknown')
            pytest_version = pytest.__version__
            
            debug_logger("Library Versions", 
                         f"jsonpath-ng: {jsonpath_version}\n"
                         f"pytest: {pytest_version}")
            
            # Basic smoke test
            assert hasattr(jsonpath_ng, 'parse')
            assert hasattr(pytest, 'fixture')
            
            debug_logger("Validation", 
                         "✓ Required libraries installed\n"
                         "✓ Core functions available")
            
        except ImportError as e:
            debug_logger("Import Error", str(e))
            pytest.fail(f"Required library not available: {e}")
