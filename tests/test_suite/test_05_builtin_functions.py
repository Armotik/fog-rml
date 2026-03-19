"""
Test Suite for Built-in Functions

This module provides comprehensive unit tests for the built-in functions
library, validating RDF term construction, type conversions, and string
manipulation operations.
"""

import pytest
from pyhartig.functions.builtins import to_iri, to_literal, concat
from pyhartig.algebra.Terms import IRI, Literal
from pyhartig.algebra.Tuple import EPSILON
from pyhartig.namespaces import XSD_INTEGER, XSD_STRING


class TestBuiltinFunctions:
    """Test suite for built-in transformation functions."""

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
    # Tests for to_iri function
    # =========================================================================

    def test_to_iri_with_string(self, debug_logger):
        """
        Test to_iri function with string input.
        
        Validates conversion of string values to IRI terms.
        """
        debug_logger("Test: to_iri with String Input", 
                     "Objective: Convert string to IRI")
        
        # Test with full IRI string
        result = to_iri("http://example.org/resource")
        
        debug_logger("Test Case: Full IRI String", 
                     f"Input: 'http://example.org/resource'\n"
                     f"Output: {result}\n"
                     f"Type: {type(result)}")
        
        assert isinstance(result, IRI)
        assert result.value == "http://example.org/resource"
        
        debug_logger("Validation", "✓ String correctly converted to IRI")

    def test_to_iri_with_base_resolution(self, debug_logger):
        """
        Test to_iri function with base IRI resolution.
        
        Validates relative IRI resolution against a base IRI.
        """
        debug_logger("Test: to_iri with Base Resolution", 
                     "Objective: Resolve relative IRI against base")
        
        result = to_iri("resource123", "http://example.org/")
        
        debug_logger("Test Case: Relative IRI", 
                     f"Input: 'resource123'\n"
                     f"Base: 'http://example.org/'\n"
                     f"Output: {result}\n"
                     f"Resolved value: {result.value}")
        
        assert isinstance(result, IRI)
        assert result.value == "http://example.org/resource123"
        
        debug_logger("Validation", "✓ Relative IRI correctly resolved")

    def test_to_iri_with_literal_input(self, debug_logger):
        """
        Test to_iri function with Literal input.
        
        Validates extraction of lexical form from Literal for IRI construction.
        """
        debug_logger("Test: to_iri with Literal Input", 
                     "Objective: Extract lexical form from Literal")
        
        lit = Literal("http://example.org/item", XSD_STRING.value)
        result = to_iri(lit)
        
        debug_logger("Test Case: Literal to IRI", 
                     f"Input: {lit}\n"
                     f"Output: {result}\n"
                     f"Type: {type(result)}")
        
        assert isinstance(result, IRI)
        assert result.value == "http://example.org/item"
        
        debug_logger("Validation", "✓ Literal lexical form converted to IRI")

    def test_to_iri_with_none(self, debug_logger):
        """
        Test to_iri function with None input.
        
        Validates that None values return EPSILON.
        """
        debug_logger("Test: to_iri with None Input", 
                     "Objective: Handle None gracefully")
        
        result = to_iri(None)
        
        debug_logger("Test Case: None Input", 
                     f"Input: None\n"
                     f"Output: {result}\n"
                     f"Is EPSILON: {result == EPSILON}")
        
        assert result == EPSILON
        
        debug_logger("Validation", "✓ None correctly returns EPSILON")

    def test_to_iri_with_epsilon(self, debug_logger):
        """
        Test to_iri function with EPSILON input.
        
        Validates that EPSILON propagates through the function.
        """
        debug_logger("Test: to_iri with EPSILON Input", 
                     "Objective: Propagate EPSILON")
        
        result = to_iri(EPSILON)
        
        debug_logger("Test Case: EPSILON Input", 
                     f"Input: {EPSILON}\n"
                     f"Output: {result}\n"
                     f"Is EPSILON: {result == EPSILON}")
        
        assert result == EPSILON
        
        debug_logger("Validation", "✓ EPSILON correctly propagated")

    def test_to_iri_with_numeric_base(self, debug_logger):
        """
        Test to_iri function with numeric value and base.
        
        Validates conversion of numeric identifiers to IRIs.
        """
        debug_logger("Test: to_iri with Numeric Value", 
                     "Objective: Convert number to IRI with base")
        
        result = to_iri("42", "http://example.org/item/")
        
        debug_logger("Test Case: Numeric String", 
                     f"Input: '42'\n"
                     f"Base: 'http://example.org/item/'\n"
                     f"Output: {result}\n"
                     f"Resolved: {result.value}")
        
        assert isinstance(result, IRI)
        assert result.value == "http://example.org/item/42"
        
        debug_logger("Validation", "✓ Numeric identifier converted to IRI")

    # =========================================================================
    # Tests for to_literal function
    # =========================================================================

    def test_to_literal_with_string(self, debug_logger):
        """
        Test to_literal function with string input.
        
        Validates conversion of strings to typed literals.
        """
        debug_logger("Test: to_literal with String Input", 
                     "Objective: Convert string to typed literal")
        
        result = to_literal("Hello World", 
                           XSD_STRING.value)
        
        debug_logger("Test Case: String to xsd:string", 
                     f"Input: 'Hello World'\n"
                     f"Datatype: xsd:string\n"
                     f"Output: {result}\n"
                     f"Lexical form: {result.lexical_form}")
        
        assert isinstance(result, Literal)
        assert result.lexical_form == "Hello World"
        assert result.datatype_iri == XSD_STRING.value
        
        debug_logger("Validation", "✓ String converted to literal")

    def test_to_literal_with_integer(self, debug_logger):
        """
        Test to_literal function with integer input.
        
        Validates conversion of integers to typed literals.
        """
        debug_logger("Test: to_literal with Integer Input", 
                     "Objective: Convert integer to xsd:integer literal")
        
        result = to_literal(42, XSD_INTEGER.value)
        
        debug_logger("Test Case: Integer to xsd:integer", 
                     f"Input: 42\n"
                     f"Datatype: xsd:integer\n"
                     f"Output: {result}\n"
                     f"Lexical form: {result.lexical_form}")
        
        assert isinstance(result, Literal)
        assert result.lexical_form == "42"
        assert result.datatype_iri == XSD_INTEGER.value
        
        debug_logger("Validation", "✓ Integer converted to literal")

    def test_to_literal_with_existing_literal(self, debug_logger):
        """
        Test to_literal function with Literal input.
        
        Validates re-typing of existing literals.
        """
        debug_logger("Test: to_literal with Literal Input", 
                     "Objective: Re-type existing literal")
        
        original = Literal("123", XSD_STRING.value)
        result = to_literal(original, XSD_INTEGER.value)
        
        debug_logger("Test Case: Literal Re-typing", 
                     f"Input: {original}\n"
                     f"New datatype: xsd:integer\n"
                     f"Output: {result}\n"
                     f"New datatype: {result.datatype_iri}")
        
        assert isinstance(result, Literal)
        assert result.lexical_form == "123"
        assert result.datatype_iri == XSD_INTEGER.value
        
        debug_logger("Validation", "✓ Literal successfully re-typed")

    def test_to_literal_with_none(self, debug_logger):
        """
        Test to_literal function with None input.
        
        Validates that None values return EPSILON.
        """
        debug_logger("Test: to_literal with None Input", 
                     "Objective: Handle None gracefully")
        
        result = to_literal(None, XSD_STRING.value)
        
        debug_logger("Test Case: None Input", 
                     f"Input: None\n"
                     f"Output: {result}\n"
                     f"Is EPSILON: {result == EPSILON}")
        
        assert result == EPSILON
        
        debug_logger("Validation", "✓ None correctly returns EPSILON")

    def test_to_literal_with_epsilon(self, debug_logger):
        """
        Test to_literal function with EPSILON input.
        
        Validates that EPSILON propagates through the function.
        """
        debug_logger("Test: to_literal with EPSILON Input", 
                     "Objective: Propagate EPSILON")
        
        result = to_literal(EPSILON, XSD_STRING.value)
        
        debug_logger("Test Case: EPSILON Input", 
                     f"Input: {EPSILON}\n"
                     f"Output: {result}\n"
                     f"Is EPSILON: {result == EPSILON}")
        
        assert result == EPSILON
        
        debug_logger("Validation", "✓ EPSILON correctly propagated")

    # =========================================================================
    # Tests for concat function
    # =========================================================================

    def test_concat_two_strings(self, debug_logger):
        """
        Test concat function with two string inputs.
        
        Validates basic string concatenation.
        """
        debug_logger("Test: concat with Two Strings", 
                     "Objective: Concatenate two string values")
        
        result = concat("Hello", "World")
        
        debug_logger("Test Case: String Concatenation", 
                     f"Input 1: 'Hello'\n"
                     f"Input 2: 'World'\n"
                     f"Output: {result}\n"
                     f"Lexical form: {result.lexical_form}")
        
        assert isinstance(result, Literal)
        assert result.lexical_form == "HelloWorld"
        assert result.datatype_iri == XSD_STRING.value
        
        debug_logger("Validation", "✓ Strings successfully concatenated")

    def test_concat_with_literals(self, debug_logger):
        """
        Test concat function with Literal inputs.
        
        Validates concatenation of RDF literals.
        """
        debug_logger("Test: concat with Literal Inputs", 
                     "Objective: Concatenate literal values")
        
        lit1 = Literal("Alice", XSD_STRING.value)
        lit2 = Literal(" Smith", XSD_STRING.value)
        result = concat(lit1, lit2)
        
        debug_logger("Test Case: Literal Concatenation", 
                     f"Input 1: {lit1}\n"
                     f"Input 2: {lit2}\n"
                     f"Output: {result}\n"
                     f"Result: {result.lexical_form}")
        
        assert isinstance(result, Literal)
        assert result.lexical_form == "Alice Smith"
        
        debug_logger("Validation", "✓ Literals successfully concatenated")

    def test_concat_with_empty_string(self, debug_logger):
        """
        Test concat function with empty string.
        
        Validates handling of empty string concatenation.
        """
        debug_logger("Test: concat with Empty String", 
                     "Objective: Handle empty string concatenation")
        
        result = concat("Hello", "")
        
        debug_logger("Test Case: Empty String", 
                     f"Input 1: 'Hello'\n"
                     f"Input 2: ''\n"
                     f"Output: {result}\n"
                     f"Result: {result.lexical_form}")
        
        assert isinstance(result, Literal)
        assert result.lexical_form == "Hello"
        
        debug_logger("Validation", "✓ Empty string handled correctly")

    def test_concat_with_none(self, debug_logger):
        """
        Test concat function with None input.
        
        Validates that None in concatenation returns EPSILON.
        """
        debug_logger("Test: concat with None Input", 
                     "Objective: Handle None in concatenation")
        
        result = concat("Hello", None)
        
        debug_logger("Test Case: None in Concatenation", 
                     f"Input 1: 'Hello'\n"
                     f"Input 2: None\n"
                     f"Output: {result}\n"
                     f"Is EPSILON: {result == EPSILON}")
        
        # concat requires both values to be convertible to strings
        # None cannot be converted, so should return EPSILON
        assert result == EPSILON
        
        debug_logger("Validation", "✓ None correctly returns EPSILON")

    def test_concat_with_epsilon(self, debug_logger):
        """
        Test concat function with EPSILON input.
        
        Validates that EPSILON in concatenation returns EPSILON.
        """
        debug_logger("Test: concat with EPSILON Input", 
                     "Objective: Propagate EPSILON in concatenation")
        
        result = concat("Hello", EPSILON)
        
        debug_logger("Test Case: EPSILON in Concatenation", 
                     f"Input 1: 'Hello'\n"
                     f"Input 2: {EPSILON}\n"
                     f"Output: {result}\n"
                     f"Is EPSILON: {result == EPSILON}")
        
        assert result == EPSILON
        
        debug_logger("Validation", "✓ EPSILON correctly propagated")

    def test_concat_chain(self, debug_logger):
        """
        Test chaining multiple concat operations.
        
        Validates that concat results can be used in subsequent
        concatenations.
        """
        debug_logger("Test: Chained Concatenation", 
                     "Objective: Chain multiple concat operations")
        
        result1 = concat("Hello", " ")
        result2 = concat(result1, "World")
        
        debug_logger("Test Case: Concat Chain", 
                     f"Step 1: concat('Hello', ' ') → {result1}\n"
                     f"Step 2: concat(result1, 'World') → {result2}\n"
                     f"Final result: {result2.lexical_form}")
        
        assert isinstance(result2, Literal)
        assert result2.lexical_form == "Hello World"
        
        debug_logger("Validation", "✓ Chained concatenation successful")

    def test_function_integration(self, debug_logger):
        """
        Test integration of multiple built-in functions.
        
        Validates that functions can be composed to create
        complex transformations.
        """
        debug_logger("Test: Function Integration", 
                     "Objective: Compose multiple built-in functions")
        
        # Concatenate strings
        name = concat("John", " Doe")
        
        # Convert to literal with explicit type
        name_literal = to_literal(name, 
                                  XSD_STRING.value)
        
        # Create IRI from concatenated value
        # Note: to_iri expects string-like input, literal provides lexical_form
        iri = to_iri(name_literal, "http://example.org/person/")
        
        debug_logger("Test Case: Function Composition", 
                     f"Step 1: concat('John', ' Doe') → {name}\n"
                     f"Step 2: to_literal(name, xsd:string) → {name_literal}\n"
                     f"Step 3: to_iri(name_literal, base) → {iri}\n"
                     f"Final IRI: {iri.value}")
        
        assert isinstance(iri, IRI)
        assert "John%20Doe" in iri.value
        
        debug_logger("Validation", 
                     "✓ Functions successfully composed\n"
                     f"✓ Final IRI: {iri}")
