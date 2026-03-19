"""
Test Suite for Expression System

This module provides comprehensive unit tests for the expression system,
including constants, references, and function calls. Tests validate
the expression evaluation mechanism and composition patterns.
"""

import pytest
from pyhartig.expressions.Constant import Constant
from pyhartig.expressions.Reference import Reference
from pyhartig.expressions.FunctionCall import FunctionCall
from pyhartig.algebra.Tuple import MappingTuple, EPSILON
from pyhartig.algebra.Terms import IRI, Literal
from pyhartig.functions.builtins import to_iri, to_literal, concat
from pyhartig.namespaces import FOAF_PERSON, XSD_STRING


class TestExpressionSystem:
    """Test suite for the algebraic expression system."""

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

    @pytest.fixture
    def sample_tuple(self):
        """
        Fixture providing a sample mapping tuple for testing.
        
        Returns:
            MappingTuple: Sample tuple with test data
        """
        return MappingTuple({
            "id": "123",
            "name": "Alice",
            "age": 30,
            "department": "Engineering"
        })

    # =========================================================================
    # Tests for Constant expression
    # =========================================================================

    def test_constant_string(self, sample_tuple, debug_logger):
        """
        Test Constant expression with string value.
        
        Validates that constant expressions return their value
        regardless of input tuple.
        """
        debug_logger("Test: Constant Expression with String", 
                     "Objective: Validate constant value return")
        
        const_expr = Constant("FixedValue")
        result = const_expr.evaluate(sample_tuple)
        
        debug_logger("Test Case: String Constant", 
                     f"Expression: {const_expr}\n"
                     f"Input tuple: {sample_tuple}\n"
                     f"Result: {result}")
        
        assert result == "FixedValue"
        
        debug_logger("Validation", "✓ Constant correctly returns fixed value")

    def test_constant_iri(self, sample_tuple, debug_logger):
        """
        Test Constant expression with IRI value.
        
        Validates that RDF terms can be used as constants.
        """
        debug_logger("Test: Constant Expression with IRI", 
                     "Objective: Validate RDF term constants")
        
        iri_value = FOAF_PERSON
        const_expr = Constant(iri_value)
        result = const_expr.evaluate(sample_tuple)
        
        debug_logger("Test Case: IRI Constant", 
                     f"Expression: {const_expr}\n"
                     f"IRI value: {iri_value}\n"
                     f"Result: {result}")
        
        assert result == iri_value
        assert isinstance(result, IRI)
        
        debug_logger("Validation", "✓ IRI constant correctly preserved")

    def test_constant_numeric(self, sample_tuple, debug_logger):
        """
        Test Constant expression with numeric value.
        
        Validates that numeric constants work correctly.
        """
        debug_logger("Test: Constant Expression with Number", 
                     "Objective: Validate numeric constants")
        
        const_expr = Constant(42)
        result = const_expr.evaluate(sample_tuple)
        
        debug_logger("Test Case: Numeric Constant", 
                     f"Expression: {const_expr}\n"
                     f"Result: {result}\n"
                     f"Type: {type(result)}")
        
        assert result == 42
        assert isinstance(result, int)
        
        debug_logger("Validation", "✓ Numeric constant correctly returned")

    def test_constant_independence(self, debug_logger):
        """
        Test that constants are independent of tuple content.
        
        Validates that constant values don't change based on
        different input tuples.
        """
        debug_logger("Test: Constant Independence", 
                     "Objective: Verify constants ignore tuple content")
        
        const_expr = Constant("Unchanged")
        
        tuple1 = MappingTuple({"a": 1, "b": 2})
        tuple2 = MappingTuple({"x": 10, "y": 20})
        
        result1 = const_expr.evaluate(tuple1)
        result2 = const_expr.evaluate(tuple2)
        
        debug_logger("Test Case: Different Tuples", 
                     f"Constant: {const_expr}\n"
                     f"Tuple 1: {tuple1}\n"
                     f"Result 1: {result1}\n"
                     f"Tuple 2: {tuple2}\n"
                     f"Result 2: {result2}")
        
        assert result1 == result2 == "Unchanged"
        
        debug_logger("Validation", "✓ Constant value independent of tuple")

    # =========================================================================
    # Tests for Reference expression
    # =========================================================================

    def test_reference_existing_attribute(self, sample_tuple, debug_logger):
        """
        Test Reference expression with existing attribute.
        
        Validates that references correctly retrieve values from tuples.
        """
        debug_logger("Test: Reference to Existing Attribute", 
                     "Objective: Retrieve attribute value from tuple")
        
        ref_expr = Reference("name")
        result = ref_expr.evaluate(sample_tuple)
        
        debug_logger("Test Case: Existing Attribute", 
                     f"Expression: {ref_expr}\n"
                     f"Input tuple: {sample_tuple}\n"
                     f"Referenced attribute: 'name'\n"
                     f"Result: {result}")
        
        assert result == "Alice"
        
        debug_logger("Validation", "✓ Reference correctly retrieved attribute")

    def test_reference_nonexistent_attribute(self, sample_tuple, debug_logger):
        """
        Test Reference expression with non-existent attribute.
        
        Validates that references to missing attributes return EPSILON.
        """
        debug_logger("Test: Reference to Non-existent Attribute", 
                     "Objective: Handle missing attributes gracefully")
        
        ref_expr = Reference("nonexistent")
        result = ref_expr.evaluate(sample_tuple)
        
        debug_logger("Test Case: Missing Attribute", 
                     f"Expression: {ref_expr}\n"
                     f"Input tuple: {sample_tuple}\n"
                     f"Referenced attribute: 'nonexistent'\n"
                     f"Result: {result}\n"
                     f"Is EPSILON: {result == EPSILON}")
        
        assert result == EPSILON
        
        debug_logger("Validation", "✓ Missing attribute returns EPSILON")

    def test_reference_multiple_attributes(self, sample_tuple, debug_logger):
        """
        Test multiple Reference expressions on same tuple.
        
        Validates that different references can access different
        attributes independently.
        """
        debug_logger("Test: Multiple References", 
                     "Objective: Access multiple attributes independently")
        
        ref_name = Reference("name")
        ref_age = Reference("age")
        ref_dept = Reference("department")
        
        result_name = ref_name.evaluate(sample_tuple)
        result_age = ref_age.evaluate(sample_tuple)
        result_dept = ref_dept.evaluate(sample_tuple)
        
        debug_logger("Test Case: Multiple Attributes", 
                     f"Input tuple: {sample_tuple}\n"
                     f"Reference 'name': {result_name}\n"
                     f"Reference 'age': {result_age}\n"
                     f"Reference 'department': {result_dept}")
        
        assert result_name == "Alice"
        assert result_age == 30
        assert result_dept == "Engineering"
        
        debug_logger("Validation", "✓ All references correctly resolved")

    # =========================================================================
    # Tests for FunctionCall expression
    # =========================================================================

    def test_function_call_with_constants(self, sample_tuple, debug_logger):
        """
        Test FunctionCall expression with constant arguments.
        
        Validates function evaluation with constant inputs.
        """
        debug_logger("Test: FunctionCall with Constants", 
                     "Objective: Evaluate function with constant arguments")
        
        func_expr = FunctionCall(
            function=concat,
            arguments=[
                Constant("Hello"),
                Constant(" World")
            ]
        )
        result = func_expr.evaluate(sample_tuple)
        
        debug_logger("Test Case: Concat Constants", 
                     f"Expression: {func_expr}\n"
                     f"Arguments: Constant('Hello'), Constant(' World')\n"
                     f"Result: {result}\n"
                     f"Lexical form: {result.lexical_form}")
        
        assert isinstance(result, Literal)
        assert result.lexical_form == "Hello World"
        
        debug_logger("Validation", "✓ Function evaluated with constants")

    def test_function_call_with_references(self, sample_tuple, debug_logger):
        """
        Test FunctionCall expression with reference arguments.
        
        Validates function evaluation with dynamic tuple values.
        """
        debug_logger("Test: FunctionCall with References", 
                     "Objective: Evaluate function with tuple values")
        
        func_expr = FunctionCall(
            function=to_iri,
            arguments=[
                Reference("id"),
                Constant("http://example.org/person/")
            ]
        )
        result = func_expr.evaluate(sample_tuple)
        
        debug_logger("Test Case: to_iri with Reference", 
                     f"Expression: {func_expr}\n"
                     f"Input tuple: {sample_tuple}\n"
                     f"Result: {result}\n"
                     f"IRI value: {result.value}")
        
        assert isinstance(result, IRI)
        assert result.value == "http://example.org/person/123"
        
        debug_logger("Validation", "✓ Function evaluated with references")

    def test_function_call_mixed_arguments(self, sample_tuple, debug_logger):
        """
        Test FunctionCall with mixed constant and reference arguments.
        
        Validates function evaluation with heterogeneous argument types.
        """
        debug_logger("Test: FunctionCall with Mixed Arguments", 
                     "Objective: Evaluate with constants and references")
        
        func_expr = FunctionCall(
            function=concat,
            arguments=[
                Constant("Name: "),
                Reference("name"),
                Constant(" ("),
                Reference("department"),
                Constant(")")
            ]
        )
        result = func_expr.evaluate(sample_tuple)
        
        debug_logger("Test Case: Complex Concatenation", 
                     f"Expression: {func_expr}\n"
                     f"Input tuple: {sample_tuple}\n"
                     f"Result: {result}\n"
                     f"Value: {result.lexical_form}")
        
        assert isinstance(result, Literal)
        assert result.lexical_form == "Name: Alice (Engineering)"
        
        debug_logger("Validation", "✓ Mixed arguments evaluated correctly")

    def test_function_call_with_epsilon_argument(self, sample_tuple, debug_logger):
        """
        Test FunctionCall behavior when argument evaluates to EPSILON.
        
        Validates EPSILON propagation in function evaluation.
        """
        debug_logger("Test: FunctionCall with EPSILON Argument", 
                     "Objective: Validate EPSILON propagation")
        
        func_expr = FunctionCall(
            function=concat,
            arguments=[
                Reference("nonexistent"),  # Returns EPSILON
                Constant(" suffix")
            ]
        )
        result = func_expr.evaluate(sample_tuple)
        
        debug_logger("Test Case: EPSILON in Arguments", 
                     f"Expression: {func_expr}\n"
                     f"First argument references non-existent attribute\n"
                     f"Result: {result}\n"
                     f"Is EPSILON: {result == EPSILON}")
        
        assert result == EPSILON
        
        debug_logger("Validation", "✓ EPSILON correctly propagated")

    def test_nested_function_calls(self, sample_tuple, debug_logger):
        """
        Test nested FunctionCall expressions.
        
        Validates that function calls can be composed, with one
        function's output serving as another's input.
        """
        debug_logger("Test: Nested Function Calls", 
                     "Objective: Compose functions hierarchically")
        
        # Inner: concat name and department
        inner_expr = FunctionCall(
            function=concat,
            arguments=[
                Reference("name"),
                Constant("_"),
                Reference("department")
            ]
        )
        
        # Outer: convert result to literal
        outer_expr = FunctionCall(
            function=to_literal,
            arguments=[
                inner_expr,
                Constant(XSD_STRING.value)
            ]
        )
        
        result = outer_expr.evaluate(sample_tuple)
        
        debug_logger("Test Case: Two-Level Nesting", 
                     f"Inner expression: {inner_expr}\n"
                     f"Outer expression: {outer_expr}\n"
                     f"Input tuple: {sample_tuple}\n"
                     f"Result: {result}\n"
                     f"Lexical form: {result.lexical_form}")
        
        assert isinstance(result, Literal)
        assert result.lexical_form == "Alice_Engineering"
        
        debug_logger("Validation", "✓ Nested functions evaluated correctly")

    def test_complex_expression_composition(self, debug_logger):
        """
        Test complex expression composition patterns.
        
        Validates that arbitrary combinations of constants, references,
        and function calls work correctly.
        """
        debug_logger("Test: Complex Expression Composition", 
                     "Objective: Validate arbitrary expression composition")
        
        tuple_data = MappingTuple({
            "first": "John",
            "last": "Doe",
            "emp_id": "E12345"
        })
        
        # Build: to_iri(concat(first, "_", last), base)
        name_concat = FunctionCall(
            function=concat,
            arguments=[
                Reference("first"),
                Constant("_"),
                Reference("last")
            ]
        )
        
        iri_expr = FunctionCall(
            function=to_iri,
            arguments=[
                name_concat,
                Constant("http://company.org/employee/")
            ]
        )
        
        result = iri_expr.evaluate(tuple_data)
        
        debug_logger("Test Case: Concat-to-IRI Pipeline", 
                     f"Expression: to_iri(concat(first, '_', last), base)\n"
                     f"Input tuple: {tuple_data}\n"
                     f"Result: {result}\n"
                     f"IRI value: {result.value}")
        
        assert isinstance(result, IRI)
        assert "John_Doe" in result.value
        
        debug_logger("Validation", 
                     f"✓ Complex composition evaluated correctly\n"
                     f"✓ Generated IRI: {result}")

    def test_expression_error_handling(self, sample_tuple, debug_logger):
        """
        Test expression error handling.
        
        Validates that errors in function execution are handled
        gracefully, returning EPSILON.
        """
        debug_logger("Test: Expression Error Handling", 
                     "Objective: Graceful error handling in expressions")
        
        # Define a function that might raise an error
        def error_prone_func(x):
            if x is None:
                raise ValueError("Cannot process None")
            return x
        
        func_expr = FunctionCall(
            function=error_prone_func,
            arguments=[Reference("nonexistent")]  # Will be EPSILON
        )
        
        result = func_expr.evaluate(sample_tuple)
        
        debug_logger("Test Case: Error in Function", 
                     f"Function that raises error on certain inputs\n"
                     f"Argument: Reference('nonexistent') → EPSILON\n"
                     f"Result: {result}\n"
                     f"Is EPSILON: {result == EPSILON}")
        
        # Since argument is EPSILON, function should not be called
        assert result == EPSILON
        
        debug_logger("Validation", "✓ Errors handled gracefully")

    def test_expression_repr(self, debug_logger):
        """
        Test string representation of expressions.
        
        Validates that expressions have meaningful string representations
        for debugging purposes.
        """
        debug_logger("Test: Expression String Representation", 
                     "Objective: Validate __repr__ methods")
        
        const = Constant("value")
        ref = Reference("attr")
        func = FunctionCall(concat, [const, ref])
        
        const_repr = repr(const)
        ref_repr = repr(ref)
        func_repr = repr(func)
        
        debug_logger("Test Case: Expression Representations", 
                     f"Constant: {const_repr}\n"
                     f"Reference: {ref_repr}\n"
                     f"FunctionCall: {func_repr}")
        
        assert "Const" in const_repr
        assert "Ref" in ref_repr
        assert "concat" in func_repr or "Concat" in func_repr
        
        debug_logger("Validation", "✓ All expressions have meaningful repr")
