"""
Test Suite for Project Operator

This module provides comprehensive unit tests for the ProjectOperator,
which restricts a mapping relation to a specified subset of attributes.
Based on Definition 11 of the relational algebra for mapping relations.

Project^P(r) : (A, I) -> (P, I')
- r = (A, I) : Source mapping relation with attributes A and instance I
- P ⊆ A : Non-empty subset of attributes to retain
- Result : New mapping relation (P, I') where I' = { t[P] | t ∈ I }
"""

import pytest
from pyhartig.operators.ProjectOperator import ProjectOperator
from pyhartig.operators.sources.JsonSourceOperator import JsonSourceOperator
from pyhartig.operators.ExtendOperator import ExtendOperator
from pyhartig.operators.UnionOperator import UnionOperator
from pyhartig.expressions.Constant import Constant
from pyhartig.expressions.Reference import Reference
from pyhartig.expressions.FunctionCall import FunctionCall
from pyhartig.functions.builtins import to_iri, concat
from pyhartig.algebra.Terms import IRI
from pyhartig.namespaces import FOAF_NAME


class TestProjectOperator:
    """Test suite for the Project operator."""

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
    def sample_dataset(self):
        """
        Fixture providing sample JSON data for testing.

        Returns:
            dict: JSON structure with person information
        """
        return {
            "team": [
                {"id": 1, "name": "Alice", "department": "Engineering", "salary": 75000},
                {"id": 2, "name": "Bob", "department": "Marketing", "salary": 65000},
                {"id": 3, "name": "Charlie", "department": "Engineering", "salary": 80000}
            ]
        }

    @pytest.fixture
    def source_operator(self, sample_dataset):
        """
        Fixture providing a configured JsonSourceOperator.

        Returns:
            JsonSourceOperator: Configured source operator
        """
        return JsonSourceOperator(
            source_data=sample_dataset,
            iterator_query="$.team[*]",
            attribute_mappings={
                "person_id": "$.id",
                "person_name": "$.name",
                "dept": "$.department",
                "salary": "$.salary"
            }
        )

    # ========================================================================
    # Basic Projection Tests
    # ========================================================================

    def test_project_single_attribute(self, source_operator, debug_logger):
        """
        Test projection to a single attribute.

        Project^{person_name}(r) should return tuples with only 'person_name'.
        """
        debug_logger("Test: Project Single Attribute",
                     "Objective: Project to a single attribute")

        project = ProjectOperator(source_operator, {"person_name"})
        result = project.execute()

        assert len(result) == 3
        for row in result:
            assert set(row.keys()) == {"person_name"}
            assert "person_id" not in row
            assert "dept" not in row
            assert "salary" not in row

        # Verify values are preserved
        names = [row["person_name"] for row in result]
        assert "Alice" in names
        assert "Bob" in names
        assert "Charlie" in names

    def test_project_multiple_attributes(self, source_operator, debug_logger):
        """
        Test projection to multiple attributes.

        Project^{person_id, person_name}(r) should return tuples with only those attributes.
        """
        debug_logger("Test: Project Multiple Attributes",
                     "Objective: Project to subset of attributes {person_id, person_name}")

        project = ProjectOperator(source_operator, {"person_id", "person_name"})
        result = project.execute()

        assert len(result) == 3
        for row in result:
            assert set(row.keys()) == {"person_id", "person_name"}
            assert "dept" not in row
            assert "salary" not in row

    def test_project_all_attributes(self, source_operator, debug_logger):
        """
        Test projection with all attributes (identity projection).

        Project^A(r) where P = A should return tuples unchanged.
        """
        debug_logger("Test: Project All Attributes",
                     "Objective: Identity projection with P = A")

        all_attrs = {"person_id", "person_name", "dept", "salary"}
        project = ProjectOperator(source_operator, all_attrs)
        result = project.execute()

        assert len(result) == 3
        for row in result:
            assert set(row.keys()) == all_attrs

    def test_project_preserves_values(self, source_operator, debug_logger):
        """
        Test that projection preserves attribute values correctly.

        For each attribute a ∈ P: t[P](a) = t(a)
        """
        debug_logger("Test: Project Preserves Values",
                     "Objective: Verify t[P](a) = t(a) for all a ∈ P")

        project = ProjectOperator(source_operator, {"person_id", "dept"})
        result = project.execute()

        # Find Alice's tuple and verify values
        alice_tuple = next(row for row in result if row["person_id"] == 1)
        assert alice_tuple["dept"] == "Engineering"

        # Find Bob's tuple and verify values
        bob_tuple = next(row for row in result if row["person_id"] == 2)
        assert bob_tuple["dept"] == "Marketing"

    # ========================================================================
    # Strict Mode Tests - Missing Attribute Validation
    # ========================================================================

    def test_project_missing_attribute_raises_error(self, source_operator, debug_logger):
        """
        Test that projecting a non-existent attribute raises KeyError.

        Strict mode: P ⊆ A is enforced. If an attribute in P is not in A,
        a KeyError must be raised.
        """
        debug_logger("Test: Missing Attribute Raises Error",
                     "Objective: Verify strict mode raises KeyError for P ⊄ A")

        project = ProjectOperator(source_operator, {"person_name", "nonexistent_attr"})

        with pytest.raises(KeyError) as exc_info:
            project.execute()

        error_message = str(exc_info.value)
        assert "nonexistent_attr" in error_message
        assert "not found in tuple" in error_message

    def test_project_multiple_missing_attributes_raises_error(self, source_operator, debug_logger):
        """
        Test that projecting multiple non-existent attributes raises KeyError.

        All missing attributes should be reported in the error message.
        """
        debug_logger("Test: Multiple Missing Attributes",
                     "Objective: Verify all missing attributes are reported")

        project = ProjectOperator(source_operator, {"person_name", "missing1", "missing2"})

        with pytest.raises(KeyError) as exc_info:
            project.execute()

        error_message = str(exc_info.value)
        assert "missing1" in error_message or "missing2" in error_message

    def test_project_only_missing_attributes_raises_error(self, source_operator, debug_logger):
        """
        Test projection with only non-existent attributes raises KeyError.
        """
        debug_logger("Test: Only Missing Attributes",
                     "Objective: Verify error when P ∩ A = ∅")

        project = ProjectOperator(source_operator, {"fake1", "fake2"})

        with pytest.raises(KeyError):
            project.execute()

    # ========================================================================
    # Empty Result Handling
    # ========================================================================

    def test_project_empty_source(self, debug_logger):
        """
        Test projection on empty source returns empty result.

        Project^P(∅) = ∅
        """
        debug_logger("Test: Project Empty Source",
                     "Objective: Verify projection of empty relation")

        empty_data = {"team": []}
        source = JsonSourceOperator(
            source_data=empty_data,
            iterator_query="$.team[*]",
            attribute_mappings={"id": "$.id", "name": "$.name"}
        )

        project = ProjectOperator(source, {"id"})
        result = project.execute()

        assert len(result) == 0

    # ========================================================================
    # Operator Composition Tests
    # ========================================================================

    def test_project_after_extend(self, source_operator, debug_logger):
        """
        Test projection after extend operation.

        Extend then Project: Project^{new_attr}(Extend(r))
        """
        debug_logger("Test: Project After Extend",
                     "Objective: Compose Extend then Project")

        # Extend with a new attribute
        extend = ExtendOperator(
            parent_operator=source_operator,
            new_attribute="full_info",
            expression=FunctionCall(concat, [
                Reference("person_name"),
                Constant(" - "),
                Reference("dept")
            ])
        )

        # Project only the new attribute and id
        project = ProjectOperator(extend, {"person_id", "full_info"})
        result = project.execute()

        assert len(result) == 3
        for row in result:
            assert set(row.keys()) == {"person_id", "full_info"}
            assert "person_name" not in row
            assert "dept" not in row
            assert "salary" not in row

        # Verify the computed value (concat returns a Literal)
        alice = next(row for row in result if row["person_id"] == 1)
        assert alice["full_info"].lexical_form == "Alice - Engineering"

    def test_project_before_extend(self, source_operator, debug_logger):
        """
        Test extend after projection.

        Project then Extend: Extend(Project^P(r))
        """
        debug_logger("Test: Extend After Project",
                     "Objective: Compose Project then Extend")

        # Project first
        project = ProjectOperator(source_operator, {"person_id", "person_name"})

        # Extend on projected result
        extend = ExtendOperator(
            parent_operator=project,
            new_attribute="greeting",
            expression=FunctionCall(concat, [
                Constant("Hello, "),
                Reference("person_name")
            ])
        )

        result = extend.execute()

        assert len(result) == 3
        for row in result:
            assert "greeting" in row
            assert "person_id" in row
            assert "person_name" in row
            # These should not be present (were projected out)
            assert "dept" not in row
            assert "salary" not in row

    def test_project_after_union(self, debug_logger):
        """
        Test projection after union operation.

        Project^P(Union(r1, r2))
        """
        debug_logger("Test: Project After Union",
                     "Objective: Compose Union then Project")

        data_a = {"items": [{"id": 1, "name": "A", "type": "alpha"}]}
        data_b = {"items": [{"id": 2, "name": "B", "type": "beta"}]}

        source_a = JsonSourceOperator(
            source_data=data_a,
            iterator_query="$.items[*]",
            attribute_mappings={"item_id": "$.id", "item_name": "$.name", "item_type": "$.type"}
        )

        source_b = JsonSourceOperator(
            source_data=data_b,
            iterator_query="$.items[*]",
            attribute_mappings={"item_id": "$.id", "item_name": "$.name", "item_type": "$.type"}
        )

        union = UnionOperator([source_a, source_b])
        project = ProjectOperator(union, {"item_id", "item_name"})
        result = project.execute()

        assert len(result) == 2
        for row in result:
            assert set(row.keys()) == {"item_id", "item_name"}
            assert "item_type" not in row

    def test_chained_projections(self, source_operator, debug_logger):
        """
        Test multiple chained projections.

        Project^{a}(Project^{a, b}(r))
        """
        debug_logger("Test: Chained Projections",
                     "Objective: Multiple sequential projections")

        project1 = ProjectOperator(source_operator, {"person_id", "person_name", "dept"})
        project2 = ProjectOperator(project1, {"person_id", "person_name"})
        project3 = ProjectOperator(project2, {"person_name"})

        result = project3.execute()

        assert len(result) == 3
        for row in result:
            assert set(row.keys()) == {"person_name"}

    # ========================================================================
    # Explain Tests
    # ========================================================================

    def test_project_explain(self, source_operator, debug_logger):
        """
        Test human-readable explanation output.
        """
        debug_logger("Test: Project Explain",
                     "Objective: Verify explain() output format")

        project = ProjectOperator(source_operator, {"person_id", "person_name"})
        explanation = project.explain()

        assert "Project(" in explanation
        assert "attributes:" in explanation
        assert "person_id" in explanation
        assert "person_name" in explanation
        assert "parent:" in explanation

    def test_project_explain_json(self, source_operator, debug_logger):
        """
        Test JSON explanation output.
        """
        debug_logger("Test: Project Explain JSON",
                     "Objective: Verify explain_json() output structure")

        project = ProjectOperator(source_operator, {"person_id", "person_name"})
        json_output = project.explain_json()

        assert json_output["type"] == "Project"
        assert "attributes" in json_output["parameters"]
        assert "person_id" in json_output["parameters"]["attributes"]
        assert "person_name" in json_output["parameters"]["attributes"]
        assert "parent" in json_output
        assert json_output["parent"]["type"] == "Source"

    def test_project_explain_nested(self, source_operator, debug_logger):
        """
        Test explanation with nested operators.
        """
        debug_logger("Test: Nested Project Explain",
                     "Objective: Verify nested operator explanation")

        extend = ExtendOperator(
            parent_operator=source_operator,
            new_attribute="subject",
            expression=FunctionCall(to_iri, [Reference("person_id"), Constant("http://example.org/")])
        )
        project = ProjectOperator(extend, {"subject", "person_name"})

        explanation = project.explain()
        json_output = project.explain_json()

        # Text explanation should show hierarchy
        assert "Project(" in explanation
        assert "Extend(" in explanation

        # JSON should have nested structure
        assert json_output["type"] == "Project"
        assert json_output["parent"]["type"] == "Extend"
        assert json_output["parent"]["parent"]["type"] == "Source"

    # ========================================================================
    # Edge Cases
    # ========================================================================

    def test_project_with_iri_values(self, source_operator, debug_logger):
        """
        Test projection preserves IRI term values.
        """
        debug_logger("Test: Project with IRI Values",
                     "Objective: Verify IRI terms are preserved in projection")

        extend = ExtendOperator(
            parent_operator=source_operator,
            new_attribute="subject",
            expression=FunctionCall(to_iri, [Reference("person_id"), Constant("http://example.org/person/")])
        )

        project = ProjectOperator(extend, {"subject"})
        result = project.execute()

        assert len(result) == 3
        for row in result:
            assert set(row.keys()) == {"subject"}
            assert isinstance(row["subject"], IRI)

    def test_project_duplicate_tuples(self, debug_logger):
        """
        Test that projection can create duplicate tuples (bag semantics).

        If multiple tuples have the same values for projected attributes,
        they should all be preserved.
        """
        debug_logger("Test: Project Duplicate Tuples",
                     "Objective: Verify bag semantics - duplicates preserved")

        data = {
            "items": [
                {"id": 1, "name": "Item", "category": "A"},
                {"id": 2, "name": "Item", "category": "B"},
                {"id": 3, "name": "Item", "category": "A"}
            ]
        }

        source = JsonSourceOperator(
            source_data=data,
            iterator_query="$.items[*]",
            attribute_mappings={"item_id": "$.id", "item_name": "$.name", "category": "$.category"}
        )

        # Project only name - all tuples become "Item"
        project = ProjectOperator(source, {"item_name"})
        result = project.execute()

        # All 3 tuples should be preserved (bag semantics)
        assert len(result) == 3
        for row in result:
            assert row["item_name"] == "Item"

    def test_project_preserves_tuple_order(self, source_operator, debug_logger):
        """
        Test that projection preserves tuple order.
        """
        debug_logger("Test: Project Preserves Order",
                     "Objective: Verify tuple order is maintained")

        project = ProjectOperator(source_operator, {"person_id"})
        result = project.execute()

        ids = [row["person_id"] for row in result]
        assert ids == [1, 2, 3]


class TestProjectOperatorIntegration:
    """Integration tests for Project operator in complex pipelines."""

    def test_rdf_triple_generation_with_project(self):
        """
        Test RDF triple generation pipeline using Project to select final attributes.
        """
        data = {
            "persons": [
                {"id": "p1", "name": "Alice", "email": "alice@example.org"},
                {"id": "p2", "name": "Bob", "email": "bob@example.org"}
            ]
        }

        source = JsonSourceOperator(
            source_data=data,
            iterator_query="$.persons[*]",
            attribute_mappings={
                "person_id": "$.id",
                "person_name": "$.name",
                "email": "$.email"
            }
        )

        # Generate subject IRI
        extend_subject = ExtendOperator(
            parent_operator=source,
            new_attribute="subject",
            expression=FunctionCall(to_iri, [Reference("person_id"), Constant("http://example.org/person/")])
        )

        # Generate predicate IRI
        extend_predicate = ExtendOperator(
            parent_operator=extend_subject,
            new_attribute="predicate",
            expression=Constant(FOAF_NAME)
        )

        # Keep only S, P, O attributes for triple
        project = ProjectOperator(extend_predicate, {"subject", "predicate", "person_name"})
        result = project.execute()

        assert len(result) == 2
        for row in result:
            assert set(row.keys()) == {"subject", "predicate", "person_name"}
            assert isinstance(row["subject"], IRI)
            assert isinstance(row["predicate"], IRI)

    def test_heterogeneous_schema_with_union_and_project(self):
        """
        Test handling heterogeneous schemas using Union + multiple Projects.

        This demonstrates the recommended approach for handling different schemas:
        project each source to common attributes before union.
        """
        # Source A has: id, name, dept
        data_a = {"items": [{"id": 1, "name": "Alice", "dept": "Eng"}]}

        # Source B has: id, name, role (different schema)
        data_b = {"items": [{"id": 2, "name": "Bob", "role": "Manager"}]}

        source_a = JsonSourceOperator(
            source_data=data_a,
            iterator_query="$.items[*]",
            attribute_mappings={"item_id": "$.id", "item_name": "$.name", "dept": "$.dept"}
        )

        source_b = JsonSourceOperator(
            source_data=data_b,
            iterator_query="$.items[*]",
            attribute_mappings={"item_id": "$.id", "item_name": "$.name", "role": "$.role"}
        )

        # Project each to common schema before union
        project_a = ProjectOperator(source_a, {"item_id", "item_name"})
        project_b = ProjectOperator(source_b, {"item_id", "item_name"})

        union = UnionOperator([project_a, project_b])
        result = union.execute()

        assert len(result) == 2
        for row in result:
            assert set(row.keys()) == {"item_id", "item_name"}
            # Schema-specific attributes should not be present
            assert "dept" not in row
            assert "role" not in row
