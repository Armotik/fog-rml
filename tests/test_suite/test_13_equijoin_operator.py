"""
Test Suite for EquiJoin Operator

This module provides comprehensive unit tests for the EquiJoinOperator,
which combines two mapping relations based on join conditions.
Based on Definition 12 of the relational algebra for mapping relations.

EqJoin^J(r₁, r₂) : Operator × Operator → Operator
- r₁ = (A₁, I₁) : Left mapping relation with attributes A₁ and instance I₁
- r₂ = (A₂, I₂) : Right mapping relation with attributes A₂ and instance I₂
- J ⊆ A₁ × A₂ : Set of join condition pairs
- Precondition: A₁ ∩ A₂ = ∅ (disjoint attribute sets)
- Result: New mapping relation (A, I) where:
    - A = A₁ ∪ A₂ (union of all attributes)
    - I = { t₁ ∪ t₂ | t₁ ∈ I₁, t₂ ∈ I₂, ∀(a₁, a₂) ∈ J : t₁(a₁) = t₂(a₂) }
"""

import pytest
from pyhartig.operators.EquiJoinOperator import EquiJoinOperator
from pyhartig.operators.sources.JsonSourceOperator import JsonSourceOperator
from pyhartig.operators.ExtendOperator import ExtendOperator
from pyhartig.operators.Operator import Operator
from pyhartig.algebra.Tuple import MappingTuple
from pyhartig.operators.UnionOperator import UnionOperator
from pyhartig.operators.ProjectOperator import ProjectOperator
from pyhartig.expressions.Constant import Constant
from pyhartig.expressions.Reference import Reference
from pyhartig.expressions.FunctionCall import FunctionCall
from pyhartig.functions.builtins import to_iri, concat
from pyhartig.algebra.Terms import IRI


class TestEquiJoinOperator:
    """Test suite for the EquiJoin operator."""

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
    def employees_dataset(self):
        """
        Fixture providing sample employee data for testing.

        Returns:
            dict: JSON structure with employee information
        """
        return {
            "employees": [
                {"emp_id": 1, "name": "Alice", "dept_id": 101},
                {"emp_id": 2, "name": "Bob", "dept_id": 102},
                {"emp_id": 3, "name": "Charlie", "dept_id": 101},
                {"emp_id": 4, "name": "Diana", "dept_id": 103}
            ]
        }

    @pytest.fixture
    def departments_dataset(self):
        """
        Fixture providing sample department data for testing.

        Returns:
            dict: JSON structure with department information
        """
        return {
            "departments": [
                {"department_id": 101, "dept_name": "Engineering"},
                {"department_id": 102, "dept_name": "Marketing"},
                {"department_id": 103, "dept_name": "HR"}
            ]
        }

    @pytest.fixture
    def employees_source(self, employees_dataset):
        """
        Fixture providing a configured JsonSourceOperator for employees.

        Returns:
            JsonSourceOperator: Configured source operator for employees
        """
        return JsonSourceOperator(
            source_data=employees_dataset,
            iterator_query="$.employees[*]",
            attribute_mappings={
                "emp_id": "$.emp_id",
                "emp_name": "$.name",
                "emp_dept_id": "$.dept_id"
            }
        )

    @pytest.fixture
    def departments_source(self, departments_dataset):
        """
        Fixture providing a configured JsonSourceOperator for departments.

        Returns:
            JsonSourceOperator: Configured source operator for departments
        """
        return JsonSourceOperator(
            source_data=departments_dataset,
            iterator_query="$.departments[*]",
            attribute_mappings={
                "dept_id": "$.department_id",
                "dept_name": "$.dept_name"
            }
        )

    # ========================================================================
    # Basic EquiJoin Tests
    # ========================================================================

    def test_equijoin_basic(self, employees_source, departments_source, debug_logger):
        """
        Test basic equijoin between employees and departments.

        EqJoin^{(emp_dept_id, dept_id)}(employees, departments)
        """
        debug_logger("Test: Basic EquiJoin",
                     "Objective: Join employees with departments on dept_id")

        equijoin = EquiJoinOperator(
            employees_source,
            departments_source,
            A=["emp_dept_id"],
            B=["dept_id"]
        )
        result = equijoin.execute()

        # Should have 4 results (all employees matched with their departments)
        assert len(result) == 4

        # Verify all tuples have attributes from both relations
        for row in result:
            assert "emp_id" in row
            assert "emp_name" in row
            assert "emp_dept_id" in row
            assert "dept_id" in row
            assert "dept_name" in row

        # Verify join correctness
        alice_row = next(r for r in result if r["emp_name"] == "Alice")
        assert alice_row["dept_name"] == "Engineering"

        bob_row = next(r for r in result if r["emp_name"] == "Bob")
        assert bob_row["dept_name"] == "Marketing"

    def test_equijoin_multiple_conditions(self, debug_logger):
        """
        Test equijoin with multiple join conditions.

        EqJoin^{(a1, b1), (a2, b2)}(r1, r2)
        """
        debug_logger("Test: Multiple Join Conditions",
                     "Objective: Join on multiple attribute pairs")

        left_data = {
            "records": [
                {"key1": "A", "key2": 1, "left_val": "L1"},
                {"key1": "B", "key2": 2, "left_val": "L2"},
                {"key1": "A", "key2": 2, "left_val": "L3"}
            ]
        }

        right_data = {
            "records": [
                {"k1": "A", "k2": 1, "right_val": "R1"},
                {"k1": "A", "k2": 2, "right_val": "R2"},
                {"k1": "B", "k2": 1, "right_val": "R3"}
            ]
        }

        left_source = JsonSourceOperator(
            source_data=left_data,
            iterator_query="$.records[*]",
            attribute_mappings={
                "key1": "$.key1",
                "key2": "$.key2",
                "left_val": "$.left_val"
            }
        )

        right_source = JsonSourceOperator(
            source_data=right_data,
            iterator_query="$.records[*]",
            attribute_mappings={
                "k1": "$.k1",
                "k2": "$.k2",
                "right_val": "$.right_val"
            }
        )

        equijoin = EquiJoinOperator(
            left_source,
            right_source,
            A=["key1", "key2"],
            B=["k1", "k2"]
        )
        result = equijoin.execute()

        # Only 2 matches: (A,1) and (A,2)
        assert len(result) == 2

        # Verify matches
        values = [(r["left_val"], r["right_val"]) for r in result]
        assert ("L1", "R1") in values  # (A, 1) match
        assert ("L3", "R2") in values  # (A, 2) match

    def test_equijoin_no_matches(self, debug_logger):
        """
        Test equijoin when no tuples satisfy the join condition.
        """
        debug_logger("Test: EquiJoin No Matches",
                     "Objective: Verify empty result when no join conditions are satisfied")

        left_data = {"items": [{"id": 1}, {"id": 2}]}
        right_data = {"items": [{"ref": 3}, {"ref": 4}]}

        left_source = JsonSourceOperator(
            source_data=left_data,
            iterator_query="$.items[*]",
            attribute_mappings={"left_id": "$.id"}
        )

        right_source = JsonSourceOperator(
            source_data=right_data,
            iterator_query="$.items[*]",
            attribute_mappings={"right_ref": "$.ref"}
        )

        equijoin = EquiJoinOperator(
            left_source,
            right_source,
            A=["left_id"],
            B=["right_ref"]
        )
        result = equijoin.execute()

        assert len(result) == 0

    def test_equijoin_cartesian_product_like(self, debug_logger):
        """
        Test equijoin where many tuples match (simulating cartesian product behavior).
        """
        debug_logger("Test: EquiJoin Many Matches",
                     "Objective: Verify correct multiplication of matching tuples")

        left_data = {
            "items": [
                {"group": "A", "left_val": 1},
                {"group": "A", "left_val": 2}
            ]
        }

        right_data = {
            "items": [
                {"grp": "A", "right_val": "X"},
                {"grp": "A", "right_val": "Y"},
                {"grp": "A", "right_val": "Z"}
            ]
        }

        left_source = JsonSourceOperator(
            source_data=left_data,
            iterator_query="$.items[*]",
            attribute_mappings={"group": "$.group", "left_val": "$.left_val"}
        )

        right_source = JsonSourceOperator(
            source_data=right_data,
            iterator_query="$.items[*]",
            attribute_mappings={"grp": "$.grp", "right_val": "$.right_val"}
        )

        equijoin = EquiJoinOperator(
            left_source,
            right_source,
            A=["group"],
            B=["grp"]
        )
        result = equijoin.execute()

        # 2 left tuples × 3 right tuples = 6 results
        assert len(result) == 6

    # ========================================================================
    # Precondition Tests
    # ========================================================================

    def test_equijoin_disjoint_attribute_violation(self, debug_logger):
        """
        Test that EquiJoin raises error when attribute sets are not disjoint.

        Precondition: A₁ ∩ A₂ = ∅
        """
        debug_logger("Test: Disjoint Attribute Violation",
                     "Objective: Verify error when A₁ ∩ A₂ ≠ ∅")

        left_data = {"items": [{"id": 1, "name": "A"}]}
        right_data = {"items": [{"id": 1, "value": "X"}]}

        left_source = JsonSourceOperator(
            source_data=left_data,
            iterator_query="$.items[*]",
            attribute_mappings={"id": "$.id", "name": "$.name"}
        )

        # Same attribute name 'id' - violates disjoint condition
        right_source = JsonSourceOperator(
            source_data=right_data,
            iterator_query="$.items[*]",
            attribute_mappings={"id": "$.id", "value": "$.value"}
        )

        equijoin = EquiJoinOperator(
            left_source,
            right_source,
            A=["id"],
            B=["id"]
        )

        with pytest.raises(ValueError, match="disjoint"):
            equijoin.execute()

    def test_equijoin_unequal_attribute_lists(self, debug_logger):
        """
        Test that EquiJoin raises error when join attribute lists have different lengths.
        """
        debug_logger("Test: Unequal Attribute Lists",
                     "Objective: Verify error when |A| ≠ |B|")

        left_data = {"items": [{"a": 1, "b": 2}]}
        right_data = {"items": [{"x": 1}]}

        left_source = JsonSourceOperator(
            source_data=left_data,
            iterator_query="$.items[*]",
            attribute_mappings={"a": "$.a", "b": "$.b"}
        )

        right_source = JsonSourceOperator(
            source_data=right_data,
            iterator_query="$.items[*]",
            attribute_mappings={"x": "$.x"}
        )

        with pytest.raises(ValueError, match="equal length"):
            EquiJoinOperator(
                left_source,
                right_source,
                A=["a", "b"],
                B=["x"]
            )

    # ========================================================================
    # Empty Relation Tests
    # ========================================================================

    def test_equijoin_empty_left_relation(self, departments_source, debug_logger):
        """
        Test equijoin with empty left relation.
        """
        debug_logger("Test: Empty Left Relation",
                     "Objective: Verify empty result when left relation is empty")

        empty_data = {"employees": []}
        empty_source = JsonSourceOperator(
            source_data=empty_data,
            iterator_query="$.employees[*]",
            attribute_mappings={"emp_id": "$.id"}
        )

        equijoin = EquiJoinOperator(
            empty_source,
            departments_source,
            A=["emp_id"],
            B=["dept_id"]
        )
        result = equijoin.execute()

        assert len(result) == 0

    def test_equijoin_empty_right_relation(self, employees_source, debug_logger):
        """
        Test equijoin with empty right relation.
        """
        debug_logger("Test: Empty Right Relation",
                     "Objective: Verify empty result when right relation is empty")

        empty_data = {"departments": []}
        empty_source = JsonSourceOperator(
            source_data=empty_data,
            iterator_query="$.departments[*]",
            attribute_mappings={"dept_id": "$.id"}
        )

        equijoin = EquiJoinOperator(
            employees_source,
            empty_source,
            A=["emp_dept_id"],
            B=["dept_id"]
        )
        result = equijoin.execute()

        assert len(result) == 0

    # ========================================================================
    # Operator Composition Tests
    # ========================================================================

    def test_equijoin_with_extend(self, employees_source, departments_source, debug_logger):
        """
        Test equijoin combined with ExtendOperator.
        """
        debug_logger("Test: EquiJoin + Extend",
                     "Objective: Apply Extend on join result")

        equijoin = EquiJoinOperator(
            employees_source,
            departments_source,
            A=["emp_dept_id"],
            B=["dept_id"]
        )

        # Extend with IRI generation
        extended = ExtendOperator(
            equijoin,
            "employee_iri",
            FunctionCall(to_iri, [
                Reference("emp_id"),
                Constant("http://example.org/employee/")
            ])
        )

        result = extended.execute()

        assert len(result) == 4
        for row in result:
            assert "employee_iri" in row
            assert isinstance(row["employee_iri"], IRI)

    def test_equijoin_with_project(self, employees_source, departments_source, debug_logger):
        """
        Test equijoin combined with ProjectOperator.
        """
        debug_logger("Test: EquiJoin + Project",
                     "Objective: Project specific attributes from join result")

        equijoin = EquiJoinOperator(
            employees_source,
            departments_source,
            A=["emp_dept_id"],
            B=["dept_id"]
        )

        # Project to only keep emp_name and dept_name
        projected = ProjectOperator(equijoin, {"emp_name", "dept_name"})
        result = projected.execute()

        assert len(result) == 4
        for row in result:
            assert set(row.keys()) == {"emp_name", "dept_name"}

    def test_chained_equijoins(self, debug_logger):
        """
        Test chaining multiple EquiJoin operators.
        """
        debug_logger("Test: Chained EquiJoins",
                     "Objective: Join three relations sequentially")

        # Employees
        emp_data = {"records": [
            {"id": 1, "name": "Alice", "dept": 101, "mgr": 10},
            {"id": 2, "name": "Bob", "dept": 102, "mgr": 20}
        ]}

        # Departments
        dept_data = {"records": [
            {"dept_id": 101, "dept_name": "Engineering"},
            {"dept_id": 102, "dept_name": "Marketing"}
        ]}

        # Managers
        mgr_data = {"records": [
            {"manager_id": 10, "manager_name": "John"},
            {"manager_id": 20, "manager_name": "Jane"}
        ]}

        emp_source = JsonSourceOperator(
            source_data=emp_data,
            iterator_query="$.records[*]",
            attribute_mappings={
                "emp_id": "$.id",
                "emp_name": "$.name",
                "emp_dept": "$.dept",
                "emp_mgr": "$.mgr"
            }
        )

        dept_source = JsonSourceOperator(
            source_data=dept_data,
            iterator_query="$.records[*]",
            attribute_mappings={
                "dept_id": "$.dept_id",
                "dept_name": "$.dept_name"
            }
        )

        mgr_source = JsonSourceOperator(
            source_data=mgr_data,
            iterator_query="$.records[*]",
            attribute_mappings={
                "manager_id": "$.manager_id",
                "manager_name": "$.manager_name"
            }
        )

        # First join: employees ⋈ departments
        first_join = EquiJoinOperator(
            emp_source,
            dept_source,
            A=["emp_dept"],
            B=["dept_id"]
        )

        # Second join: (employees ⋈ departments) ⋈ managers
        second_join = EquiJoinOperator(
            first_join,
            mgr_source,
            A=["emp_mgr"],
            B=["manager_id"]
        )

        result = second_join.execute()

        assert len(result) == 2

        # Verify full join
        alice_row = next(r for r in result if r["emp_name"] == "Alice")
        assert alice_row["dept_name"] == "Engineering"
        assert alice_row["manager_name"] == "John"

        bob_row = next(r for r in result if r["emp_name"] == "Bob")
        assert bob_row["dept_name"] == "Marketing"
        assert bob_row["manager_name"] == "Jane"

    # ========================================================================
    # Explain Functionality Tests
    # ========================================================================

    def test_explain_text_format(self, employees_source, departments_source, debug_logger):
        """
        Test explain() generates readable text output.
        """
        debug_logger("Test: Explain Text Format",
                     "Objective: Verify human-readable explain output")

        equijoin = EquiJoinOperator(
            employees_source,
            departments_source,
            A=["emp_dept_id"],
            B=["dept_id"]
        )

        explanation = equijoin.explain()

        assert "EquiJoin" in explanation
        assert "conditions" in explanation
        assert "emp_dept_id = dept_id" in explanation
        assert "left:" in explanation
        assert "right:" in explanation

        debug_logger("Explain Output", explanation)

    def test_explain_json_format(self, employees_source, departments_source, debug_logger):
        """
        Test explain_json() generates proper JSON structure.
        """
        debug_logger("Test: Explain JSON Format",
                     "Objective: Verify JSON serializable explain output")

        equijoin = EquiJoinOperator(
            employees_source,
            departments_source,
            A=["emp_dept_id"],
            B=["dept_id"]
        )

        json_explanation = equijoin.explain_json()

        assert json_explanation["type"] == "EquiJoin"
        assert "parameters" in json_explanation
        assert "join_conditions" in json_explanation["parameters"]
        assert "left" in json_explanation
        assert "right" in json_explanation

        # Verify join conditions
        conditions = json_explanation["parameters"]["join_conditions"]
        assert len(conditions) == 1
        assert conditions[0]["left"] == "emp_dept_id"
        assert conditions[0]["right"] == "dept_id"

        import json
        json_str = json.dumps(json_explanation, indent=2)
        debug_logger("Explain JSON Output", json_str)

    def test_explain_nested_operators(self, debug_logger):
        """
        Test explain with nested operator tree.
        """
        debug_logger("Test: Explain Nested Operators",
                     "Objective: Verify explain works with complex operator trees")

        left_data = {"items": [{"id": 1, "val": "A"}]}
        right_data = {"items": [{"ref": 1, "data": "X"}]}

        left_source = JsonSourceOperator(
            source_data=left_data,
            iterator_query="$.items[*]",
            attribute_mappings={"left_id": "$.id", "left_val": "$.val"}
        )

        right_source = JsonSourceOperator(
            source_data=right_data,
            iterator_query="$.items[*]",
            attribute_mappings={"right_ref": "$.ref", "right_data": "$.data"}
        )

        # Extend left source
        extended_left = ExtendOperator(
            left_source,
            "left_iri",
            FunctionCall(to_iri, [Reference("left_id"), Constant("http://left/")])
        )

        equijoin = EquiJoinOperator(
            extended_left,
            right_source,
            A=["left_id"],
            B=["right_ref"]
        )

        explanation = equijoin.explain()

        assert "EquiJoin" in explanation
        assert "Extend" in explanation
        assert "Source" in explanation

        debug_logger("Nested Explain Output", explanation)

    # ========================================================================
    # RML Use Case Tests
    # ========================================================================

    def test_referencing_object_map_use_case(self, debug_logger):
        """
        Test EquiJoin for RML referencing object map translation.

        This simulates the use case where an object map references
        another triples map via a join condition.
        """
        debug_logger("Test: Referencing Object Map Use Case",
                     "Objective: Simulate RML referencing object map join")

        # Student triples map data
        students_data = {
            "students": [
                {"id": "S1", "name": "Alice", "university_id": "U1"},
                {"id": "S2", "name": "Bob", "university_id": "U2"}
            ]
        }

        # University triples map data
        universities_data = {
            "universities": [
                {"uni_id": "U1", "uni_name": "MIT"},
                {"uni_id": "U2", "uni_name": "Stanford"}
            ]
        }

        student_source = JsonSourceOperator(
            source_data=students_data,
            iterator_query="$.students[*]",
            attribute_mappings={
                "student_id": "$.id",
                "student_name": "$.name",
                "student_uni_ref": "$.university_id"
            }
        )

        university_source = JsonSourceOperator(
            source_data=universities_data,
            iterator_query="$.universities[*]",
            attribute_mappings={
                "university_id": "$.uni_id",
                "university_name": "$.uni_name"
            }
        )

        # Generate student subject IRI
        student_extended = ExtendOperator(
            student_source,
            "student_subject",
            FunctionCall(to_iri, [
                Reference("student_id"),
                Constant("http://example.org/student/")
            ])
        )

        # Generate university subject IRI
        university_extended = ExtendOperator(
            university_source,
            "university_subject",
            FunctionCall(to_iri, [
                Reference("university_id"),
                Constant("http://example.org/university/")
            ])
        )

        # Join on the foreign key relationship
        equijoin = EquiJoinOperator(
            student_extended,
            university_extended,
            A=["student_uni_ref"],
            B=["university_id"]
        )

        result = equijoin.execute()

        assert len(result) == 2

        # Verify both IRIs are present and correct
        alice_row = next(r for r in result if r["student_name"] == "Alice")
        assert isinstance(alice_row["student_subject"], IRI)
        assert isinstance(alice_row["university_subject"], IRI)
        assert "student/S1" in str(alice_row["student_subject"])
        assert "university/U1" in str(alice_row["university_subject"])

        bob_row = next(r for r in result if r["student_name"] == "Bob")
        assert "student/S2" in str(bob_row["student_subject"])
        assert "university/U2" in str(bob_row["university_subject"])

    # ========================================================================
    # Edge Cases
    # ========================================================================

    def test_equijoin_with_epsilon_values(self, debug_logger):
        """
        Test equijoin behavior with EPSILON values in join attributes.
        Per the algebra, EPSILON represents undefined/error values and should
        match other EPSILON values in join conditions.
        """
        debug_logger("Test: EquiJoin with EPSILON Values",
                     "Objective: Verify EPSILON values are handled correctly in joins")

        from pyhartig.algebra.Tuple import EPSILON

        # Create tuples directly with EPSILON values
        left_tuples = [
            MappingTuple({"left_id": 1, "left_val": "A"}),
            MappingTuple({"left_id": EPSILON, "left_val": "B"})
        ]

        right_tuples = [
            MappingTuple({"right_ref": 1, "right_data": "X"}),
            MappingTuple({"right_ref": EPSILON, "right_data": "Y"})
        ]

        # Create mock source operators that return predefined tuples
        class MockOperator(Operator):
            def __init__(self, tuples):
                super().__init__()
                self.tuples = tuples

            def execute(self):
                return self.tuples

            def explain(self, indent=0, prefix=""):
                return "MockOperator"

            def explain_json(self):
                return {"type": "MockOperator"}

        left_source = MockOperator(left_tuples)
        right_source = MockOperator(right_tuples)

        equijoin = EquiJoinOperator(
            left_source,
            right_source,
            A=["left_id"],
            B=["right_ref"]
        )

        result = equijoin.execute()

        # Should have 2 matches: (1, 1) and (EPSILON, EPSILON)
        assert len(result) == 2

        # Verify both matches exist
        values = [(r["left_val"], r["right_data"]) for r in result]
        assert ("A", "X") in values
        assert ("B", "Y") in values

    def test_equijoin_preserves_tuple_values(self, debug_logger):
        """
        Test that equijoin correctly preserves all attribute values from both relations.
        """
        debug_logger("Test: EquiJoin Preserves Values",
                     "Objective: Verify all original values are maintained after join")

        left_data = {
            "items": [{"key": 1, "left_a": "LA", "left_b": 100}]
        }

        right_data = {
            "items": [{"key": 1, "right_x": "RX", "right_y": True}]
        }

        left_source = JsonSourceOperator(
            source_data=left_data,
            iterator_query="$.items[*]",
            attribute_mappings={
                "left_key": "$.key",
                "left_a": "$.left_a",
                "left_b": "$.left_b"
            }
        )

        right_source = JsonSourceOperator(
            source_data=right_data,
            iterator_query="$.items[*]",
            attribute_mappings={
                "right_key": "$.key",
                "right_x": "$.right_x",
                "right_y": "$.right_y"
            }
        )

        equijoin = EquiJoinOperator(
            left_source,
            right_source,
            A=["left_key"],
            B=["right_key"]
        )

        result = equijoin.execute()

        assert len(result) == 1
        row = result[0]

        # Verify all values from both sides
        assert row["left_key"] == 1
        assert row["left_a"] == "LA"
        assert row["left_b"] == 100
        assert row["right_key"] == 1
        assert row["right_x"] == "RX"
        assert row["right_y"] is True

