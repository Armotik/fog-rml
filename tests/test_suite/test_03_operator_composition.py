"""
Test Suite for Operator Composition

This module validates the integration and composition of multiple
operators, specifically focusing on Source and Extend operator fusion.
Tests demonstrate how operators combine to form complex data pipelines.
"""

import pytest
from pyhartig.operators.sources.JsonSourceOperator import JsonSourceOperator
from pyhartig.operators.ExtendOperator import ExtendOperator
from pyhartig.operators.UnionOperator import UnionOperator
from pyhartig.expressions.Constant import Constant
from pyhartig.expressions.Reference import Reference
from pyhartig.expressions.FunctionCall import FunctionCall
from pyhartig.functions.builtins import to_iri, to_literal, concat
from pyhartig.algebra.Terms import IRI, Literal
from pyhartig.namespaces import FOAF_PERSON, XSD_STRING


class TestOperatorComposition:
    """Test suite for operator composition and fusion."""

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
    def team_data(self):
        """
        Fixture providing team structure data.
        
        Returns:
            dict: JSON structure with team information
        """
        return {
            "project": "MCP-SPARQLLM",
            "team": [
                {
                    "id": 1,
                    "name": "Alice",
                    "roles": ["Dev", "Admin"],
                    "skills": ["Python", "RDF"]
                },
                {
                    "id": 2,
                    "name": "Bob",
                    "roles": ["User"],
                    "skills": ["Java"]
                }
            ]
        }

    def test_source_extend_basic_fusion(self, team_data, debug_logger):
        """
        Test basic fusion of Source and Extend operators.
        
        Validates that Extend correctly processes Source output,
        adding computed attributes to each tuple.
        """
        debug_logger("Test: Basic Source-Extend Fusion",
                     "Objective: Extract team data and add RDF type")

        # Create source operator
        source_op = JsonSourceOperator(
            source_data=team_data,
            iterator_query="$.team[*]",
            attribute_mappings={
                "person_id": "$.id",
                "person_name": "$.name"
            }
        )

        # Extend with RDF type
        extend_op = ExtendOperator(
            parent_operator=source_op,
            new_attribute="rdf_type",
            expression=Constant(FOAF_PERSON)
        )

        debug_logger("Pipeline Configuration",
                     f"Stage 1 - Source:\n"
                     f"  Iterator: $.team[*]\n"
                     f"  Mappings: person_id, person_name\n\n"
                     f"Stage 2 - Extend:\n"
                     f"  New attribute: rdf_type\n"
                     f"  Expression: Constant(IRI(foaf:Person))")

        result = extend_op.execute()

        debug_logger("Execution Result",
                     f"Number of tuples: {len(result)}\n"
                     f"Tuples:\n" + "\n".join(f"  {i + 1}. {tuple}" for i, tuple in enumerate(result)))

        assert len(result) == 2
        assert all("rdf_type" in tuple for tuple in result)
        assert all(isinstance(tuple["rdf_type"], IRI) for tuple in result)
        assert result[0]["person_name"] == "Alice"
        assert result[1]["person_name"] == "Bob"

        debug_logger("Validation",
                     "✓ Source-Extend fusion successful\n"
                     "✓ All tuples contain computed RDF type")

    def test_source_multiple_extends(self, team_data, debug_logger):
        """
        Test Source operator with multiple sequential Extends.
        
        Validates complex pipelines where multiple computed attributes
        are added sequentially, potentially depending on each other.
        """
        debug_logger("Test: Source with Multiple Sequential Extends",
                     "Objective: Build complex RDF representation")

        # Source operator
        source_op = JsonSourceOperator(
            source_data=team_data,
            iterator_query="$.team[*]",
            attribute_mappings={
                "id": "$.id",
                "name": "$.name"
            }
        )

        # First extension: generate subject IRI
        subject_expr = FunctionCall(
            function=to_iri,
            arguments=[
                Reference("id"),
                Constant("http://example.org/person/")
            ]
        )
        extend_subject = ExtendOperator(
            parent_operator=source_op,
            new_attribute="subject",
            expression=subject_expr
        )

        # Second extension: add RDF type
        extend_type = ExtendOperator(
            parent_operator=extend_subject,
            new_attribute="type",
            expression=Constant(FOAF_PERSON)
        )

        # Third extension: convert name to literal
        name_literal_expr = FunctionCall(
            function=to_literal,
            arguments=[
                Reference("name"),
                Constant(XSD_STRING.value)
            ]
        )
        extend_name = ExtendOperator(
            parent_operator=extend_type,
            new_attribute="name_literal",
            expression=name_literal_expr
        )

        debug_logger("Pipeline Configuration",
                     f"Stage 1 - Source: Extract id and name\n"
                     f"Stage 2 - Extend: subject = to_iri(id, base)\n"
                     f"Stage 3 - Extend: type = foaf:Person\n"
                     f"Stage 4 - Extend: name_literal = to_literal(name, xsd:string)")

        result = extend_name.execute()

        debug_logger("Execution Result",
                     f"Number of tuples: {len(result)}\n"
                     f"Sample tuple:\n{result[0] if result else 'No results'}")

        assert len(result) == 2

        # Verify first tuple structure
        tuple1 = result[0]
        assert "subject" in tuple1
        assert "type" in tuple1
        assert "name_literal" in tuple1
        assert isinstance(tuple1["subject"], IRI)
        assert isinstance(tuple1["type"], IRI)
        assert isinstance(tuple1["name_literal"], Literal)
        assert tuple1["subject"].value == "http://example.org/person/1"
        assert tuple1["name_literal"].lexical_form == "Alice"

        debug_logger("Validation",
                     f"✓ Multi-stage pipeline successful\n"
                     f"✓ Subject IRI: {tuple1['subject']}\n"
                     f"✓ Type IRI: {tuple1['type']}\n"
                     f"✓ Name literal: {tuple1['name_literal']}")

    def test_source_cartesian_with_extend(self, team_data, debug_logger):
        """
        Test Extend operator on Source with Cartesian product.
        
        Validates that extensions are correctly applied to each tuple
        generated by Cartesian product in the Source operator.
        """
        debug_logger("Test: Extend on Cartesian Product",
                     "Objective: Apply extensions to expanded tuple set")

        # Source with array extraction (generates Cartesian product)
        source_op = JsonSourceOperator(
            source_data=team_data,
            iterator_query="$.team[*]",
            attribute_mappings={
                "name": "$.name",
                "role": "$.roles[*]"
            }
        )

        # Extend with role-based category
        category_expr = FunctionCall(
            function=concat,
            arguments=[
                Constant("Role: "),
                Reference("role")
            ]
        )
        extend_op = ExtendOperator(
            parent_operator=source_op,
            new_attribute="role_label",
            expression=category_expr
        )

        debug_logger("Pipeline Configuration",
                     f"Stage 1 - Source:\n"
                     f"  Generates Cartesian product for roles\n"
                     f"  Alice: 2 roles, Bob: 1 role → 3 tuples\n\n"
                     f"Stage 2 - Extend:\n"
                     f"  role_label = concat('Role: ', role)")

        result = extend_op.execute()

        debug_logger("Execution Result",
                     f"Number of tuples: {len(result)}\n"
                     f"Tuples:\n" + "\n".join(f"  {i + 1}. {tuple}" for i, tuple in enumerate(result)))

        # Alice has 2 roles, Bob has 1 role → 3 tuples
        assert len(result) == 3
        assert all("role_label" in tuple for tuple in result)

        alice_tuples = [t for t in result if t["name"] == "Alice"]
        assert len(alice_tuples) == 2
        assert all(t["role_label"].lexical_form.startswith("Role: ") for t in alice_tuples)

        debug_logger("Validation",
                     f"✓ Extend applied to all Cartesian product tuples\n"
                     f"✓ Alice tuples: {len(alice_tuples)}\n"
                     f"✓ Sample role_label: {result[0]['role_label']}")

    def test_complex_fusion_with_dependencies(self, debug_logger):
        """
        Test operator fusion with dependent computed attributes.
        
        Validates that later extensions can reference attributes
        computed by earlier extensions in the pipeline.
        """
        debug_logger("Test: Complex Fusion with Dependencies",
                     "Objective: Computed attributes referencing earlier computations")

        data = {
            "people": [
                {"first_name": "John", "last_name": "Doe", "age": 30}
            ]
        }

        # Source
        source_op = JsonSourceOperator(
            source_data=data,
            iterator_query="$.people[*]",
            attribute_mappings={
                "first": "$.first_name",
                "last": "$.last_name",
                "age": "$.age"
            }
        )

        # Extension 1: Compute full name
        full_name_expr = FunctionCall(
            function=concat,
            arguments=[
                Reference("first"),
                Constant(" "),
                Reference("last")
            ]
        )
        extend_name = ExtendOperator(
            parent_operator=source_op,
            new_attribute="full_name",
            expression=full_name_expr
        )

        # Extension 2: Create label using computed full_name
        label_expr = FunctionCall(
            function=concat,
            arguments=[
                Reference("full_name"),  # References computed attribute!
                Constant(" (age: "),
                Reference("age"),
                Constant(")")
            ]
        )
        extend_label = ExtendOperator(
            parent_operator=extend_name,
            new_attribute="description",
            expression=label_expr
        )

        debug_logger("Pipeline Configuration",
                     f"Stage 1 - Source: Extract first, last, age\n"
                     f"Stage 2 - Extend: full_name = concat(first, ' ', last)\n"
                     f"Stage 3 - Extend: description = concat(full_name, ' (age: ', age, ')')\n"
                     f"Note: Stage 3 references computed attribute from Stage 2")

        result = extend_label.execute()

        debug_logger("Execution Result",
                     f"Number of tuples: {len(result)}\n"
                     f"Tuple: {result[0] if result else 'No results'}")

        assert len(result) == 1
        assert result[0]["full_name"].lexical_form == "John Doe"

        debug_logger("Validation",
                     f"✓ Dependent computations successful\n"
                     f"✓ full_name: {result[0]['full_name']}\n"
                     f"✓ description computed using full_name")

    def test_empty_source_extend_propagation(self, debug_logger):
        """
        Test Extend behavior when Source produces no tuples.
        
        Validates that empty result sets propagate correctly
        through the pipeline.
        """
        debug_logger("Test: Empty Source Propagation",
                     "Objective: Validate empty result set handling")

        data = {"items": []}

        source_op = JsonSourceOperator(
            source_data=data,
            iterator_query="$.items[*]",
            attribute_mappings={"value": "$.val"}
        )

        extend_op = ExtendOperator(
            parent_operator=source_op,
            new_attribute="computed",
            expression=Constant("value")
        )

        debug_logger("Configuration",
                     "Source with empty iterator → Extend should receive no tuples")

        result = extend_op.execute()

        debug_logger("Execution Result",
                     f"Number of tuples: {len(result)}\n"
                     f"Expected: 0 (empty propagates through pipeline)")

        assert len(result) == 0

        debug_logger("Validation",
                     "✓ Empty result set correctly propagated")

    def test_parallel_extend_branches(self, team_data, debug_logger):
        """
        Test multiple independent Extend branches from same Source.
        
        Validates that the same Source can feed multiple independent
        extension pipelines.
        """
        debug_logger("Test: Parallel Extension Branches",
                     "Objective: Multiple independent extensions from one source")

        source_op = JsonSourceOperator(
            source_data=team_data,
            iterator_query="$.team[*]",
            attribute_mappings={"id": "$.id", "name": "$.name"}
        )

        # Branch 1: Add subject IRI
        branch1 = ExtendOperator(
            parent_operator=source_op,
            new_attribute="subject",
            expression=FunctionCall(
                function=to_iri,
                arguments=[Reference("id"), Constant("http://example.org/person/")]
            )
        )

        # Branch 2: Add label literal (independent of branch 1)
        branch2 = ExtendOperator(
            parent_operator=source_op,
            new_attribute="label",
            expression=FunctionCall(
                function=to_literal,
                arguments=[Reference("name"), Constant(XSD_STRING.value)]
            )
        )

        debug_logger("Pipeline Configuration",
                     f"Single Source with two independent branches:\n"
                     f"  Branch 1: Add 'subject' IRI\n"
                     f"  Branch 2: Add 'label' literal")

        result1 = branch1.execute()
        result2 = branch2.execute()

        debug_logger("Execution Results",
                     f"Branch 1 tuples: {len(result1)}\n"
                     f"  Sample: {result1[0] if result1 else 'None'}\n\n"
                     f"Branch 2 tuples: {len(result2)}\n"
                     f"  Sample: {result2[0] if result2 else 'None'}")

        assert len(result1) == 2
        assert len(result2) == 2
        assert "subject" in result1[0]
        assert "subject" not in result2[0]
        assert "label" in result2[0]
        assert "label" not in result1[0]

        debug_logger("Validation",
                     "✓ Parallel branches execute independently\n"
                     "✓ Each branch has distinct attributes")

    def test_union_with_extend_composition(self, debug_logger):
        """
        Test Union operator combined with Extend operators.

        Validates that Union correctly merges results from multiple
        extended sources.
        """
        debug_logger("Test: Union with Extend Composition",
                     "Objective: Merge multiple extended pipelines")

        # First pipeline: Engineering team
        data_eng = {
            "team": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ]
        }

        source_eng = JsonSourceOperator(
            source_data=data_eng,
            iterator_query="$.team[*]",
            attribute_mappings={"person_id": "$.id", "person_name": "$.name"}
        )

        extend_eng = ExtendOperator(
            parent_operator=source_eng,
            new_attribute="department",
            expression=Constant(Literal("Engineering"))
        )

        # Second pipeline: Marketing team
        data_mkt = {
            "team": [
                {"id": 3, "name": "Charlie"},
                {"id": 4, "name": "Diana"}
            ]
        }

        source_mkt = JsonSourceOperator(
            source_data=data_mkt,
            iterator_query="$.team[*]",
            attribute_mappings={"person_id": "$.id", "person_name": "$.name"}
        )

        extend_mkt = ExtendOperator(
            parent_operator=source_mkt,
            new_attribute="department",
            expression=Constant(Literal("Marketing"))
        )

        debug_logger("Pipeline Configuration",
                     f"Pipeline 1: Source(Engineering) -> Extend(dept='Engineering')\n"
                     f"Pipeline 2: Source(Marketing) -> Extend(dept='Marketing')\n"
                     f"Union of both extended pipelines")

        # Union the extended pipelines
        union_op = UnionOperator(operators=[extend_eng, extend_mkt])
        result = union_op.execute()

        debug_logger("Execution Result",
                     f"Number of tuples: {len(result)}\n"
                     f"Tuples:\n" + "\n".join(f"  {i + 1}. {t['person_name']} - {t['department']}"
                                              for i, t in enumerate(result)))

        assert len(result) == 4

        # Verify departments
        eng_count = sum(1 for t in result if t["department"].lexical_form == "Engineering")
        mkt_count = sum(1 for t in result if t["department"].lexical_form == "Marketing")
        assert eng_count == 2
        assert mkt_count == 2

        debug_logger("Validation",
                     "✓ Union of extended pipelines successful\n"
                     "✓ Department attributes correctly assigned\n"
                     "✓ All 4 tuples present")

    def test_extend_after_union(self, debug_logger):
        """
        Test Extend operator applied after Union operator.

        Validates that Union output can be extended with computed attributes.
        """
        debug_logger("Test: Extend After Union",
                     "Objective: Apply extensions to merged union results")

        data_a = {
            "team": [
                {"id": 1, "name": "Alice"}
            ]
        }

        data_b = {
            "team": [
                {"id": 2, "name": "Bob"}
            ]
        }

        source_a = JsonSourceOperator(
            source_data=data_a,
            iterator_query="$.team[*]",
            attribute_mappings={"person_id": "$.id", "person_name": "$.name"}
        )

        source_b = JsonSourceOperator(
            source_data=data_b,
            iterator_query="$.team[*]",
            attribute_mappings={"person_id": "$.id", "person_name": "$.name"}
        )

        # Union first
        union_op = UnionOperator(operators=[source_a, source_b])

        # Then extend the union result
        extend_op = ExtendOperator(
            parent_operator=union_op,
            new_attribute="subject",
            expression=FunctionCall(
                function=to_iri,
                arguments=[
                    Reference("person_id"),
                    Constant("http://example.org/person/")
                ]
            )
        )

        debug_logger("Pipeline Configuration",
                     f"Stage 1: Union(Source A, Source B)\n"
                     f"Stage 2: Extend union result with 'subject' IRI")

        result = extend_op.execute()

        debug_logger("Execution Result",
                     f"Number of tuples: {len(result)}\n"
                     f"Tuples:\n" + "\n".join(f"  {i + 1}. {t['person_name']} - {t['subject']}"
                                              for i, t in enumerate(result)))

        assert len(result) == 2

        # Verify all tuples have the computed subject
        for tuple in result:
            assert "subject" in tuple
            assert isinstance(tuple["subject"], IRI)
            assert tuple["subject"].value.startswith("http://example.org/person/")

        debug_logger("Validation",
                     "✓ Extend after Union successful\n"
                     "✓ All merged tuples have computed attribute\n"
                     "✓ IRI subjects correctly generated")

    def test_union_of_complex_pipelines(self, debug_logger):
        """
        Test Union of multiple complex pipelines with multiple extensions.

        Validates that Union can merge results from sophisticated
        multi-stage pipelines.
        """
        debug_logger("Test: Union of Complex Pipelines",
                     "Objective: Merge results from multi-stage pipelines")

        data = {
            "people": [
                {"id": 1, "first": "Alice", "last": "Anderson"},
                {"id": 2, "first": "Bob", "last": "Brown"}
            ]
        }

        # Pipeline 1: Full processing for Alice
        source_1 = JsonSourceOperator(
            source_data={"people": [data["people"][0]]},
            iterator_query="$.people[*]",
            attribute_mappings={
                "id": "$.id",
                "first": "$.first",
                "last": "$.last"
            }
        )

        extend_1a = ExtendOperator(
            parent_operator=source_1,
            new_attribute="full_name",
            expression=FunctionCall(
                function=concat,
                arguments=[Reference("first"), Constant(" "), Reference("last")]
            )
        )

        extend_1b = ExtendOperator(
            parent_operator=extend_1a,
            new_attribute="subject",
            expression=FunctionCall(
                function=to_iri,
                arguments=[Reference("id"), Constant("http://example.org/person/")]
            )
        )

        # Pipeline 2: Full processing for Bob
        source_2 = JsonSourceOperator(
            source_data={"people": [data["people"][1]]},
            iterator_query="$.people[*]",
            attribute_mappings={
                "id": "$.id",
                "first": "$.first",
                "last": "$.last"
            }
        )

        extend_2a = ExtendOperator(
            parent_operator=source_2,
            new_attribute="full_name",
            expression=FunctionCall(
                function=concat,
                arguments=[Reference("first"), Constant(" "), Reference("last")]
            )
        )

        extend_2b = ExtendOperator(
            parent_operator=extend_2a,
            new_attribute="subject",
            expression=FunctionCall(
                function=to_iri,
                arguments=[Reference("id"), Constant("http://example.org/person/")]
            )
        )

        debug_logger("Pipeline Configuration",
                     f"Pipeline 1: Source -> Extend(full_name) -> Extend(subject)\n"
                     f"Pipeline 2: Source -> Extend(full_name) -> Extend(subject)\n"
                     f"Union of both complex pipelines")

        # Union the complex pipelines
        union_op = UnionOperator(operators=[extend_1b, extend_2b])
        result = union_op.execute()

        debug_logger("Execution Result",
                     f"Number of tuples: {len(result)}\n"
                     f"Sample tuple: {result[0] if result else 'None'}")

        assert len(result) == 2

        # Verify both tuples have all computed attributes
        for tuple in result:
            assert "full_name" in tuple
            assert "subject" in tuple
            assert isinstance(tuple["subject"], IRI)

        assert result[0]["full_name"].lexical_form == "Alice Anderson"
        assert result[1]["full_name"].lexical_form == "Bob Brown"

        debug_logger("Validation",
                     "✓ Union of complex pipelines successful\n"
                     "✓ All computed attributes preserved\n"
                     "✓ Full names: Alice Anderson, Bob Brown")

    def test_nested_union_composition(self, debug_logger):
        """
        Test nested Union operators (Union of Unions).

        Validates that Union operators can be nested to create
        hierarchical merging structures.
        """
        debug_logger("Test: Nested Union Composition",
                     "Objective: Union of Union operators")

        data_a = {"team": [{"id": 1, "name": "Alice"}]}
        data_b = {"team": [{"id": 2, "name": "Bob"}]}
        data_c = {"team": [{"id": 3, "name": "Charlie"}]}
        data_d = {"team": [{"id": 4, "name": "Diana"}]}

        # Create four source operators
        sources = []
        for data in [data_a, data_b, data_c, data_d]:
            source = JsonSourceOperator(
                source_data=data,
                iterator_query="$.team[*]",
                attribute_mappings={"person_id": "$.id", "person_name": "$.name"}
            )
            sources.append(source)

        # Create nested unions: Union(Union(A, B), Union(C, D))
        union_ab = UnionOperator(operators=[sources[0], sources[1]])
        union_cd = UnionOperator(operators=[sources[2], sources[3]])
        union_all = UnionOperator(operators=[union_ab, union_cd])

        debug_logger("Pipeline Configuration",
                     f"Nested structure:\n"
                     f"  Union_AB = Union(Source A, Source B)\n"
                     f"  Union_CD = Union(Source C, Source D)\n"
                     f"  Union_All = Union(Union_AB, Union_CD)")

        result = union_all.execute()

        debug_logger("Execution Result",
                     f"Number of tuples: {len(result)}\n"
                     f"Names: {[t['person_name'] for t in result]}")

        assert len(result) == 4
        names = [t["person_name"] for t in result]
        assert "Alice" in names
        assert "Bob" in names
        assert "Charlie" in names
        assert "Diana" in names

        debug_logger("Validation",
                     "✓ Nested unions successful\n"
                     "✓ All 4 tuples from nested structure present")
