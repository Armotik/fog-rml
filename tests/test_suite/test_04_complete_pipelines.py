"""
Test Suite for Complete Pipelines

This module provides end-to-end tests for complete data transformation
pipelines, demonstrating realistic use cases that combine multiple
operators and transformations to produce RDF-like output structures.
"""

import pytest
import json
from pathlib import Path
from pyhartig.operators.sources.JsonSourceOperator import JsonSourceOperator
from pyhartig.operators.ExtendOperator import ExtendOperator
from pyhartig.operators.UnionOperator import UnionOperator
from pyhartig.expressions.Constant import Constant
from pyhartig.expressions.Reference import Reference
from pyhartig.expressions.FunctionCall import FunctionCall
from pyhartig.functions.builtins import to_iri, to_literal, concat
from pyhartig.algebra.Terms import IRI, Literal
from pyhartig.namespaces import FOAF_PERSON, RDF_TYPE, XSD_INTEGER, XSD_STRING


class TestCompletePipelines:
    """Test suite for complete end-to-end data transformation pipelines."""

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
    def test_data_path(self):
        """
        Fixture providing path to test data file.
        
        Returns:
            Path: Path to test_data.json
        """
        return Path(__file__).parent.parent.parent / "data" / "test_data.json"

    @pytest.fixture
    def load_test_data(self, test_data_path):
        """
        Fixture loading test data from file.
        
        Returns:
            dict: Parsed JSON test data
        """
        with open(test_data_path, 'r') as f:
            return json.load(f)

    def test_rdf_triples_generation_pipeline(self, load_test_data, debug_logger):
        """
        Test complete pipeline for RDF triple generation.
        
        Simulates a realistic scenario: extracting JSON data and
        transforming it into RDF-like triple structures with proper
        subject, predicate, and object handling.
        """
        debug_logger("Test: RDF Triple Generation Pipeline", 
                     f"Objective: Transform JSON to RDF triple structure\n"
                     f"Input data: {json.dumps(load_test_data, indent=2)}")
        
        # Stage 1: Extract team members
        source_op = JsonSourceOperator(
            source_data=load_test_data,
            iterator_query="$.team[*]",
            attribute_mappings={
                "member_id": "$.id",
                "member_name": "$.name",
                "role": "$.roles[*]",
                "skill": "$.skills[*]"
            }
        )
        
        debug_logger("Stage 1: Source Operator", 
                     f"Iterator: $.team[*]\n"
                     f"Attributes:\n"
                     f"  - member_id: $.id\n"
                     f"  - member_name: $.name\n"
                     f"  - role: $.roles[*]\n"
                     f"  - skill: $.skills[*]\n"
                     f"Note: Cartesian product over roles and skills")
        
        # Stage 2: Generate subject IRI
        subject_expr = FunctionCall(
            function=to_iri,
            arguments=[
                Reference("member_id"),
                Constant("http://example.org/person/")
            ]
        )
        extend_subject = ExtendOperator(
            parent_operator=source_op,
            new_attribute="subject",
            expression=subject_expr
        )
        
        debug_logger("Stage 2: Generate Subject IRI", 
                     "subject = to_iri(member_id, 'http://example.org/person/')")
        
        # Stage 3: Add RDF type
        extend_type = ExtendOperator(
            parent_operator=extend_subject,
            new_attribute="rdf_type",
            expression=Constant(FOAF_PERSON)
        )
        
        debug_logger("Stage 3: Add RDF Type", 
                     "rdf_type = foaf:Person (constant)")
        
        # Stage 4: Convert name to literal
        name_literal_expr = FunctionCall(
            function=to_literal,
            arguments=[
                Reference("member_name"),
                Constant(XSD_STRING.value)
            ]
        )
        extend_name = ExtendOperator(
            parent_operator=extend_type,
            new_attribute="name_literal",
            expression=name_literal_expr
        )
        
        debug_logger("Stage 4: Convert Name to Literal", 
                     "name_literal = to_literal(member_name, xsd:string)")
        
        # Stage 5: Convert role to literal
        role_literal_expr = FunctionCall(
            function=to_literal,
            arguments=[
                Reference("role"),
                Constant(XSD_STRING.value)
            ]
        )
        extend_role = ExtendOperator(
            parent_operator=extend_name,
            new_attribute="role_literal",
            expression=role_literal_expr
        )
        
        debug_logger("Stage 5: Convert Role to Literal", 
                     "role_literal = to_literal(role, xsd:string)")
        
        # Execute pipeline
        result = extend_role.execute()
        
        debug_logger("Pipeline Execution Result", 
                     f"Total tuples generated: {len(result)}\n"
                     f"First 5 tuples:\n" + 
                     "\n".join(f"  {i+1}. {tuple}" for i, tuple in enumerate(result[:5])))
        
        # Validate results
        assert len(result) > 0, "Pipeline should generate tuples"
        
        # Check structure of first tuple
        first_tuple = result[0]
        assert "subject" in first_tuple
        assert "rdf_type" in first_tuple
        assert "name_literal" in first_tuple
        assert "role_literal" in first_tuple
        
        # Type checks
        assert isinstance(first_tuple["subject"], IRI)
        assert isinstance(first_tuple["rdf_type"], IRI)
        assert isinstance(first_tuple["name_literal"], Literal)
        assert isinstance(first_tuple["role_literal"], Literal)
        
        # Alice has 2 roles and 2 skills → 4 combinations
        # Bob has 1 role and 1 skill → 1 combination
        # Total: 5 tuples
        assert len(result) == 5
        
        alice_tuples = [t for t in result if t["member_name"] == "Alice"]
        bob_tuples = [t for t in result if t["member_name"] == "Bob"]
        
        assert len(alice_tuples) == 4
        assert len(bob_tuples) == 1
        
        debug_logger("Validation Results", 
                     f"✓ Pipeline executed successfully\n"
                     f"✓ Total tuples: {len(result)}\n"
                     f"✓ Alice tuples: {len(alice_tuples)}\n"
                     f"✓ Bob tuples: {len(bob_tuples)}\n"
                     f"✓ All RDF structures properly typed\n\n"
                     f"Sample triple structure:\n"
                     f"  Subject: {first_tuple['subject']}\n"
                     f"  Type: {first_tuple['rdf_type']}\n"
                     f"  Name: {first_tuple['name_literal']}\n"
                     f"  Role: {first_tuple['role_literal']}")

    def test_person_profile_pipeline(self, debug_logger):
        """
        Test pipeline for generating person profiles with metadata.
        
        Demonstrates a practical pipeline that enriches person data
        with computed attributes and RDF representations.
        """
        debug_logger("Test: Person Profile Generation Pipeline", 
                     "Objective: Generate enriched person profiles")
        
        data = {
            "persons": [
                {
                    "id": "emp001",
                    "firstName": "Alice",
                    "lastName": "Smith",
                    "age": 30,
                    "department": "Engineering"
                },
                {
                    "id": "emp002",
                    "firstName": "Bob",
                    "lastName": "Jones",
                    "age": 25,
                    "department": "Sales"
                }
            ]
        }
        
        debug_logger("Input Data", json.dumps(data, indent=2))
        
        # Source
        source = JsonSourceOperator(
            source_data=data,
            iterator_query="$.persons[*]",
            attribute_mappings={
                "emp_id": "$.id",
                "first": "$.firstName",
                "last": "$.lastName",
                "age": "$.age",
                "dept": "$.department"
            }
        )
        
        # Generate URI
        uri_expr = FunctionCall(
            function=to_iri,
            arguments=[
                Reference("emp_id"),
                Constant("http://company.org/employee/")
            ]
        )
        pipeline = ExtendOperator(source, "uri", uri_expr)
        
        # Full name
        full_name_expr = FunctionCall(
            function=concat,
            arguments=[
                Reference("first"),
                Constant(" "),
                Reference("last")
            ]
        )
        pipeline = ExtendOperator(pipeline, "full_name", full_name_expr)
        
        # Full name as literal
        full_name_lit_expr = FunctionCall(
            function=to_literal,
            arguments=[
                Reference("full_name"),
                Constant(XSD_STRING.value)
            ]
        )
        pipeline = ExtendOperator(pipeline, "name_lit", full_name_lit_expr)
        
        # Age as literal
        age_lit_expr = FunctionCall(
            function=to_literal,
            arguments=[
                Reference("age"),
                Constant(XSD_INTEGER.value)
            ]
        )
        pipeline = ExtendOperator(pipeline, "age_lit", age_lit_expr)
        
        # Department IRI
        dept_iri_expr = FunctionCall(
            function=to_iri,
            arguments=[
                Reference("dept"),
                Constant("http://company.org/department/")
            ]
        )
        pipeline = ExtendOperator(pipeline, "dept_uri", dept_iri_expr)
        
        debug_logger("Pipeline Configuration", 
                     f"5-stage extension pipeline:\n"
                     f"  1. uri = to_iri(emp_id, base)\n"
                     f"  2. full_name = concat(first, ' ', last)\n"
                     f"  3. name_lit = to_literal(full_name, xsd:string)\n"
                     f"  4. age_lit = to_literal(age, xsd:integer)\n"
                     f"  5. dept_uri = to_iri(dept, dept_base)")
        
        result = pipeline.execute()
        
        debug_logger("Execution Result", 
                     f"Number of profiles: {len(result)}\n"
                     f"Profiles:\n" + 
                     "\n".join(f"  {i+1}. {tuple}" for i, tuple in enumerate(result)))
        
        assert len(result) == 2
        
        # Validate Alice's profile
        alice = result[0]
        assert alice["uri"].value == "http://company.org/employee/emp001"
        assert alice["full_name"].lexical_form == "Alice Smith"
        assert alice["name_lit"].lexical_form == "Alice Smith"
        assert alice["age_lit"].lexical_form == "30"
        assert alice["dept_uri"].value == "http://company.org/department/Engineering"
        
        debug_logger("Validation", 
                     f"✓ Person profiles generated successfully\n"
                     f"✓ Alice profile:\n"
                     f"    URI: {alice['uri']}\n"
                     f"    Full name: {alice['full_name']}\n"
                     f"    Age literal: {alice['age_lit']}\n"
                     f"    Department URI: {alice['dept_uri']}")

    def test_hierarchical_data_pipeline(self, debug_logger):
        """
        Test pipeline processing hierarchical data structures.
        
        Validates handling of nested data with multiple levels
        and complex relationships.
        """
        debug_logger("Test: Hierarchical Data Pipeline", 
                     "Objective: Process nested organizational structure")
        
        data = {
            "company": {
                "name": "TechCorp",
                "divisions": [
                    {
                        "id": "div1",
                        "name": "R&D",
                        "projects": [
                            {"code": "P001", "title": "AI Research"},
                            {"code": "P002", "title": "Data Platform"}
                        ]
                    },
                    {
                        "id": "div2",
                        "name": "Operations",
                        "projects": [
                            {"code": "P003", "title": "Infrastructure"}
                        ]
                    }
                ]
            }
        }
        
        debug_logger("Input Data", json.dumps(data, indent=2))
        
        # Source: iterate over projects within divisions
        source = JsonSourceOperator(
            source_data=data,
            iterator_query="$.company.divisions[*].projects[*]",
            attribute_mappings={
                "project_code": "$.code",
                "project_title": "$.title"
            }
        )
        
        # Generate project URI
        proj_uri_expr = FunctionCall(
            function=to_iri,
            arguments=[
                Reference("project_code"),
                Constant("http://techcorp.org/project/")
            ]
        )
        pipeline = ExtendOperator(source, "project_uri", proj_uri_expr)
        
        # Add project type
        pipeline = ExtendOperator(
            pipeline,
            "project_type",
            Constant(IRI("http://techcorp.org/ontology/Project"))
        )
        
        # Title as literal
        title_lit_expr = FunctionCall(
            function=to_literal,
            arguments=[
                Reference("project_title"),
                Constant(XSD_STRING.value)
            ]
        )
        pipeline = ExtendOperator(pipeline, "title_lit", title_lit_expr)
        
        debug_logger("Pipeline Configuration", 
                     f"Processing nested structure:\n"
                     f"  Iterator: $.company.divisions[*].projects[*]\n"
                     f"  Extensions: project_uri, project_type, title_lit")
        
        result = pipeline.execute()
        
        debug_logger("Execution Result", 
                     f"Number of projects: {len(result)}\n"
                     f"Projects:\n" + 
                     "\n".join(f"  {i+1}. {tuple}" for i, tuple in enumerate(result)))
        
        # 3 projects total
        assert len(result) == 3
        
        # Check all have required attributes
        for project in result:
            assert "project_uri" in project
            assert "project_type" in project
            assert "title_lit" in project
            assert isinstance(project["project_uri"], IRI)
            assert isinstance(project["project_type"], IRI)
            assert isinstance(project["title_lit"], Literal)
        
        # Check specific projects
        p001 = [p for p in result if p["project_code"] == "P001"][0]
        assert p001["project_uri"].value == "http://techcorp.org/project/P001"
        assert p001["title_lit"].lexical_form == "AI Research"
        
        debug_logger("Validation", 
                     f"✓ Hierarchical data processed successfully\n"
                     f"✓ 3 projects extracted from nested structure\n"
                     f"✓ Sample project: {p001['project_uri']}")

    def test_error_resilient_pipeline(self, debug_logger):
        """
        Test pipeline resilience with incomplete data.
        
        Validates graceful handling of missing or incomplete data
        in realistic scenarios.
        """
        debug_logger("Test: Error-Resilient Pipeline", 
                     "Objective: Handle incomplete data gracefully")
        
        data = {
            "entries": [
                {"id": "1", "value": "Complete"},
                {"id": "2"},  # Missing value
                {"id": "3", "value": "AlsoComplete"}
            ]
        }
        
        debug_logger("Input Data", 
                     f"{json.dumps(data, indent=2)}\n"
                     f"Note: Entry 2 has missing 'value' attribute")
        
        source = JsonSourceOperator(
            source_data=data,
            iterator_query="$.entries[*]",
            attribute_mappings={
                "entry_id": "$.id",
                "entry_value": "$.value"
            }
        )
        
        # Generate URI from ID
        uri_expr = FunctionCall(
            function=to_iri,
            arguments=[
                Reference("entry_id"),
                Constant("http://example.org/entry/")
            ]
        )
        pipeline = ExtendOperator(source, "uri", uri_expr)
        
        # Try to convert value to literal (will be EPSILON for missing)
        value_lit_expr = FunctionCall(
            function=to_literal,
            arguments=[
                Reference("entry_value"),
                Constant(XSD_STRING.value)
            ]
        )
        pipeline = ExtendOperator(pipeline, "value_lit", value_lit_expr)
        
        result = pipeline.execute()
        
        debug_logger("Execution Result", 
                     f"Number of tuples: {len(result)}\n"
                     f"Tuples:\n" + 
                     "\n".join(f"  {i+1}. {tuple}" for i, tuple in enumerate(result)))
        
        # Only entries with all required attributes generate tuples
        # Entry 2 has missing value, so Cartesian product yields 0 tuples for it
        assert len(result) == 2
        
        entry_ids = {t["entry_id"] for t in result}
        assert entry_ids == {"1", "3"}
        
        debug_logger("Validation", 
                     f"✓ Pipeline handled incomplete data\n"
                     f"✓ Generated {len(result)} tuples (incomplete entry filtered)\n"
                     f"✓ Entry IDs: {entry_ids}")

    def test_multi_source_union_pipeline(self, debug_logger):
        """
        Test complete pipeline with Union merging multiple data sources.

        Demonstrates realistic scenario of combining data from multiple
        sources into a unified result set.
        """
        debug_logger("Test: Multi-Source Union Pipeline",
                     "Objective: Merge and process data from multiple sources")

        # Data from different departments
        engineering_data = {
            "employees": [
                {"id": "E001", "name": "Alice", "role": "Engineer", "department": "Engineering"},
                {"id": "E002", "name": "Bob", "role": "Senior Engineer", "department": "Engineering"}
            ]
        }

        marketing_data = {
            "employees": [
                {"id": "M001", "name": "Charlie", "role": "Marketing Manager", "department": "Marketing"},
                {"id": "M002", "name": "Diana", "role": "Content Creator", "department": "Marketing"}
            ]
        }

        sales_data = {
            "employees": [
                {"id": "S001", "name": "Eve", "role": "Sales Rep", "department": "Sales"}
            ]
        }

        debug_logger("Input Data",
                     f"Engineering: 2 employees\n"
                     f"Marketing: 2 employees\n"
                     f"Sales: 1 employee\n"
                     f"Total expected: 5 employees")

        # Pipeline 1: Engineering
        source_eng = JsonSourceOperator(
            source_data=engineering_data,
            iterator_query="$.employees[*]",
            attribute_mappings={
                "emp_id": "$.id",
                "emp_name": "$.name",
                "emp_role": "$.role",
                "dept": "$.department"
            }
        )

        extend_eng = ExtendOperator(
            parent_operator=source_eng,
            new_attribute="subject",
            expression=FunctionCall(
                function=to_iri,
                arguments=[Reference("emp_id"), Constant("http://company.org/employee/")]
            )
        )

        # Pipeline 2: Marketing
        source_mkt = JsonSourceOperator(
            source_data=marketing_data,
            iterator_query="$.employees[*]",
            attribute_mappings={
                "emp_id": "$.id",
                "emp_name": "$.name",
                "emp_role": "$.role",
                "dept": "$.department"
            }
        )

        extend_mkt = ExtendOperator(
            parent_operator=source_mkt,
            new_attribute="subject",
            expression=FunctionCall(
                function=to_iri,
                arguments=[Reference("emp_id"), Constant("http://company.org/employee/")]
            )
        )

        # Pipeline 3: Sales
        source_sales = JsonSourceOperator(
            source_data=sales_data,
            iterator_query="$.employees[*]",
            attribute_mappings={
                "emp_id": "$.id",
                "emp_name": "$.name",
                "emp_role": "$.role",
                "dept": "$.department"
            }
        )

        extend_sales = ExtendOperator(
            parent_operator=source_sales,
            new_attribute="subject",
            expression=FunctionCall(
                function=to_iri,
                arguments=[Reference("emp_id"), Constant("http://company.org/employee/")]
            )
        )

        debug_logger("Pipeline Configuration",
                     f"Three independent pipelines:\n"
                     f"  1. Engineering: Source -> Extend(subject)\n"
                     f"  2. Marketing: Source -> Extend(subject)\n"
                     f"  3. Sales: Source -> Extend(subject)\n"
                     f"Union all three pipelines")

        # Union all pipelines
        union_op = UnionOperator(operators=[extend_eng, extend_mkt, extend_sales])

        # Add common RDF type to all employees after union
        final_pipeline = ExtendOperator(
            parent_operator=union_op,
            new_attribute="rdf_type",
            expression=Constant(FOAF_PERSON)
        )

        result = final_pipeline.execute()

        debug_logger("Execution Result",
                     f"Number of employees: {len(result)}\n"
                     f"Employees:\n" + "\n".join(
                         f"  {i + 1}. {t['emp_name']} ({t['dept']}) - {t['subject']}"
                         for i, t in enumerate(result)))

        assert len(result) == 5

        # Verify all have required attributes
        for emp in result:
            assert "subject" in emp
            assert "rdf_type" in emp
            assert isinstance(emp["subject"], IRI)
            assert isinstance(emp["rdf_type"], IRI)

        # Count by department
        dept_counts = {}
        for emp in result:
            dept = emp["dept"]
            dept_counts[dept] = dept_counts.get(dept, 0) + 1

        assert dept_counts["Engineering"] == 2
        assert dept_counts["Marketing"] == 2
        assert dept_counts["Sales"] == 1

        debug_logger("Validation",
                     f"✓ Multi-source union pipeline successful\n"
                     f"✓ All 5 employees present\n"
                     f"✓ Department distribution: {dept_counts}\n"
                     f"✓ All employees have subject IRI and RDF type")

    def test_union_with_post_processing_pipeline(self, debug_logger):
        """
        Test Union followed by complex post-processing.

        Demonstrates merging data from different sources and then
        applying uniform transformations to the merged result.
        """
        debug_logger("Test: Union with Post-Processing",
                     "Objective: Merge data and apply uniform transformations")

        # Two different data sources with different schemas
        authors_data = {
            "authors": [
                {"id": "A1", "firstName": "Alice", "lastName": "Smith"},
                {"id": "A2", "firstName": "Bob", "lastName": "Jones"}
            ]
        }

        contributors_data = {
            "contributors": [
                {"id": "C1", "firstName": "Charlie", "lastName": "Brown"},
                {"id": "C2", "firstName": "Diana", "lastName": "Prince"}
            ]
        }

        debug_logger("Input Data",
                     f"Authors: 2 persons\n"
                     f"Contributors: 2 persons\n"
                     f"Goal: Merge and create unified person representation")

        # Pipeline 1: Authors
        source_authors = JsonSourceOperator(
            source_data=authors_data,
            iterator_query="$.authors[*]",
            attribute_mappings={
                "person_id": "$.id",
                "first": "$.firstName",
                "last": "$.lastName"
            }
        )

        extend_authors_role = ExtendOperator(
            parent_operator=source_authors,
            new_attribute="role",
            expression=Constant(Literal("Author"))
        )

        # Pipeline 2: Contributors
        source_contributors = JsonSourceOperator(
            source_data=contributors_data,
            iterator_query="$.contributors[*]",
            attribute_mappings={
                "person_id": "$.id",
                "first": "$.firstName",
                "last": "$.lastName"
            }
        )

        extend_contributors_role = ExtendOperator(
            parent_operator=source_contributors,
            new_attribute="role",
            expression=Constant(Literal("Contributor"))
        )

        # Union both pipelines
        union_op = UnionOperator(operators=[extend_authors_role, extend_contributors_role])

        # Post-processing: Generate URI
        post_process_uri = ExtendOperator(
            parent_operator=union_op,
            new_attribute="subject",
            expression=FunctionCall(
                function=to_iri,
                arguments=[Reference("person_id"), Constant("http://example.org/person/")]
            )
        )

        # Post-processing: Generate full name
        post_process_name = ExtendOperator(
            parent_operator=post_process_uri,
            new_attribute="full_name",
            expression=FunctionCall(
                function=concat,
                arguments=[Reference("first"), Constant(" "), Reference("last")]
            )
        )

        # Post-processing: Generate label
        post_process_label = ExtendOperator(
            parent_operator=post_process_name,
            new_attribute="label",
            expression=FunctionCall(
                function=concat,
                arguments=[
                    Reference("full_name"),
                    Constant(" ("),
                    Reference("role"),
                    Constant(")")
                ]
            )
        )

        debug_logger("Pipeline Configuration",
                     f"Stage 1: Process Authors (add role='Author')\n"
                     f"Stage 2: Process Contributors (add role='Contributor')\n"
                     f"Stage 3: Union both pipelines\n"
                     f"Stage 4-6: Post-process union result:\n"
                     f"  - Generate subject URI\n"
                     f"  - Generate full_name\n"
                     f"  - Generate label with role")

        result = post_process_label.execute()

        debug_logger("Execution Result",
                     f"Number of persons: {len(result)}\n"
                     f"Persons:\n" + "\n".join(
                         f"  {i + 1}. {t['label'].lexical_form}"
                         for i, t in enumerate(result)))

        assert len(result) == 4

        # Verify all have computed attributes
        for person in result:
            assert "subject" in person
            assert "full_name" in person
            assert "label" in person
            assert "role" in person
            assert isinstance(person["subject"], IRI)

        # Check roles
        authors = [p for p in result if p["role"].lexical_form == "Author"]
        contributors = [p for p in result if p["role"].lexical_form == "Contributor"]
        assert len(authors) == 2
        assert len(contributors) == 2

        # Check label format
        alice = [p for p in result if p["first"] == "Alice"][0]
        assert alice["label"].lexical_form == "Alice Smith (Author)"

        debug_logger("Validation",
                     f"✓ Union with post-processing successful\n"
                     f"✓ All 4 persons merged and processed\n"
                     f"✓ Authors: {len(authors)}, Contributors: {len(contributors)}\n"
                     f"✓ Sample label: {alice['label'].lexical_form}")

    def test_complex_rdf_generation_with_union(self, debug_logger):
        """
        Test complete RDF generation pipeline with Union for multiple entity types.

        Demonstrates realistic scenario of generating RDF triples for
        different entity types and merging them into a unified graph.
        """
        debug_logger("Test: Complex RDF Generation with Union",
                     "Objective: Generate RDF triples for multiple entity types")

        data = {
            "people": [
                {"id": "P1", "name": "Alice", "email": "alice@example.org"},
                {"id": "P2", "name": "Bob", "email": "bob@example.org"}
            ],
            "projects": [
                {"id": "PR1", "title": "Project Alpha", "leader": "P1"},
                {"id": "PR2", "title": "Project Beta", "leader": "P2"}
            ]
        }

        debug_logger("Input Data",
                     f"People: 2 persons\n"
                     f"Projects: 2 projects\n"
                     f"Goal: Generate RDF triples for both entity types")

        # Pipeline 1: People
        source_people = JsonSourceOperator(
            source_data=data,
            iterator_query="$.people[*]",
            attribute_mappings={
                "id": "$.id",
                "name": "$.name",
                "email": "$.email"
            }
        )

        # Person URI
        people_uri = ExtendOperator(
            parent_operator=source_people,
            new_attribute="subject",
            expression=FunctionCall(
                function=to_iri,
                arguments=[Reference("id"), Constant("http://example.org/person/")]
            )
        )

        # Person type
        people_type = ExtendOperator(
            parent_operator=people_uri,
            new_attribute="predicate",
            expression=Constant(RDF_TYPE)
        )

        people_type_obj = ExtendOperator(
            parent_operator=people_type,
            new_attribute="object",
            expression=Constant(FOAF_PERSON)
        )

        # Pipeline 2: Projects
        source_projects = JsonSourceOperator(
            source_data=data,
            iterator_query="$.projects[*]",
            attribute_mappings={
                "id": "$.id",
                "title": "$.title",
                "leader": "$.leader"
            }
        )

        # Project URI
        projects_uri = ExtendOperator(
            parent_operator=source_projects,
            new_attribute="subject",
            expression=FunctionCall(
                function=to_iri,
                arguments=[Reference("id"), Constant("http://example.org/project/")]
            )
        )

        # Project type
        projects_type = ExtendOperator(
            parent_operator=projects_uri,
            new_attribute="predicate",
            expression=Constant(RDF_TYPE)
        )

        projects_type_obj = ExtendOperator(
            parent_operator=projects_type,
            new_attribute="object",
            expression=Constant(IRI("http://example.org/ontology/Project"))
        )

        debug_logger("Pipeline Configuration",
                     f"Pipeline 1: People -> Generate (subject, predicate, object) for type\n"
                     f"Pipeline 2: Projects -> Generate (subject, predicate, object) for type\n"
                     f"Union both to create unified triple set")

        # Union both pipelines to create combined triple set
        union_triples = UnionOperator(operators=[people_type_obj, projects_type_obj])

        result = union_triples.execute()

        debug_logger("Execution Result",
                     f"Number of triples: {len(result)}\n"
                     f"Triples:\n" + "\n".join(
                         f"  {i + 1}. <{t['subject']}> <{t['predicate']}> <{t['object']}>"
                         for i, t in enumerate(result)))

        assert len(result) == 4  # 2 people + 2 projects

        # Verify all triples have required structure
        for triple in result:
            assert "subject" in triple
            assert "predicate" in triple
            assert "object" in triple
            assert isinstance(triple["subject"], IRI)
            assert isinstance(triple["predicate"], IRI)
            assert isinstance(triple["object"], IRI)

        # Count by object type
        person_triples = [t for t in result if "Person" in t["object"].value]
        project_triples = [t for t in result if "Project" in t["object"].value]
        assert len(person_triples) == 2
        assert len(project_triples) == 2

        # Verify specific triples
        alice_triple = [t for t in result if "P1" in t["subject"].value][0]
        assert alice_triple["object"].value == FOAF_PERSON.value

        alpha_triple = [t for t in result if "PR1" in t["subject"].value][0]
        assert alpha_triple["object"].value == "http://example.org/ontology/Project"

        debug_logger("Validation",
                     f"✓ RDF generation with union successful\n"
                     f"✓ Total triples: {len(result)}\n"
                     f"✓ Person triples: {len(person_triples)}\n"
                     f"✓ Project triples: {len(project_triples)}\n"
                     f"✓ All triples have proper RDF structure")
