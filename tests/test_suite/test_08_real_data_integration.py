"""
Test Suite for Real Data Integration

This module provides integration tests using actual project data files,
including the test_data.json and test_mapping.rml files. These tests
validate end-to-end functionality with realistic data scenarios.
"""

import pytest
import json
from pathlib import Path
from pyhartig.operators.sources.JsonSourceOperator import JsonSourceOperator
from pyhartig.operators.ExtendOperator import ExtendOperator
from pyhartig.expressions.Constant import Constant
from pyhartig.expressions.Reference import Reference
from pyhartig.expressions.FunctionCall import FunctionCall
from pyhartig.functions.builtins import to_iri, to_literal
from pyhartig.algebra.Terms import IRI, Literal
from pyhartig.namespaces import FOAF_PERSON, XSD_STRING


class TestRealDataIntegration:
    """Test suite for integration with actual project data files."""

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
    def data_dir(self):
        """
        Fixture providing path to data directory.
        
        Returns:
            Path: Path to data directory
        """
        return Path(__file__).parent.parent.parent / "data"

    @pytest.fixture
    def test_data_json(self, data_dir):
        """
        Fixture loading test_data.json.
        
        Returns:
            dict: Parsed JSON data
        """
        data_file = data_dir / "test_data.json"
        with open(data_file, 'r') as f:
            return json.load(f)

    @pytest.fixture
    def mapping_yaml_path(self, data_dir):
        """
        Fixture providing path to test_mapping.rml.
        
        Returns:
            Path: Path to mapping file
        """
        return data_dir / "mappings" / "test_mapping.rml"

    def test_load_test_data_file(self, test_data_json, debug_logger):
        """
        Test loading and validating test_data.json structure.
        
        Validates that the test data file is properly formatted
        and contains expected fields.
        """
        debug_logger("Test: Load Test Data File", 
                     "Objective: Validate test_data.json structure")
        
        debug_logger("Loaded Data", 
                     json.dumps(test_data_json, indent=2))
        
        # Validate structure
        assert "project" in test_data_json
        assert "team" in test_data_json
        assert isinstance(test_data_json["team"], list)
        assert len(test_data_json["team"]) > 0
        
        # Validate team member structure
        for member in test_data_json["team"]:
            assert "id" in member
            assert "name" in member
            assert "roles" in member
            assert "skills" in member
        
        debug_logger("Validation", 
                     f"✓ Data file loaded successfully\n"
                     f"✓ Project: {test_data_json['project']}\n"
                     f"✓ Team members: {len(test_data_json['team'])}")

    def test_extract_team_members(self, test_data_json, debug_logger):
        """
        Test extracting team members from test data.
        
        Validates basic extraction of team member information
        using Source operator.
        """
        debug_logger("Test: Extract Team Members", 
                     "Objective: Extract member data using Source operator")
        
        operator = JsonSourceOperator(
            source_data=test_data_json,
            iterator_query="$.team[*]",
            attribute_mappings={
                "member_id": "$.id",
                "member_name": "$.name"
            }
        )
        
        result = operator.execute()
        
        debug_logger("Execution Result", 
                     f"Extracted {len(result)} team members:\n" + 
                     "\n".join(f"  {i+1}. ID={t['member_id']}, Name={t['member_name']}" 
                              for i, t in enumerate(result)))
        
        assert len(result) == len(test_data_json["team"])
        
        # Verify Alice and Bob are present
        names = {t["member_name"] for t in result}
        assert "Alice" in names
        assert "Bob" in names
        
        debug_logger("Validation", 
                     f"✓ All team members extracted\n"
                     f"✓ Names: {names}")

    def test_team_roles_cartesian_product(self, test_data_json, debug_logger):
        """
        Test Cartesian product with team roles.
        
        Validates that multiple roles per member generate
        correct number of tuples.
        """
        debug_logger("Test: Team Roles Cartesian Product", 
                     "Objective: Generate tuples for each member-role pair")
        
        operator = JsonSourceOperator(
            source_data=test_data_json,
            iterator_query="$.team[*]",
            attribute_mappings={
                "member_name": "$.name",
                "role": "$.roles[*]"
            }
        )
        
        result = operator.execute()
        
        debug_logger("Execution Result", 
                     f"Total member-role pairs: {len(result)}\n"
                     f"Tuples:\n" + 
                     "\n".join(f"  {i+1}. {t['member_name']} - {t['role']}" 
                              for i, t in enumerate(result)))
        
        # Count expected tuples
        expected_count = sum(len(member["roles"]) for member in test_data_json["team"])
        assert len(result) == expected_count
        
        # Alice should have 2 roles
        alice_tuples = [t for t in result if t["member_name"] == "Alice"]
        assert len(alice_tuples) == 2
        
        alice_roles = {t["role"] for t in alice_tuples}
        assert "Dev" in alice_roles
        assert "Admin" in alice_roles
        
        debug_logger("Validation", 
                     f"✓ Cartesian product correct: {len(result)} tuples\n"
                     f"✓ Alice roles: {alice_roles}")

    def test_team_skills_extraction(self, test_data_json, debug_logger):
        """
        Test extraction of team member skills.
        
        Validates handling of nested array attributes.
        """
        debug_logger("Test: Team Skills Extraction", 
                     "Objective: Extract member skills")
        
        operator = JsonSourceOperator(
            source_data=test_data_json,
            iterator_query="$.team[*]",
            attribute_mappings={
                "member_name": "$.name",
                "skill": "$.skills[*]"
            }
        )
        
        result = operator.execute()
        
        debug_logger("Execution Result", 
                     f"Total member-skill pairs: {len(result)}\n"
                     f"Sample tuples:\n" + 
                     "\n".join(f"  {i+1}. {t['member_name']} - {t['skill']}" 
                              for i, t in enumerate(result[:5])))
        
        # Alice has Python and RDF skills
        alice_skills = [t["skill"] for t in result if t["member_name"] == "Alice"]
        assert "Python" in alice_skills
        assert "RDF" in alice_skills
        
        debug_logger("Validation", 
                     f"✓ Skills extracted: {len(result)} pairs\n"
                     f"✓ Alice skills: {alice_skills}")

    def test_complete_rdf_generation_pipeline(self, test_data_json, debug_logger):
        """
        Test complete RDF generation pipeline with test data.
        
        Demonstrates full transformation from JSON to RDF-like structures,
        simulating a realistic mapping scenario.
        """
        debug_logger("Test: Complete RDF Generation Pipeline", 
                     "Objective: Transform test data to RDF triples\n"
                     f"Input data:\n{json.dumps(test_data_json, indent=2)}")
        
        # Stage 1: Extract team members with roles and skills
        source_op = JsonSourceOperator(
            source_data=test_data_json,
            iterator_query="$.team[*]",
            attribute_mappings={
                "member_id": "$.id",
                "member_name": "$.name",
                "role": "$.roles[*]",
                "skill": "$.skills[*]"
            }
        )
        
        debug_logger("Pipeline Stage 1: Source Operator", 
                     "Extracting: member_id, member_name, role, skill\n"
                     "Expected Cartesian product: ID × roles × skills")
        
        # Stage 2: Generate subject IRI
        subject_expr = FunctionCall(
            function=to_iri,
            arguments=[
                Reference("member_id"),
                Constant("http://example.org/person/")
            ]
        )
        pipeline = ExtendOperator(source_op, "subject", subject_expr)
        
        debug_logger("Pipeline Stage 2: Generate Subject", 
                     "subject = to_iri(member_id, 'http://example.org/person/')")
        
        # Stage 3: Add RDF type
        pipeline = ExtendOperator(
            pipeline,
            "rdf_type",
            Constant(FOAF_PERSON)
        )
        
        debug_logger("Pipeline Stage 3: Add RDF Type", 
                     "rdf_type = foaf:Person")
        
        # Stage 4: Name as literal
        name_lit_expr = FunctionCall(
            function=to_literal,
            arguments=[
                Reference("member_name"),
                Constant(XSD_STRING.value)
            ]
        )
        pipeline = ExtendOperator(pipeline, "name_literal", name_lit_expr)
        
        debug_logger("Pipeline Stage 4: Name Literal", 
                     "name_literal = to_literal(member_name, xsd:string)")
        
        # Stage 5: Role as literal
        role_lit_expr = FunctionCall(
            function=to_literal,
            arguments=[
                Reference("role"),
                Constant(XSD_STRING.value)
            ]
        )
        pipeline = ExtendOperator(pipeline, "role_literal", role_lit_expr)
        
        debug_logger("Pipeline Stage 5: Role Literal", 
                     "role_literal = to_literal(role, xsd:string)")
        
        # Stage 6: Skill as literal
        skill_lit_expr = FunctionCall(
            function=to_literal,
            arguments=[
                Reference("skill"),
                Constant(XSD_STRING.value)
            ]
        )
        pipeline = ExtendOperator(pipeline, "skill_literal", skill_lit_expr)
        
        debug_logger("Pipeline Stage 6: Skill Literal", 
                     "skill_literal = to_literal(skill, xsd:string)")
        
        # Execute full pipeline
        result = pipeline.execute()
        
        debug_logger("Pipeline Execution Complete", 
                     f"Total RDF tuple structures: {len(result)}\n"
                     f"First 5 tuples:\n" + 
                     "\n".join(f"  {i+1}. {tuple}" for i, tuple in enumerate(result[:5])))
        
        # Validate results
        assert len(result) > 0
        
        # Check structure of first tuple
        first = result[0]
        assert "subject" in first
        assert "rdf_type" in first
        assert "name_literal" in first
        assert "role_literal" in first
        assert "skill_literal" in first
        
        # Type checks
        assert isinstance(first["subject"], IRI)
        assert isinstance(first["rdf_type"], IRI)
        assert isinstance(first["name_literal"], Literal)
        assert isinstance(first["role_literal"], Literal)
        assert isinstance(first["skill_literal"], Literal)
        
        # Calculate expected tuple count
        # Alice: 2 roles × 2 skills = 4
        # Bob: 1 role × 1 skill = 1
        # Total: 5
        assert len(result) == 5
        
        # Group by person
        alice_tuples = [t for t in result if t["member_name"] == "Alice"]
        bob_tuples = [t for t in result if t["member_name"] == "Bob"]
        
        debug_logger("Result Analysis", 
                     f"Total tuples: {len(result)}\n"
                     f"Alice tuples: {len(alice_tuples)}\n"
                     f"Bob tuples: {len(bob_tuples)}\n\n"
                     f"Sample Alice tuple:\n"
                     f"  Subject: {alice_tuples[0]['subject']}\n"
                     f"  Type: {alice_tuples[0]['rdf_type']}\n"
                     f"  Name: {alice_tuples[0]['name_literal']}\n"
                     f"  Role: {alice_tuples[0]['role_literal']}\n"
                     f"  Skill: {alice_tuples[0]['skill_literal']}")
        
        assert len(alice_tuples) == 4
        assert len(bob_tuples) == 1
        
        # Verify Alice's combinations
        alice_combos = {
            (t["role_literal"].lexical_form, t["skill_literal"].lexical_form)
            for t in alice_tuples
        }
        expected_alice_combos = {
            ("Dev", "Python"), ("Dev", "RDF"),
            ("Admin", "Python"), ("Admin", "RDF")
        }
        assert alice_combos == expected_alice_combos
        
        debug_logger("Validation Complete", 
                     f"✓ Pipeline executed successfully\n"
                     f"✓ {len(result)} RDF-like tuples generated\n"
                     f"✓ All tuples properly typed (IRI/Literal)\n"
                     f"✓ Cartesian product correct\n"
                     f"✓ Alice combinations: {alice_combos}")

    def test_mapping_file_structure(self, mapping_yaml_path, debug_logger):
        """
        Test loading and validating mapping configuration file.
        
        Validates that the YAML mapping file is properly formatted.
        Note: Requires PyYAML library for full parsing.
        """
        debug_logger("Test: Mapping File Structure", 
                     f"Objective: Validate mapping file\n"
                     f"File: {mapping_yaml_path}")
        
        # Check file exists
        assert mapping_yaml_path.exists(), f"Mapping file not found: {mapping_yaml_path}"
        
        # Read raw content
        with open(mapping_yaml_path, 'r') as f:
            content = f.read()
        
        debug_logger("Mapping File Content", content)
        
        # Basic validation
        assert "prefixes:" in content
        assert "mappings:" in content
        assert "logicalSource:" in content
        
        # Try to parse with PyYAML if available
        try:
            import yaml
            with open(mapping_yaml_path, 'r') as f:
                mapping_data = yaml.safe_load(f)
            
            debug_logger("Parsed Mapping Structure", 
                         f"Prefixes: {list(mapping_data.get('prefixes', {}).keys())}\n"
                         f"Mappings count: {len(mapping_data.get('mappings', []))}")
            
            assert "prefixes" in mapping_data
            assert "mappings" in mapping_data
            
            debug_logger("Validation", 
                         "✓ YAML mapping file parsed successfully\n"
                         f"✓ Prefixes defined: {len(mapping_data['prefixes'])}\n"
                         f"✓ Mappings defined: {len(mapping_data['mappings'])}")
            
        except ImportError:
            debug_logger("Note", 
                         "PyYAML not installed, skipping full parse validation")
            debug_logger("Validation", 
                         "✓ Mapping file exists and contains expected keywords")

    def test_project_metadata_extraction(self, test_data_json, debug_logger):
        """
        Test extraction of project-level metadata.
        
        Validates extraction of top-level attributes from test data.
        """
        debug_logger("Test: Project Metadata Extraction", 
                     "Objective: Extract project-level information")
        
        operator = JsonSourceOperator(
            source_data=test_data_json,
            iterator_query="$",
            attribute_mappings={
                "project_name": "$.project"
            }
        )
        
        result = operator.execute()
        
        debug_logger("Execution Result", 
                     f"Metadata tuples: {len(result)}\n"
                     f"Project: {result[0]['project_name'] if result else 'N/A'}")
        
        assert len(result) == 1
        assert result[0]["project_name"] == "MCP-SPARQLLM"
        
        debug_logger("Validation", 
                     f"✓ Project metadata extracted\n"
                     f"✓ Project name: {result[0]['project_name']}")
