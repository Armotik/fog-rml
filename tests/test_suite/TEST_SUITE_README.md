# fog-rml Test Suite

## Overview

The test suite is now organized as a mirror of `src/fog_rml/`.

For each source file in `src/fog_rml/**`, there is a corresponding test module in
`tests/test_suite/fog_rml/**`. Each mirrored test module contains:

- at least one `coverage_suite` test used by SonarQube coverage runs
- at least one `edge_case` test used to exercise robustness paths

This structure makes it easier to see which source module is covered and where
edge-case validation lives.

## Execution Categories

Two logical categories are available through `pytest --suite`:

- `coverage`: the coverage-oriented suite used in CI and SonarQube
- `edge_case`: the robustness-oriented suite focused on invalid, empty, or unusual inputs

Commands:

```bash
.venv/bin/python -m pytest tests/test_suite --suite coverage -q
.venv/bin/python -m pytest tests/test_suite --suite edge_case -q
.venv/bin/python -m pytest tests/test_suite -q
```

Equivalent marker-based commands are also available:

```bash
.venv/bin/python -m pytest tests/test_suite -m coverage_suite -q
.venv/bin/python -m pytest tests/test_suite -m edge_case -q
```

## Layout

The main mirrored directories are:

- `tests/test_suite/fog_rml/`
- `tests/test_suite/fog_rml/algebra/`
- `tests/test_suite/fog_rml/commands/`
- `tests/test_suite/fog_rml/expressions/`
- `tests/test_suite/fog_rml/functions/`
- `tests/test_suite/fog_rml/mapping/`
- `tests/test_suite/fog_rml/operators/`
- `tests/test_suite/fog_rml/operators/sources/`
- `tests/test_suite/fog_rml/serializers/`
- `tests/test_suite/fog_rml/sparql/`
- `tests/test_suite/fog_rml/utils/`

Examples:

- `src/fog_rml/commands/run.py` -> `tests/test_suite/fog_rml/commands/test_run.py`
- `src/fog_rml/operators/SourceFactory.py` -> `tests/test_suite/fog_rml/operators/test_SourceFactory.py`
- `src/fog_rml/operators/sources/JsonSourceOperator.py` -> `tests/test_suite/fog_rml/operators/sources/test_JsonSourceOperator.py`
- `src/fog_rml/sparql/service_call.py` -> `tests/test_suite/fog_rml/sparql/test_service_call.py`

## Fixtures

Shared fixtures live in `tests/test_suite/conftest.py`:

- `project_root`
- `data_dir`
- `dataset`
- `write_mapping_files`
- `stream_to_list`

They are intended to support lightweight module-level tests without coupling the
suite to large integration fixtures or numbered scenario files.
- Complex nested pipeline structures
- Valid JSON serialization verification

**Key Features**:
- Tests the `explain_json()` method for all operator types
- Validates complete serializable pipeline representation
- Ensures proper JSON structure with type information
- Provides programmatic access to pipeline structure

### 12. Project Operator Tests (`test_12_project_operator.py`)

**Objective**: Validate the Project operator for restricting mapping relations to specified attributes.

Based on Definition 11: `Project^P(r) : (A, I) -> (P, I')` where:
- `r = (A, I)`: Source mapping relation with attributes A and instance I
- `P âŠ† A`: Non-empty subset of attributes to retain
- Result: New mapping relation `(P, I')` where `I' = { t[P] | t âˆˆ I }`

**Test Coverage**:
- Single attribute projection
- Multiple attribute projection
- Identity projection (P = A)
- Value preservation verification (`t[P](a) = t(a)`)
- **Strict mode validation**: Missing attribute raises `KeyError`
- Multiple missing attributes error reporting
- Empty source handling
- Operator composition (Project + Extend, Project + Union)
- Chained projections
- Explain functionality (`explain()` and `explain_json()`)
- IRI value preservation
- Duplicate tuple handling (bag semantics)
- Tuple order preservation

**Key Features**:
- Tests the `ProjectOperator` core functionality
- Validates strict mode behavior (P âŠ† A enforced)
- Tests integration with other operators (Source, Extend, Union)
- Validates edge cases and error handling
- Integration tests for RDF generation and heterogeneous schema handling

**Strict Mode Rationale**:
- Safer behavior: detects bugs early when projecting non-existent attributes
- Conforms to classical relational algebra where `P âŠ† A` is required
- Heterogeneous schemas can be handled with `Union` + multiple `Project` operations

**Example - Handling Heterogeneous Schemas**:
```python
# Source A has: id, name, dept
source_a = JsonSourceOperator(data_a, "$.items[*]", {"id": "$.id", "name": "$.name", "dept": "$.dept"})

# Source B has: id, name, role (different schema)
source_b = JsonSourceOperator(data_b, "$.items[*]", {"id": "$.id", "name": "$.name", "role": "$.role"})

# Project each to common schema before union
project_a = ProjectOperator(source_a, {"id", "name"})
project_b = ProjectOperator(source_b, {"id", "name"})

# Union now works with homogeneous schemas
union = UnionOperator([project_a, project_b])
```

**Example Output**:
```json
{
  "type": "Union",
  "parameters": {
    "operator_count": 36
  },
  "children": [
    {
      "type": "Extend",
      "parameters": {
        "new_attribute": "object",
        "expression": {
          "type": "FunctionCall",
          "function": "to_literal",
          "arguments": [
            {
              "type": "Reference",
              "attribute": "created_at"
            },
            {
              "type": "Constant",
              "value_type": "str",
              "value": "http://www.w3.org/2001/XMLSchema#string"
            }
          ]
        }
      },
      "parent": {
        "type": "Extend",
        "parameters": {
          "new_attribute": "predicate",
          "expression": {
            "type": "Constant",
            "value_type": "IRI",
            "value": "http://schema.org/dateCreated"
          }
        },
        "parent": {
          "type": "Source",
          "operator_class": "JsonSourceOperator",
          "parameters": {
            "iterator": "$[*]",
            "attribute_mappings": {
              "number": "number",
              "created_at": "created_at"
            },
            "source_type": "JSON",
            "jsonpath_iterator": "$[*]"
          }
        }
      }
    }
  ]
}
```

## Running the Tests

### Run All Tests

```bash
# Retournez Ã  la racine du projet
cd ../..

# Puis exÃ©cutez les tests
pytest tests/test_suite/ -v -s

# Ou le script de tests
python tests/test_suite/run_all_tests.py

```

### Run Specific Test Categories

```bash
cd ../..

# Source operators only
pytest tests/test_suite/test_01_source_operators.py -v -s

# Extend operators only
pytest tests/test_suite/test_02_extend_operators.py -v -s

# Union operators only
pytest tests/test_suite/test_09_union_operator.py -v -s

# Explain tests
pytest tests/test_suite/test_10_explain.py -v -s

# Explain JSON tests
pytest tests/test_suite/test_11_explain_json.py -v -s

# Project operators only
pytest tests/test_suite/test_12_project_operator.py -v -s

# Complete pipelines
pytest tests/test_suite/test_04_complete_pipelines.py -v -s

# Real data integration
pytest tests/test_suite/test_08_real_data_integration.py -v -s

# All Union-related tests (across all files)
pytest tests/test_suite/ -k union -v -s

# All explain-related tests
pytest tests/test_suite/ -k explain -v -s
```

### Run with Markers

```bash
cd ../.. 
# Unit tests only
pytest tests/test_suite/ -m unit -v

# Integration tests only
pytest tests/test_suite/ -m integration -v
```

## Debug Output

All tests include comprehensive debug output that provides:

1. **Test Objective**: Clear statement of what is being tested
2. **Configuration Details**: Input data, operator setup, expression definitions
3. **Execution Results**: Detailed output including tuple counts and sample data
4. **Validation Summary**: Confirmation of assertions and key findings

### Example Debug Output

```
================================================================================
[DEBUG] Test: Simple Iterator and Extraction
--------------------------------------------------------------------------------
Objective: Extract team member names and IDs
================================================================================

================================================================================
[DEBUG] Configuration
--------------------------------------------------------------------------------
Iterator: $.team[*]
Mappings:
  - person_id: $.id
  - person_name: $.name
================================================================================

================================================================================
[DEBUG] Execution Result
--------------------------------------------------------------------------------
Number of tuples: 2
Tuples:
  1. Tuple(person_id=1, person_name='Alice')
  2. Tuple(person_id=2, person_name='Bob')
================================================================================

================================================================================
[DEBUG] Validation
--------------------------------------------------------------------------------
âœ“ All assertions passed
================================================================================
```

## Test Data Files

The test suite uses the following data files:

- **`data/test_data.json`**: Sample JSON data with team structure
- **`data/mappings/test_mapping.yaml`**: Example RML-like mapping configuration
- **`data/mappings/expected_test_output.nt`**: Expected RDF output (N-Triples format)

## Test Statistics

- **Total Test Files**: 11
- **Total Tests**: 108
- **Test Categories**: Source, Extend, Union, Composition, Pipelines, Functions, Expressions, Libraries, Integration, Explain, Explain JSON
- **Coverage Areas**: Operators, Expressions, Functions, Libraries, Real Data, Multi-Source Merging, Pipeline Visualization
- **Debug Traces**: Comprehensive output for all tests

## Requirements

```
pytest>=7.0.0
jsonpath-ng~=1.7.0
```

Optional for YAML mapping validation:
```
PyYAML>=6.0
```

## Continuous Integration

These tests are designed to be:
- **Automated**: Can run in CI/CD pipelines
- **Reproducible**: Consistent results across environments
- **Documented**: Self-documenting through debug output
- **Comprehensive**: Cover all major system components

## Troubleshooting

### Common Issues

**Import Errors**: Ensure the project is installed in development mode:
```bash
pip install -e .
```

**JSONPath Errors**: Verify jsonpath-ng is installed:
```bash
pip install jsonpath-ng~=1.7.0
```

**Debug Output Not Showing**: Use the `-s` flag with pytest:
```bash
pytest tests/test_suite/ -v -s
```

## License

This test suite is part of the fog-rml project and follows the same license.

---

**Last Updated**: 2025-12-09
**Test Suite Version**: 2.1.0

