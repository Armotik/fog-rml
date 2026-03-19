# pyhartig

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://python.org/downloads)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> A Python implementation of the formal algebra for Knowledge Graph Construction, based on the work of Olaf
> Hartig. [An Algebraic Foundation for Knowledge Graph Construction](https://arxiv.org/abs/2503.10385)

---

## Table of Contents

- [1. Project Context](#1-project-context)
- [2. Quick Start](#2-quick-start)
- [3. Features](#3-features)
- [4. Command Line Interface (CLI)](#4-command-line-interface-cli)
- [5. Python API Usage](#5-python-api-usage)
- [6. Project Structure](#6-project-structure)
- [7. Testing](#7-testing)
- [8. Authors](#8-authors)
- [9. License](#9-license)
- [10. Acknowledgements](#10-acknowledgements)
- [11. Contact](#11-contact)

## 1. Project Context

This library is a research project developed for the **"Engineering For Research I"** module.

It is part of the **M1 Computer Science, SMART Computing Master's Program** at **Nantes Université**.

The project is hosted by the **LS2N (Laboratoire des Sciences du Numérique de Nantes)**, within the **GDD (Gestion des Données Distribuées) team**.

It serves as the core logical component for the **MCP-SPARQLLM** project, aiming to translate heterogeneous data sources
into RDF Knowledge Graphs via algebraic operators.

---

## 2. Quick Start

### 1. Installation

This project is designed to be installed in "editable" mode for development.

```bash
# Clone the repository
git clone [https://github.com/Armotik/pyhartig](https://github.com/Armotik/pyhartig)
cd pyhartig

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -e '.[test]'
```

### 2. Verify Installation

Run the help command to ensure everything is set up correctly:

```bash
python -m pyhartig --help
```

---


## 3. Features

`pyhartig` provides a set of composable Python objects representing the core algebraic operators for querying
heterogeneous data sources.

Current implementation status covers the foundations required to reproduce **Source**, **Extend**, **Union**, and **Project** operators as defined in the paper:

* **Algebraic Structures**: Strict typing for `MappingTuple`, `IRI`, `Literal`, `BlankNode`, and the special error value `EPSILON` ($\epsilon$).
* **Source Operator**:
    * Extracts data from heterogeneous sources (JSON support implemented).
    * Handles iteration logic (JSONPath) and Cartesian Product flattening for multivalued attributes.
* **Extend Operator**:
    * Dynamically creates new attributes based on expressions (φ).
    * Used to generate IRIs, Literals, or derived values.
* **Union Operator**:
    * Merges data streams from multiple pipelines.
    * Supports Bag Semantics (preserves duplicates) and maintains tuple order.
* **Project Operator**:
    * Restricts the relation to a specific subset of attributes.
    * Enforces strict schema validation (P⊆A).
* **EquiJoin Operator**
    * Joins two data streams based on equality conditions
    * Essential for handling RML Referencing Object Maps (Foreign Keys).
* **Expression System ($\varphi$)**:
    * Composite pattern implementation for recursive expressions.
    * Supports `Constant`, `Reference` (attributes), and `FunctionCall`.
* **Built-in Functions**:
    * Implementation of Annex B functions: `toIRI`, `toLiteral`, `concat`.
    * Strict error propagation handling (Epsilon).
* **RML Mapping Support**:
    * RML Parser: Compiles declarative RML mapping files (.ttl) into an optimized algebraic plan.
    * Built-in Functions: Supports standard transformation functions (toIRI, toLiteral, concat, toBNode).
    * Strict Typing: Operates strictly on RDF Terms (IRI, Literal, BlankNode) rather than raw Python strings.
* **Pipeline Visualization**:
    * `explain()` method for human-readable pipeline trees
    * `explain_json()` method for programmatic access to pipeline structure
    * Detailed expression and operator visualization

## 4. Command Line Interface (CLI)

PyHartig provides a plugin-based CLI architecture for executing mappings and utility tasks.

### 3.1. Execute a Mapping (`run`)

The standard command to execute an RML file and generate N-Triples.
```bash
python -m -v pyhartig run \
    -m mappings/my_mapping.ttl \
    -o output.nt
```
* `-m` / `--mapping`: Path to the RML mapping file (Turtle format).
* `-o` / `--output`: Path to the output N-Triples file.
* `-v` : Verbose mode (logs info). `-vv` for debug.
* `--explain`: Print the algebraic execution plan instead of running it.

### 3.2. Multi-Repo Aggregation (list-issues)
A specialized command demonstrating the dynamic generation of Knowledge Graphs from multiple GitHub/GitLab repositories.
```bash
python -m pyhartig -vv list-issues \
     https://github.com/facebook/react \
     https://github.com/tensorflow/tensorflow \
     https://gitlab.com/gitlab-org/gitlab \
     https://gitlab.com/inkscape/inkscape \
     -m data/mappings/commands/issue_template.ttl \
     -o output_issues.nt
```
* **Auto-Detection**: Automatically detects GitHub vs GitLab URLs.
* **Aggregation**: Fetches data from APIs and merges them into unified sources.
* **Templating**: Injects data into the provided RML template before execution.

### 3.3. Run External RML Conformance Tests
Use the conformance runner to execute external RML test cases and compare pyhartig output with expected RDF.

```bash
python scripts/run_rml_conformance.py \
    --tests-dir external/rml-test-cases/test-cases \
    --output-dir external/rml-test-cases/results
```

Optional flags:
* `--suite <name>`: Restrict execution to a sub-suite/folder.
* `--verbose`: Print per-case execution details.

Example (single case):

```bash
python scripts/run_rml_conformance.py \
    --tests-dir external/rml-test-cases/test-cases/RMLTC0007g-JSON \
    --output-dir external/rml-test-cases/results
```

Behavior summary:
* Detects mapping + expected output files per case.
* Executes `pyhartig run` with an output format matching the expected file extension.
* Compares output via RDF graph isomorphism (triples/quads aware).
* Uses `metadata.csv` when available to track expected-error cases.
* Writes per-case artifacts to `results/<case>/` (mapping/resources + `output_pyhartig.*`).

## 5. Python API Usage
You can embed the engine directly in your own Python scripts.

### 4.1. **Basic Pipeline Execution**
```python
from pyhartig.mapping.MappingParser import MappingParser

# 1. Initialize the parser
parser = MappingParser("data/mappings/fusion_mapping.ttl")

# 2. Parse into an algebraic pipeline (Operator Tree)
pipeline = parser.parse()

# 3. (Optional) Visualize the plan
print(pipeline.explain())

# 4. Execute (Lazy Iterator)
results = pipeline.execute()

# 5. Process results
for row in results:
    # row is an immutable MappingTuple
    print(f"{row['subject']} {row['predicate']} {row['object']} .")
```

### 4.2. **Manual Pipeline Construction**
```python
from pyhartig.operators.sources.JsonSourceOperator import JsonSourceOperator
from pyhartig.operators.ExtendOperator import ExtendOperator
from pyhartig.functions.builtins import to_iri, concat
from pyhartig.expressions import FunctionCall, Reference, Constant

# 1. Define Source
source = JsonSourceOperator(
    source_data={"users": [{"id": 1, "name": "Alice"}]},
    iterator_query="$.users[*]",
    attribute_mappings={"uid": "id", "name": "name"}
)

# 2. Add Transformation (URI Generation)
# Expression: to_iri(concat("[http://ex.org/](http://ex.org/)", Ref("uid")))
uri_expr = FunctionCall(
    to_iri,
    [FunctionCall(concat, [Constant("[http://ex.org/](http://ex.org/)"), Reference("uid")])]
)

pipeline = ExtendOperator(source, "subject", uri_expr)

# 3. Execute
for row in pipeline.execute():
    print(row)
```

### 4.3. Registering FnO functions (FunctionRegistry)

This project supports FnML/FnO style extension functions by exposing a small plugin registry and
runtime resolution mechanism. The goal is to let mapping authors refer to functions by standard FnO URIs
while implementers supply Python callables that perform the actual work — without editing core code.

What it does
- Provides a global `FunctionRegistry` to bind FnO URI strings to Python callables.
- Parses `fnml:functionValue` / `fno:executes` mapping constructs into `FunctionCall` expression nodes.
- Resolves and invokes registered callables at evaluation time, supporting nested function values and
    reference/constant arguments.

How it works (high level)
- Register: call `FunctionRegistry.register(uri, callable)` from your application or a plugin module.
- Parse: `MappingParser` converts `fnml:functionValue` declarations into `FunctionCall(func_iri, args...)`.
- Resolve: when `FunctionCall.evaluate()` runs, if its `function` is a string IRI the registry is queried
    for the corresponding callable; resolution is lazy so registration may occur before or after parsing.
- Execute: the callable is invoked with evaluated positional arguments; its return value is inserted into the
    algebraic pipeline (preferably as a `pyhartig` RDF term).

Inputs and outputs
- Mapping-time input: a term map node using `fnml:functionValue` and an `fno:executes` URI, plus any
    predicateObjectMap parameters (constants, `rml:reference`, nested functionValues).
- Runtime input to the Python callable: evaluated argument values (often `pyhartig.algebra.Terms.Literal`,
    `IRI`, or `BlankNode` objects). Callables should unwrap `.lexical_form` / `.value` as needed.
- Output from the callable: ideally a `pyhartig` RDF term (`IRI`, `Literal`, `BlankNode`) so the pipeline can
    use it directly. Returning raw Python primitives is tolerated but may require normalization.

Error semantics
- If an argument evaluates to `EPSILON`, or if the function lookup fails, or the callable raises an exception,
    the `FunctionCall` returns `EPSILON` to preserve strict error propagation across the algebra.

Why this is beneficial
- Extensibility: add custom transformations without modifying `pyhartig` core sources; register functions from
    application code or plugin modules.
- Decoupling: mappings remain declarative (use FnO URIs); implementations live in Python and can be tested
    independently and shared across projects.
- Interoperability: supports FnML/FnO standard patterns so mappings authored for other engines can reference
    the same URIs.

Security and best practices
- Registered callables execute arbitrary Python code — only register trusted code in production.
- Validate and coerce inputs inside the callable; prefer returning `pyhartig` RDF term objects.
- Catch exceptions and return `EPSILON` or a controlled default to avoid crashing the pipeline.

Example
```python
from pyhartig.functions.registry import FunctionRegistry

def my_upper(arg):
        val = getattr(arg, 'lexical_form', None) or getattr(arg, 'value', None) or arg
        return str(val).upper()

FunctionRegistry.register("http://example.com/fn/upper", my_upper)
```

References / where to look in the codebase
- `pyhartig/functions/registry.py` — registry API and storage
- `pyhartig/expressions/FunctionCall.py` — evaluation and EPSILON semantics
- `pyhartig/mapping/MappingParser.py` — FnML parsing and `FunctionCall` construction
- `tests/test_suite/test_18_fnml_plugins.py` and `tests/test_suite/test_19_fno.py` — concrete examples and tests

Notes
- Registration is lazy — functions can be registered at any time before evaluation.
- Prefer returning RDF term objects from callables to avoid ambiguity and preserve typing.

## 5.4. SPARQL SERVICE-CALL Example

This project includes a pragmatic SPARQL integration that demonstrates running RML mappings per-repository
and querying the materialized named graphs via a `SERVICE-CALL`-style pattern.

Quick run (from project root):

```bash
# activate your virtualenv
source .venv/bin/activate          # Unix/macOS
.venv\Scripts\Activate.ps1       # PowerShell on Windows

# ensure rdflib is installed
pip install rdflib

# run the example demo (prints resulting quads)
python pyhartig/examples/multi_repo_service_demo.py
```

What the demo does
- Locates example mapping files under `pyhartig/examples/data/` for repositories (r1, r2, r3).
- For each `?repo` token it runs the corresponding mapping (via `MappingParser`) and materializes output into
    a named graph (IRI derived from the repo token).
- Executes a SPARQL query using `BIND SERVICE-CALL(?repo, "mapping.ttl") AS ?g` plus `GRAPH ?g { ... }`
    to read data from the materialized graphs; the demo prints materialized quads in a readable format.

Using the handler from Python code

```python
from rdflib import Dataset
from pathlib import Path
from pyhartig.sparql.service_call import execute_query_with_service_call

ds = Dataset()
query = '''
PREFIX ex: <http://example.org/>
SELECT ?repo ?x ?y ?title WHERE {
    VALUES ?repo { <http://example.org/r1> <http://example.org/r2> <http://example.org/r3> }
    BIND SERVICE-CALL(?repo, "mapping.ttl") AS ?g
    GRAPH ?g { ?x ex:issue ?y . ?y ex:title ?title }
}
'''

res = execute_query_with_service_call(ds, query, Path('pyhartig/examples/data'))
for row in res:
        print(row)
```

Notes and recommendations
- The handler runs mappings and injects `VALUES ?g` clauses; when rewritten queries return no rows it falls back to
    a per-graph evaluation to guarantee results across different SPARQL engine behaviors.
- Ensure the mapping files referenced by `SERVICE-CALL` exist in the `mapping_dir` you pass to the handler.
- The demo is an example and can be adapted for production: replace ad-hoc file resolution with repository
    metadata and register any FnO functions your mappings require.

## 6. Project Structure

```text
src/pyhartig/
├── algebra/            # Core algebraic definitions
│   ├── Terms.py        # RDF Terms (IRI, Literal, BlankNode)
│   └── Tuple.py        # MappingTuple and Epsilon
├── commands/          # CLI command implementations
│   ├── base.py         # Base command class
│   ├── run.py          # Standard mapping execution command
│   └── list_issues.py  # Multi-repo GitHub/GitLab
├── expressions/        # Recursive expression system 
│   ├── Expression.py   # Abstract base class
│   ├── Constant.py     # Constant values
│   ├── Reference.py    # Attribute references
│   └── FunctionCall.py # Extension function applications
├── functions/          # Extension functions
│   └── builtins.py     # Implementation of toIRI, concat, etc.
├── mapping/            # RML Mapping Parser
│   └── MappingParser.py # Parses RML files into operator pipelines
├── serializers/      # Serialization utilities
│   ├── NTriplesSerializer.py # N-Triples output
│   └── NQuadsSerializer.py # N-Quads output
├── operators/          # Algebraic Operators
│   ├── Operator.py     # Abstract base class for all operators
│   ├── SourceFactory.py # Factory for creating Source operators
│   ├── EquiJoinOperator.py # EquiJoin operator implementation
│   ├── ExtendOperator.py # Extend operator implementation
│   ├── ProjectOperator.py # Project operator implementation
│   ├── UnionOperator.py  # Union operator implementation
│   ├── SourceOperator.py # Abstract Source operator
│   └── sources/        # Source operator implementations
│       └── CsvSourceOperator.py # CSV data source operator
│       └── JsonSourceOperator.py # JSON data source operator
│       └── XmlSourceOperator.py # XML data source operator
│       └── SparqlSourceOperator.py # SPARQL source operator
│       └── MysqlSourceOperator.py # MySQL source operator
│       └── PostgresqlSourceOperator.py # PostgreSQL source operator
│       └── SqlserverSourceOperator.py # SQL Server source operator
├── namespaces.py    # Common RDF namespaces
└── __main__.py       # Entry point for CLI
tests/                  # Unit tests for all components
├── use_cases/        # Example usage scripts
│   └── github_gitlab/ # Example with GitHub and GitLab data
└── test_suite/     # Comprehensive test suite
data/               # Sample data files for testing
└── mappings/       # RML mapping files
    └── commands/   # Mappings for CLI commands
LICENSE                 # MIT License
README.md               # Project documentation
CHANGELOG.md            # Project changelog
pyproject.toml          # Project configuration and dependencies
requirements.txt        # Additional dependencies
```

## 7. Testing
To run the test suite, ensure you have installed the testing dependencies and execute:

```bash
# Run all tests
pytest tests/

# Run specific category
pytest tests/test_suite/test_13_equijoin_operator.py
```

### 6.1. Initial Regression Suite
Run the project regression suite (quiet mode):

```bash
python -m pytest -q
```

### 6.2. RML Conformance Suite
Run the external RML conformance checks:

```bash
python scripts/run_rml_conformance.py \
    --tests-dir external/rml-test-cases/test-cases \
    --output-dir external/rml-test-cases/results
```

The conformance summary reports:
* total executed cases,
* passed/failed counts,
* expected-error pass/fail counts,
* coverage percentage.

Generated outputs are retained under `external/rml-test-cases/results` in per-case folders for inspection.

### Test Coverage
* Cartesian Product Flattening
* Recursive Expression Evaluation
* Multi-Source Union & Joins
* Strict Mode Projections
* Real-world Data Integration Scenarios

## 8. Authors

This project is developed by:

* **Anthony MUDET**
* **Léo FERMÉ**
* **Mohamed Lamine MERAH**

### 8.1. Supervision

This project is supervised by:

* **Full Professor Pascal MOLLI**
* **Full Professor Hala SKAF-MOLLI**
* **Associate Professor Gabriela MONTOYA**

## 9. License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 10. Acknowledgements

We would like to thank the LS2N and GDD team for their support and resources provided during this project.
We also acknowledge the foundational work of Olaf Hartig, which inspired this implementation.

## 11. Contact

For any questions or contributions, please open an issue or contact the authors directly.

