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


## 2. Features

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

## 3. Command Line Interface (CLI)

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

## 4. Python API Usage
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

## 5. Project Structure

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
├── serialiazers/      # Serialization utilities
│   └── NTriplesSerializer.py # N-Triples output
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

## 6. Testing
To run the test suite, ensure you have installed the testing dependencies and execute:

```bash
# Run all tests
pytest tests/

# Run specific category
pytest tests/test_suite/test_13_equijoin_operator.py
```

### Test Coverage
* Cartesian Product Flattening
* Recursive Expression Evaluation
* Multi-Source Union & Joins
* Strict Mode Projections
* Real-world Data Integration Scenarios

## 7. Authors

This project is developed by:

* **Anthony MUDET**
* **Léo FERMÉ**
* **Mohamed Lamine MERAH**

### 7.1. Supervision

This project is supervised by:

* **Full Professor Pascal MOLLI**
* **Full Professor Hala SKAF-MOLLI**
* **Associate Professor Gabriela MONTOYA**

## 8. License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 9. Acknowledgements

We would like to thank the LS2N and GDD team for their support and resources provided during this project.
We also acknowledge the foundational work of Olaf Hartig, which inspired this implementation.

## 10. Contact

For any questions or contributions, please open an issue or contact the authors directly.

