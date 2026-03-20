# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### TODO

#### Demo & Use Cases
- Exemple avec structure JSON bien plus complexe (niveaux imbriqués, tableaux dans tableaux)
- Exemple avec des règles plus compliquées et utiliser chaque Operator (Project, Join, Union, Extend)
- Faire une commande exemple cli source + mapping -> output (pas le use case)
- **Implement "Multi-Repo Issues" Demo Scenario**:
  - **Goal**: Aggregate issues from multiple heterogeneous sources (GitHub & GitLab) into a single Knowledge Graph.
  - **CLI Tool**: Create a specific command `listissues` (just an example name might be better) :
    ```bash
    python listissues repo1-github repo2-gitlab repo3-github [params]
    ```
  - **SPARQL Integration**: Demonstrate querying the virtual graph via a `SERVICE` call:
    ```sparql
    Select * {
      Values ?repo {r1 r2 r3}
      Bind SERVICE-CALL(?repo, "mapping.ttl") as ?g
      Graph ?g {
        ?x ex:issue ?y .
        ?y ex:title ?title
      }
    }
    ```
(Possible Request but not the best, might find a better one)
- Add 2-3 standalone example programs: Create ready-to-run scripts in `examples/` demonstrating specific features (e.g., CSV+JSON join, complex transformations).

#### Documentation & Onboarding
- Move detailed sections to separate `docs/` files

#### Tests Suite (`tests/test_suite/`)

- `test_01_source_operators.py`: Reduce coupling to JSON: Currently tests only `JsonSourceOperator`, but there are no
  tests for the abstract `SourceOperator` class. Ideally, the base logic (e.g., `itertools.product` for Cartesian
  product) should be tested independently of the JSONPath implementation.
- `test_01_source_operators.py`: Add JSONPath edge case tests: Missing tests for invalid or malformed JSONPath
  expressions. Does it crash or return an empty list?
- `test_02_extend_operators.py`: Improve expression typing: Tests use many raw strings. To be rigorous with the algebra,
  tests should use `Constant(Literal(...))` instead of raw Python strings.
- `test_04_complete_pipelines.py`: Reduce test redundancy: This file duplicates many tests already covered in `test_03`.
  While useful for demonstration, this creates maintenance overhead - if `Extend` breaks, 50 tests will fail instead of
    5. **Suggested fix**: Consider refactoring to focus on integration scenarios not covered elsewhere, or use
       fixtures/parameterization to reduce duplication.
- `test_05_builtin_functions.py`: Add negative IRI validation tests: As noted in the code analysis, current tests
  validate that any string becomes an IRI. Missing negative tests: `to_iri("spaces not allowed")` should return
  `EPSILON`, but current tests would likely pass by creating an invalid IRI. **Suggested fix**: Add tests that verify
  invalid IRI strings (with spaces, without scheme, etc.) return `EPSILON`.
- `test_06_expression_system.py`: Verify Reference behavior for missing attributes:
  `test_reference_nonexistent_attribute` asserts that referencing a missing attribute returns `EPSILON`. However, the
  code uses `t[self.attribute]` which should raise `KeyError`. If this test passes, it's correct per Definition 9, but
  verify why it passes - either `MappingTuple.__getitem__` is modified to not raise `KeyError`, or there's unexpected
  behavior. **Action**: Confirm this aligns with the critical TODO in `Reference.py`.
- `test_07_library_integration.py`: Accessory tests: These are tests of dependencies, not tests of the project's own
  code. Consider moving to a separate folder or marking as optional/smoke tests.
- `test_08_real_data_integration.py`: Fragile tests: Depends on files on disk. If someone changes `test_data.json`,
  tests break. **Suggested fix**: Use fixtures with data injected directly into the test (as done elsewhere) to improve
  test isolation and reliability.
- `test_09_union_operator.py`: Add more tuple order tests: Missing tests for exact ordering if order matters. While
  `test_union_preserves_tuple_order` partially covers this, more comprehensive tests could verify order guarantees
  across different scenarios.
- `test_10_explain.py` / `test_11_explain_json.py`: Fragile text-based tests: Text tests in `test_10` are fragile - if a
  single space changes in `explain()` formatting, the test breaks. `test_11` (JSON) is much more robust and relevant. *
  *Suggested fix**: Consider relaxing string comparisons in `test_10` (e.g., check for key substrings rather than exact
  matches) or rely primarily on `test_11` for validation.
- `test_13_equijoin_operator.py`: Add load/performance test: Since the implementation uses O(N×M) Nested Loop, current
  tests pass instantly with only 4 rows. Missing a test with 1000+ rows on each side to "feel" the slowness and justify
  the need for Hash Join optimization.
- `test_13_equijoin_operator.py`: Verify NULL join semantics: In `test_equijoin_with_null_values`, `None = None` appears
  to match (True). In standard SQL, `NULL = NULL` is False. This is a debatable semantic choice for joins. The paper
  does not specify this case (since `ε` typically does not propagate in strict equality). **Action**: Document the
  chosen semantics or align with SQL NULL behavior.

#### Testing & Quality
- Confirm execution of all current tests (Regression testing).
- Integrate external RML conformance tests (Compatibility):
  - [RML Test Cases](https://rml.io/test-cases/) (done)
  - [PyRML Test Suite](https://github.com/anuzzolese/pyrml)
  - [RMLMapper-Java Test Suite](https://github.com/RMLio/rmlmapper-java)
  - [Morph-KGC Test Suite](https://github.com/morph-kgc/morph-kgc)
  - (test cases)

#### General Improvements

##### Logging vs Print (Improvement)

- Replace `print()` with `logging` module: In `MappingParser.py` (and elsewhere), `print(f"Error: ...")` or try/except
  blocks print errors. This pollutes stdout, which is problematic if users want to redirect output (
  `python -m pyhartig ... > output.nt`). **Suggested fix**: Use standard `logging` module with
  `logger = logging.getLogger(__name__)`.

##### Output Serialization (Missing)

- Add RDF Serializer: `execute()` returns `List[MappingTuple]`, which is good for algebra but not RDF. Need a Serializer
  class/function that takes final tuples and writes them in N-Triples or Turtle format. For each tuple, extract
  `subject`, `predicate`, `object` (and `graph`), verify they are valid RDF terms (`IRI`, `Literal`, `BNode`), and
  format the line (e.g., `<http://s> <http://p> "o" .`).

#### Architectural Improvements

##### Visitor Pattern for `explain()` and `execute()` (Refactoring)

- Implement Visitor Pattern: Currently, each operator class (`Source`, `Project`, `Union`...) contains its own logic for
  `explain()` and `explain_json()`. This violates the Single Responsibility Principle (SRP) - an operator should only
  know how to perform its operation, not how to display itself. If we want to export the plan to GraphViz (`.dot`) or
  Mermaid.js in the future, we would need to modify all operator classes. **Suggested fix**: Operators should only have
  an `accept(visitor)` method. Create `ExplainVisitor`, `JsonExplainVisitor`, and later `OptimizationVisitor` classes.
  This separates structure from algorithm and is academically appreciated for manipulating expression trees.

##### Code Quality and Standardization (DevOps)

- Add linting and formatting configuration: `pyproject.toml` is missing configuration for Black (auto-formatting) or
  Ruff/Pylint. Risk of having different code styles between contributors. **Action**: Add `[tool.ruff]` or
  `[tool.black]` section in `pyproject.toml` and enforce it in CI.
- Enable static type checking (Mypy): Type hints are used (`List[MappingTuple]`) but without Mypy in CI, they only serve
  as documentation. Enabling Mypy would detect subtle bugs (e.g., passing `None` where an `Operator` is expected). *
  *Action**: Add `[tool.mypy]` configuration and integrate into CI pipeline.

##### Path and Resource Management (Improvement)

- Remove global `os.chdir()` usage: In `github_gitlab_test.py`, changing the Current Working Directory globally is
  dangerous (especially in multithreading or web applications). `MappingParser` and `SourceOperator` should handle
  absolute or relative paths (relative to the mapping file) without requiring `os.chdir()`. **Suggested fix**: Use
  `pathlib.Path` everywhere for robust path resolution.

##### Function Ontology (FnO) Extension (Feature)

- Implement dynamic function registry: Currently, functions (`concat`, `to_iri`) are hardcoded in `builtins.py`. RML
  normally allows calling any custom function via FnO (Function Ontology). If a user wants `to_uppercase` or
  `convert_currency`, they cannot add it without modifying pyhartig source code. **Suggested fix**: Implement a plugin
  mechanism to dynamically register external functions:
  ```python
  FunctionRegistry.register("http://ex.org/functions#toUpper", my_python_func)
  ```

##### Technical Documentation (Enhancement)

- Generate API documentation with Sphinx/MkDocs: The README is excellent for an overview, but docstrings are high
  quality (e.g., `ProjectOperator` explains Definition 11 well) and remain buried in code. Generating a static
  documentation site would be a significant improvement for project presentation. **Action**: Set up Sphinx or MkDocs
  with autodoc to generate documentation from docstrings.

##### Serialization Module (Missing - Critical)

- Extract serialization logic from tests: In `github_gitlab_test.py` (lines 90-135), RDF serialization code is written
  manually in the test file. This logic should be in the library, not in tests. **Action**: Create
  `src/pyhartig/serializers/NTriplesSerializer.py` with proper character escaping handling. Tests should not contain
  business logic.
- Implement proper RDF serializers: Create serializer classes for N-Triples, Turtle, and potentially N-Quads formats
  with proper escaping and validation.

##### Memory Management and Streaming (Critical - Architecture)

- **[CRITICAL]** Switch to Generator-based streaming: The entire project relies on `List[]`. Loading a 500 MB JSON file
  will likely consume 2-3 GB of RAM and crash Python. This is the most important architectural improvement for project
  viability. **Action**: Replace `List[MappingTuple]` returns with `Iterator[MappingTuple]` using `yield` throughout the
  codebase. This differentiates a "student project" from a viable "ETL engine".

## [0.3.5] - 2026-03-20

### Changed
- Reorganized the test suite into explicit `coverage` and `edge_case` categories, added runner support for `--suite`, and aligned the SonarCloud workflow with the coverage category.
- Rebuilt `tests/test_suite` as a mirrored `src/pyhartig` test tree, with one dedicated test module per source file and separate `coverage_suite` / `edge_case` checks in each mirrored area.
- Expanded the mirrored test suite with targeted branch coverage on factories, source operators, serializers, joins, and builtins to raise local source coverage above 90%.
- Fixed invalid helper calls in `ExtendOperator.explain()` / `_explain_expression()` to match the actual helper signature.
- Refactored CSV, JSON, SQL fixture, SQL Server, source factory, source, union, and equi-join operators into smaller helpers; standardized iterable return types where operators materialize `StreamRows`.
- Aligned operator and serializer type hints with actual returned values, including `NQuadsSerializer.serialize()` and iterable-producing operator `execute()` methods.
- Renamed EquiJoin constructor join-attribute parameters to comply with naming conventions and split join execution into focused indexing/probing helpers.
- Simplified or replaced regex patterns flagged in SQL fixture helpers and pinned external GitHub Actions in `sonarcloud.yml` to commit references.

## [0.3.4] - 2026-03-20

### Changed
- Centralized additional intentional HTTP IRIs and FnO/FnML namespaces in `namespaces.py` to reduce false-positive hotspot reports without changing RDF/FnO semantics.
- Replaced remaining hardcoded intentional HTTP/FnO IRIs in `builtins.py`, `idlab_plugins.py`, `MappingParser.py`, `SparqlSourceOperator.py`, and `service_call.py` with centralized constants or scheme-based checks.
- Refactored the `run` command into smaller helpers to reduce cognitive complexity without changing execution behavior.
- Refactored `builtins.to_iri()` into smaller helpers to reduce cognitive complexity without changing IRI resolution semantics.
- Simplified the blank-node identifier regex in `builtins.to_bnode()` without changing ASCII identifier handling.
- Renamed non-snake-case helper functions in `idlab_plugins.py` while keeping the same registered IDLab function URIs.
- Refactored `service_call.execute_query_with_service_call()` into focused SERVICE-CALL preparation, rewrite, and fallback helpers while standardizing its materialized list return type.
- Replaced slow regex hotspots in `service_call.py` and `MappingParser.py` with deterministic string parsing for `SELECT` clause extraction and FnML parameter suffix ordering.
- Refactored `SparqlSourceOperator._validate_sparql_query()` into focused validation helpers to reduce cognitive complexity without changing SPARQL query validation behavior.
- Refactored `SparqlSourceOperator._emulate_from_local_rdf()` into focused local-resource resolution and RDF-query helpers to reduce cognitive complexity without changing local SPARQL emulation behavior.
- Simplified the ASCII identifier regex in `SparqlSourceOperator._normalize_attribute_mappings()` without changing attribute normalization behavior.
- Simplified the SPARQL `SELECT` variable regex in `SparqlSourceOperator._find_duplicate_select_vars()` without changing duplicate-variable detection behavior.
- Simplified `MappingParser._flush_literal_buffer()` and related template parsing helpers as part of the cognitive-complexity cleanup in `MappingParser.py`.
- Refactored `MappingParser._iter_template_segments()` into smaller brace-handling helpers to reduce cognitive complexity without changing template parsing behavior.
- Refactored `MappingParser._create_ext_expr()` into dedicated FnML, constant, reference, template, and literal-handling helpers to reduce cognitive complexity without changing extension-expression semantics.
- Refactored `MappingParser._find_candidate_parent_triples_map()` into dedicated source-resolution and candidate-matching helpers to reduce cognitive complexity without changing parent TriplesMap selection behavior.
- Refactored `MappingParser._extract_join_attributes()` into dedicated operand-extraction and pair-appending helpers to reduce cognitive complexity without changing join-attribute extraction behavior.
- Removed the duplicate `MappingParser._normalize_join_name()` implementation by reusing `_normalize_query_name()` for join operand normalization.
- Refactored `MappingParser._extract_queries()` into focused query-normalization and term-map scanning helpers to reduce cognitive complexity without changing extracted attribute mappings.
- Renamed the local `vars` binding in `MappingParser._register_template_query_variables()` to avoid shadowing the Python builtin.
- Renamed the local `vars` binding in `MappingParser._extract_join_operand()` to avoid shadowing the Python builtin.
- Refactored `MappingParser._inject_parent_join_mappings()` into a single-entry helper to reduce cognitive complexity without changing parent join attribute injection behavior.
- Refactored `MappingParser._prepare_parent_source_mappings()` into separate parent-injection and collision-renaming helpers to reduce cognitive complexity without changing parent join mapping behavior.
- Refactored `MappingParser._prepare_child_join_mappings()` into focused helpers for fallback cleanup and join-attribute injection to reduce cognitive complexity without changing join behavior.
- Refactored `MappingParser.parse()` into dedicated loading, TriplesMap, and join helpers to reduce cognitive complexity while preserving pipeline construction behavior.

## [0.3.3] - 2026-03-19

### Changed
- Repaired regressions after rebase by restoring `0.3.3`, builtin function registrations used by FnML, and the security fixes kept from the SonarQube cleanup.

## [0.3.2] - 2026-03-19

### Changed
- Hardened JSON source loading in `SourceFactory` and `JsonSourceOperator` by validating file paths and sanitizing parsed JSON payloads before execution.
- Reused centralized namespace constants in SQL fixture fallback and documented official RDF namespace IRIs to reduce false-positive insecure-HTTP hotspots.
- Replaced additional hardcoded RDF/RML/XSD namespace URIs in Python code and tests with centralized namespace constants where applicable.
- Replaced weak hashing for generated blank node identifiers and simplified hotspot-reported regex handling in mapping, SPARQL, and SQL fixture helpers.

## [0.3.1] - 2026-03-19

### Added
- **SonarQube code quality analysis**: Added SonarQube Cloud integration through CI to analyze the `src/pyhartig` codebase automatically on pushes and pull requests.

### Changed
- **Coverage reporting pipeline**: Added automated `pytest` coverage export (`coverage.xml`) for SonarQube analysis.
- **Coverage scope refinement**: Updated coverage reporting so RML test files are excluded from the reported test coverage metrics.

## [0.3.0] - 2026-03-02

### Added
- **Full RML conformance runner**: Added and stabilized `scripts/run_rml_conformance.py` to execute external RML test-cases end-to-end, compare RDF outputs via graph isomorphism, and report coverage.
- **Expected-error semantics**: Integrated `metadata.csv` handling (`error expected?`) so expected failures are counted correctly and surfaced explicitly in summaries.
- **Per-case output colocation**: Runner now materializes each case under `external/rml-test-cases/results/<case>/` with copied mapping/resources and generated `output_pyhartig.*` for direct inspection.
- **N-Quads serialization support**: Added `src/pyhartig/serializers/NQuadsSerializer.py` and output-format-aware serialization in `run` command when target extension is `.nq`.
- **Database source operators**: Added relational source support with
  - `src/pyhartig/operators/sources/MysqlSourceOperator.py`
  - `src/pyhartig/operators/sources/PostgresqlSourceOperator.py`
  - `src/pyhartig/operators/sources/SqlserverSourceOperator.py`
  - `src/pyhartig/operators/sources/sql_fixture_fallback.py` for SQL fixture emulation.
- **SPARQL local emulation path**: Added `src/pyhartig/operators/sources/SparqlSourceOperator.py` support path in `SourceFactory` for service/query cases backed by local `resource*.ttl` where applicable.

### Changed
- **Mapping normalization/runtime resilience** (`src/pyhartig/mapping/MappingParser.py`):
  - Kept Query 4 and Query 5 normalization blocks aligned with required canonical text.
  - Added runtime fallback/provenance handling for no-join parentTriplesMap and TM clone reconstruction after destructive normalization.
  - Improved parent resolution heuristics and join attribute handling to preserve semantics across edge cases.
  - Added explicit default graph extension (`rr:defaultGraph`) when no graphMap is present on subject/POM branches.
  - Added per-branch projection to `{subject, predicate, object, graph}` before union to align pipeline shape.
  - Added `rr:datatype` handling for reference/template-valued term maps, with validation for invalid language+datatype combinations.
  - Preserved typed and language-tagged `rr:constant` literals when constructing extend expressions.
- **Source dispatch and DB wiring** (`src/pyhartig/operators/SourceFactory.py`):
  - Expanded logical source detection for SPARQL and relational sources.
  - Added DB operator routing and mapping directory propagation for fixture fallback flows.
- **Join execution behavior** (`src/pyhartig/operators/EquiJoinOperator.py`):
  - Improved join-key normalization and overlap handling to avoid false-negative joins caused by type/lexical mismatches.
- **Source extraction robustness**:
  - `src/pyhartig/operators/sources/CsvSourceOperator.py`: improved JSONPath-like/case-insensitive key handling.
  - `src/pyhartig/operators/sources/JsonSourceOperator.py`: improved tolerant query parsing and fallback behavior.
  - `src/pyhartig/operators/sources/XmlSourceOperator.py`: improved iterator/extraction fallbacks for common XPath forms.
- **CLI run serialization flow** (`src/pyhartig/commands/run.py`):
  - Serializer auto-selection by output extension.
  - Post-processing to avoid duplicate triple/quad emission for same `(s,p,o)` key where quads are present.
- **Documentation consistency** (`README.md`):
  - Corrected section numbering and project-structure entries (including serializer/source-operator listings) to match current implementation.

### Fixed
- **Default graph handling in N-Quads**: `rr:defaultGraph` now serializes as default graph (triple form, no 4th term), matching expected conformance outputs.
- **Blank node identifiers**: Updated `to_bnode` behavior in `src/pyhartig/functions/builtins.py` to emit readable deterministic labels when valid (e.g., `_:Venus`, `_:BobSmith`) with hashed fallback only for unsafe labels.
- **Conformance result accounting**: Compare exceptions are now correctly counted as failures.

### Tests
- Expanded source/operator coverage in `tests/test_suite/test_01_source_operators.py` for SPARQL and DB operator paths (including fixture fallback scenarios).
- Updated suite expectations to align with finalized behavior (IRI percent-encoding and EquiJoin overlap semantics).
- Validated both tracks after fixes:
  - **Conformance**: `324/324` passed, expected-error cases `51/0`, coverage `100.0%`.
  - **Initial test suite**: `python -m pytest -q` fully passing.

## [0.2.9] - 2026-02-20

### Added
- **Named Graphs (rr:graphMap) support**: `MappingParser` now detects `rr:graphMap` on
  `rr:predicateObjectMap` and `rr:subjectMap` and attaches an `ExtendOperator`
  that produces a `graph` attribute for each generated tuple. This enables
  generation of quads (subject, predicate, object, graph) from RML mappings.
- **Unit tests**: Added `tests/test_suite/test_17_named_graphs.py` covering
  template- and reference-based graph maps and POM-level override semantics.

### Changed
- **SourceFactory path resolution**: `SourceFactory.create_source_operator`
  accepts `rml:reference` as a fallback for the logical source file, resolves
  relative paths against the mapping file directory, and includes additional
  fallback heuristics (CWD fallback, mapping-dir search) to make test and
  example mappings more robust.

### Fixed
- Ensure graph attribute generation coexists with existing `Extend`/`EquiJoin`
  wiring and does not break pipeline branch generation.

## [0.2.8] - 2026-02-18

### Added
- **Referencing Object Maps (Joins) support**: Implemented handling of `rr:parentTriplesMap` in the mapping
  parser. `MappingParser` now detects referencing object maps, instantiates a parent source pipeline, extracts
  `rr:joinCondition` attributes, and wires an `EquiJoinOperator` between child and parent sources to produce
  joined mapping tuples.
- **Unit test for referencing object maps**: Added `tests/test_suite/test_15_referencing_object_maps.py` which
  validates pipeline generation includes an `EquiJoin` and asserts expected join semantics on example JSON
  sources (parents/children).

### Changed
- **MappingParser**: Normalizes join attribute names and extraction queries; extracts join attributes earlier so
  parent resolution and attribute-mapping augmentation occur before parent `SourceOperator` creation. The parser
  now constructs EquiJoin on raw sources and then applies `ExtendOperator` to generate `subject` and
  `parent_subject` attributes to avoid attribute-name collisions during join.

### Fixed
- **Join wiring bug**: Fixed a use-before-assignment bug when resolving inline `rr:parentTriplesMap` by pre-extracting
  join attributes and using them to resolve parent TriplesMaps that lacked an explicit logical source node after
  normalization.
- **JSON iterator edge-case**: `JsonSourceOperator` now unwraps a root JSON array returned by an iterator so the
  operator iterates over elements (not the array object) — fixes missed rows when the iterator points at the root
  list.

### Tests
- Added pipeline-level assertion ensuring the generated pipeline contains an `EquiJoin` with the expected
  join condition (e.g., `author = user_id`).
 - `test_15_referencing_object_maps`: Added comprehensive join tests and  integrated an additional case covering referenced `rr:parentTriplesMap`.
 - `test_16_join_edge_cases.py`: new file containing multiple-edge-case tests (multiple
     join conditions, template-based joins, missing parent attributes, and multi-variable template joins).


## [0.2.7] - 2026-02-04


### Added
- **SourceFactory dispatch registry**: `src/pyhartig/operators/SourceFactory.py` now includes a registry to select the correct `SourceOperator` implementation from `rml:referenceFormulation` (e.g., `ql:JSONPath`, `ql:CSV`, `ql:XPath`).
- **CSV and XML Source Operators**: Added `src/pyhartig/operators/sources/CsvSourceOperator.py` and `src/pyhartig/operators/sources/XmlSourceOperator.py` to support `ql:CSV` and `ql:XPath` reference formulations.
- **Unit tests for CSV/XML sources**: Added `tests/test_sources/test_csv_xml_sources.py` to validate CSV and XML source handling.

### Changed
- **Fail-fast file handling in MappingParser**: `src/pyhartig/mapping/MappingParser.py` now checks the mapping file existence and raises `FileNotFoundError` immediately if the RML file is missing (stops the pipeline rather than producing silent empty results).
- **Source IO errors surface**: `SourceFactory` now re-raises `FileNotFoundError` and other IO errors when loading source files instead of falling back to empty data.

### Fixed
- **Removed JSON-only coupling**: `MappingParser` no longer instantiates `JsonSourceOperator` directly; it delegates source creation to `SourceFactory` to avoid parsing non-JSON sources as JSON.

### Refactored
- **Parsing robustness**: Added fallback parsing formats and a TTL-string sanitization attempt to help with common Windows path quoting issues during RML parsing.

## [0.2.6] - 2026-01-28

### Added
- **Source Factory Pattern**: Implemented `src/pyhartig/operators/SourceFactory.py` to decouple the `MappingParser` from specific data source implementations.
    - The factory automatically detects `rml:referenceFormulation` (e.g., `ql:JSONPath`) and instantiates the correct operator.
- **Blank Node Support**: Updated `MappingParser` to correctly handle `rr:termType rr:BlankNode`. It now maps these terms to the `to_bnode` function (generating Skolemized Blank Nodes) instead of incorrectly treating them as Literals.

### Fixed
- **Strict Typing Compliance**: Fixed `MappingParser` to strictly use `AlgebraIRI` and `AlgebraLiteral` when creating `Constant` expressions. This resolves type errors raised by the algebraic engine which now forbids raw Python strings.
- **Namespace Handling**: Corrected URI generation for `rr:termType` and `xsd:string` in `MappingParser`. Replaced error-prone manual concatenation (e.g., `f"{BASE}#..."`) with `rdflib.Namespace` usage to prevent malformed URIs.

### Refactored
- **MappingParser Decoupling**: Removed hardcoded JSON loading logic and `JsonSourceOperator` instantiation from `MappingParser.py`. It now delegates source creation to `SourceFactory`, making the engine extensible for future CSV/XML support without modifying the core parser.

## [0.2.5] - 2026-01-26

### Added
- **NTriples Serializer**: Created `src/pyhartig/serializers/NTriplesSerializer.py` to handle the conversion of algebraic execution results (`MappingTuple`) into valid N-Triples string format, with proper character escaping and term handling.
- **Robust Error Handling**: Added generic exception handling in `MappingParser` during source loading to catch unexpected errors (e.g., malformed JSON) in addition to `FileNotFoundError`.
- **Plugin-based CLI Architecture**: Completely refactored `__main__.py` and the `src/pyhartig/commands/` package.
    - Implemented a command discovery system that automatically loads any class inheriting from `BaseCommand` located in the commands directory.
    - This allows for easy extension of the CLI functionality by third-party developers without modifying the core engine.
    - **Implemented shared argument parsing to allow global flags (like `-v`) to be placed before or after subcommands.**
- **`list-issues` Command**: Added a new use-case specific command that:
    - Accepts a list of repository URLs (GitHub/GitLab) as arguments.
    - Automatically resolves API endpoints and fetches issue data.
    - Aggregates (merges) data from multiple repositories into unified JSON sources.
    - Injects these sources into a user-provided RML template (replacing `{{GITHUB_SOURCE}}` and `{{GITLAB_SOURCE}}`) before execution.

### Changed
- **Logging Integration**: Replaced `print()` statements in `MappingParser.py` with the standard `logging` module, enabling verbosity control (INFO/DEBUG) and preventing stdout pollution.

### Fixed
- **Generator Exhaustion Bug**: Fixed a critical bug in `MappingParser` where a debug log statement consumed the `predicateObjectMap` iterator, causing the parser to silently skip all mappings and generate zero triples without error.
- **Relative Path Resolution**: Fixed source file path resolution in `MappingParser`. Paths are now correctly resolved relative to the mapping file's directory, ensuring the CLI works correctly regardless of the user's current working directory.

### Refactored
- **Namespace Centralization**: Refactored `MappingParser.py` to import namespace constants from `src/pyhartig/namespaces.py` instead of using hardcoded URI strings and prefixes. This ensures a Single Source of Truth for RDF/RML namespaces.
- **CLI Structure**: Moved the standard execution logic from `__main__.py` to `src/pyhartig/commands/run.py` to maintain separation of concerns.

### Documentation
- **README Overhaul**: Completely refactored `README.md` to prioritize "Quick Start", installation instructions, and CLI usage examples.
- **New Operators**: Added documentation for `EquiJoinOperator` and `ProjectOperator` features in the main README.

## [0.2.4] - 2026-01-23

### Changed

- **Operators Module (`src/pyhartig/operators/`)**
  - `Operator.py`: Switched from eager to lazy execution. `execute()` now returns an `Iterator[MappingTuple]` (generator) and operators stream rows using `yield`. Added `StreamRows` helper to provide lazy iteration plus on-demand materialization (`len()` and indexing). This reduces peak memory usage for large datasets and enables streaming pipelines.
  - `SourceOperator.py`: `execute()` now streams rows via generators and returns a `StreamRows` wrapper. This avoids building large in-memory lists in multi-stage pipelines.

- **Source / JSON parsing (`src/pyhartig/operators/` + `src/pyhartig/operators/sources/`)**
  - `JsonSourceOperator.py`: JSONPath expressions are now compiled and cached in `__init__` (compiled iterator and per-attribute extraction cache) to avoid reparsing the same query for every context object. This significantly reduces CPU overhead when extraction queries are applied many times (e.g., for large sources).

- **Union semantics (`src/pyhartig/operators/UnionOperator.py`)**
  - Added an optional `distinct` parameter (default `False`) to control Bag vs Set semantics. By default the operator preserves Bag semantics for performance; setting `distinct=True` enables Set semantics (duplicate elimination) when needed.

- **Join algorithm (`src/pyhartig/operators/EquiJoinOperator.py`)**
  - Reworked join execution to use a hash-join style algorithm: the implementation builds an index (hash map) on the smaller side and probes it with the other side, yielding matches. This reduces worst-case behavior from O(N×M) nested loops to near O(N+M) in typical scenarios and is suitable for large relations.

### Fixed

- **Extend operator (`src/pyhartig/operators/ExtendOperator.py`)**
  - Fixed incorrect in-place mutation of `MappingTuple`. `MappingTuple` is immutable by design; `ExtendOperator` now uses `MappingTuple.extend()` (or constructs a new `MappingTuple`) to produce extended rows, avoiding `TypeError` and preserving immutability guarantees.

## [0.2.3] - 2026-01-21

### Changed

- **Expressions Module (`src/pyhartig/expressions/`)**
  - `Expression.py`: Documented the `evaluate()` method's behavior in the docstring: it must never raise an exception for data issues and should return `EPSILON` when evaluation is undefined, aligning implementation with the formal semantics.
  - `Constant.py`: Enforced strict typing in `Constant.__init__()` so that only RDF term instances (`IRI`, `Literal`, `BlankNode`) are accepted; raw Python primitives (e.g. `str`, `int`) are rejected to avoid ambiguity between plain strings and typed `xsd:string` literals.
  - `FunctionCall.py`: Added EPSILON propagation in `FunctionCall.evaluate()` — the function now returns `EPSILON` immediately if any evaluated argument is `EPSILON`, ensuring strict propagation and simplifying builtin implementations.

### Fixed

- **Expressions Module (`src/pyhartig/expressions/`)**
  - `Reference.py`: Fixed error handling per Definition 9: when an attribute is not present in a tuple, `Reference.evaluate()` now returns `EPSILON` instead of raising `KeyError` (implementation uses safe lookup or explicit handling), improving pipeline robustness for incomplete data.

## [0.2.2] - 2026-01-20

### Added

- **Centralized Namespace Management**: Created `src/pyhartig/namespaces.py` to manage RDF namespaces (XSD, RDF, RDFS,
  RML, RR) and common IRIs as constant objects. This eliminates "magic strings" and prevents URI typos throughout the
  codebase.
- **Automatic Type Inference**: `JsonSourceOperator` now automatically infers the correct XSD datatype from JSON values:
    - JSON `12` (int) → `"12"^^xsd:integer`
    - JSON `true` (bool) → `"true"^^xsd:boolean`
    - JSON `3.14` (float) → `"3.14"^^xsd:double`
- **Blank Node Generation**: Implemented `to_bnode` in `builtins.py`. It uses SHA-1 hashing (Skolemization) to
  generate deterministic Blank Node identifiers from input values, enabling the future implementation of joins via
  Referencing Object Maps.

### Changed

- **Strict Typing Architecture (Upstream Typing)**: The algebraic pipeline now operates strictly on RDF Terms (`IRI`,
  `Literal`, `BlankNode`). Raw Python types (`str`, `int`, `float`) are no longer treated as valid algebraic values.
- **AlgebraicValue Definition**: Updated `AlgebraicValue` type definition in `Tuple.py` to exclude Python primitives,
  enforcing strict adherence to the formal algebra definition.
- **Source Operator Behavior**: `JsonSourceOperator` now converts extracted data into typed `Literal` objects
  immediately upon extraction. This ensures that data circulating in the pipeline is always "RDF-native", removing
  semantic ambiguity in downstream operators and functions.

### Refactored

- **Built-in Functions Cleanup**: Refactored `src/pyhartig/functions/builtins.py`. Validation is now delegated to the
  `IRI` class, complying with the DRY (Don't Repeat Yourself) principle.
- **Type Safety in Functions**: `to_iri`, `to_literal`, and `concat` no longer perform "loose" type checking on raw
  Python types. They rely entirely on the strict `AlgebraicValue` types provided by the upstream pipeline.

### Fixed

#### Naive IRI Validation (`functions/builtins.py`)

- Replaced simplistic colon check in `to_iri()`
- Now raises `ValueError` for invalid IRIs (e.g., `to_iri("invalid iri")` returns `EPSILON`)

## [0.2.1] - 2026-01-16

### Added

#### IRI Syntax Validation (`Terms.py`)

- Added RFC 3987-based IRI validation via `__post_init__` in `IRI` dataclass
- New `InvalidIRIError` exception for invalid IRIs (e.g., `IRI("not an iri")` now raises)
- Validates scheme, authority, path, query, and fragment components

#### Language Tag Support (`Terms.py`)

- Added optional `language` field to `Literal` dataclass (e.g., `Literal("Bonjour", language="fr")`)
- BCP 47-based language tag validation with `InvalidLanguageTagError` exception
- Auto-sets datatype to `rdf:langString` per RDF 1.1 specification

#### Immutable MappingTuple (`Tuple.py`)

- `MappingTuple` now implements `Mapping` protocol instead of inheriting from `dict`
- Tuples are immutable: `t["col"] = val` now raises `TypeError`
- New methods: `extend(key, value)` and `project(attributes)` for algebraic operations
- Tuples are now hashable, enabling use in `set()` for duplicate elimination

### Changed

- **AlgebraicValue**: Removed `None` from type union to avoid confusion with `EPSILON`
- **SourceOperator**: Now converts `None` values from data sources to `EPSILON` automatically
- **ExtendOperator**: Uses new `tuple.extend()` method instead of mutation
- **_Epsilon**: Now hashable for use as dict keys and in sets

### Breaking Changes

- `MappingTuple` no longer supports item assignment (`t["key"] = value`)
- `None` values in `MappingTuple` now raise `ValueError` (use `EPSILON` instead)

## [0.2.0] - 2025-12-21

### Added

#### Project Operator (Opérateur de Projection)

- **New Operator**: Added `ProjectOperator` for restricting mapping relations to specified attributes
    - Based on Definition 11 of relational algebra for mapping relations
    - Formal notation: `Project^P(r) : (A, I) -> (P, I')`
    - Input: Mapping relation `r = (A, I)` and non-empty subset `P ⊆ A`
    - Output: New mapping relation `(P, I')` where `I' = { t[P] | t ∈ I }`
    - For each tuple `t`, creates `t[P]` with `dom(t[P]) = P` and `t[P](a) = t(a)` for all `a ∈ P`

- **Strict Mode (Default)**:
    - Raises `KeyError` when projecting attributes not present in tuple
    - Enforces constraint `P ⊆ A` from classical relational algebra
    - Safer behavior: detects bugs early
    - Heterogeneous schemas handled via `Union` + multiple `Project` operations

- **ProjectOperator Features**:
    - `execute()`: Projects tuples to retain only specified attributes (strict mode)
    - `explain()`: Human-readable ASCII tree visualization
    - `explain_json()`: Machine-readable JSON format for API/tools

- **Use Case**: Useful for retaining only attributes needed in subsequent mapping steps

#### EquiJoin Operator (Opérateur d'Équi-jointure)

- **New Operator**: Implemented `EquiJoinOperator` for combining two mapping relations based on join conditions
    - Based on Definition 12 of relational algebra for mapping relations
    - Formal notation: `EqJoin^J(r₁, r₂) : Operator × Operator → Operator`
    - Input: Two mapping relations `r₁ = (A₁, I₁)` and `r₂ = (A₂, I₂)`
    - Join Conditions: Set `J ⊆ A₁ × A₂` defining attribute pairs for equality testing
    - Precondition: `A₁ ∩ A₂ = ∅` (attribute sets must be disjoint)
    - Output: New mapping relation `(A, I)` where:
        - `A = A₁ ∪ A₂` (union of all attributes)
        - `I = { t₁ ∪ t₂ | t₁ ∈ I₁, t₂ ∈ I₂, ∀(a₁, a₂) ∈ J : t₁(a₁) = t₂(a₂) }`

- **Join Condition Semantics**:
    - Supports multiple join conditions (compound keys)
    - Tuple pairs are merged only when ALL conditions are satisfied
    - Correctly handles `None` values in join attributes

- **EquiJoinOperator Features**:
    - `execute()`: Performs nested loop equi-join with condition checking
    - `explain()`: Human-readable ASCII tree visualization showing join conditions
    - `explain_json()`: Machine-readable JSON format for API/tools

- **Validation**:
    - Raises `ValueError` if attribute sets are not disjoint (A₁ ∩ A₂ ≠ ∅)
    - Raises `ValueError` if join attribute lists have different lengths

- **Use Case**: Particularly relevant for referencing object maps (referencing object maps) in RML translation, where
  joins are needed to combine data from multiple sources

### Testing

- Added comprehensive test suite for `ProjectOperator` (`test_12_project_operator.py`)
    - Basic projection tests (single, multiple, all attributes)
    - Strict mode validation (missing attribute raises `KeyError`)
    - Empty result handling
    - Operator composition (Project + Extend, Project + Union)
    - Chained projections
    - Explain functionality tests
    - Edge cases (IRI values, duplicate tuples, tuple order)
    - Integration tests (RDF generation, heterogeneous schema handling)
    - 20 new tests

- Added comprehensive test suite for `EquiJoinOperator` (`test_13_equijoin_operator.py`)
    - Basic equijoin tests (single condition, multiple conditions)
    - No-match scenarios (empty result)
    - Cartesian product-like behavior (many-to-many matches)
    - Precondition validation (disjoint attributes, equal-length attribute lists)
    - Empty relation handling (left/right empty)
    - Operator composition (EquiJoin + Extend, EquiJoin + Project)
    - Chained equijoins (three-way joins)
    - Explain functionality tests (text and JSON formats)
    - RML referencing object map use case simulation
    - Edge cases (null values, value preservation)
    - 17 new tests

## [0.1.15] - 2025-12-9

### Added

#### Explain Functionality

- **Pipeline Visualization**: Added `explain()` method to all operators for human-readable pipeline visualization
    - ASCII tree format with proper indentation
    - Shows operator hierarchy and parameters
    - Displays expression details inline
    - Example output:
      ```
      Union(
        operators: 2
        ├─ [0]:
          Extend(
            attribute: subject
            expression: to_iri(Ref(person_id), Const('http://example.org/'))
            parent:
              └─ Source(
                   iterator: $.team[*]
                   mappings: ['person_id', 'person_name']
                 )
          )
        └─ [1]:
          Source(...)
      )
      ```

- **JSON Explanation API**: Added `explain_json()` method to all operators for programmatic access
    - Machine-readable JSON format
    - Complete operator tree structure
    - Expression details with type information
    - Suitable for visualization tools and API endpoints
    - Example output:
      ```json
      {
        "type": "Extend",
        "parameters": {
          "new_attribute": "subject",
          "expression": {
            "type": "FunctionCall",
            "function": "to_iri",
            "arguments": [...]
          }
        },
        "parent": {...}
      }
      ```

- **MappingParser Integration**: Added helper methods for pipeline explanation
    - `MappingParser.explain()` - Get text explanation of RML-generated pipeline
    - `MappingParser.explain_json()` - Get JSON explanation
    - `MappingParser.save_explanation(path, format)` - Save explanation to file

### Changed

- **Operator Base Class**: Updated `Operator` abstract class with new abstract methods
    - Added `explain(indent: int, prefix: str) -> str` abstract method
    - Added `explain_json() -> Dict[str, Any]` abstract method
    - All concrete operators now implement these methods

- **SourceOperator**: Enhanced with explanation capabilities
    - Implements `explain()` for text format
    - Implements `explain_json()` for JSON format
    - Shows iterator query and attribute mappings

- **JsonSourceOperator**: Enhanced with JSON-specific explanation details
    - Adds `source_type: "JSON"` to JSON output
    - Shows JSONPath-specific parameters

- **ExtendOperator**: Enhanced with expression visualization
    - Recursive expression explanation
    - Shows parent operator hierarchy
    - Detailed RDF term representation in JSON (IRI, Literal, BlankNode)

- **UnionOperator**: Enhanced with multi-child visualization
    - Shows operator count
    - Lists all child operators with proper tree formatting
    - JSON format includes all children in array

### Testing

- Added comprehensive test suite for `explain()` functionality
    - Tests for all operator types
    - Tests for expression formatting
    - Tests for nested operator trees
    - Tests for Union with multiple children

- Added comprehensive test suite for `explain_json()` functionality
    - Validates JSON structure for all operators
    - Tests expression serialization
    - Tests RDF term representation (IRI, Literal, BlankNode)
    - Tests nested pipelines
    - Validates JSON serializability (no serialization errors)
    - 12 new tests with 100% pass rate

- Updated GitHub/GitLab use case test
    - Added validation for proper IRI generation
    - Validates that subjects are IRI type, not Literal
    - Ensures N-Triples output is valid RDF

### Documentation

- Added "Pipeline Visualization" section to README
    - Documents `explain()` and `explain_json()` usage
    - Provides examples for both formats
    - Shows integration with MappingParser

- Updated test suite documentation
    - Added test category for explain functionality
    - Updated test count (95 → 107 tests)

## [0.1.14] - 2025-12-09

### Changed

- **Project structure in `README.md`**.

## [0.1.13] - 2025-12-09

### Fixed

- **RML Term Type Defaults**: Fixed `MappingParser._create_ext_expr()` to correctly apply R2RML default term types:
    - Subject Maps now default to `rr:IRI` (was incorrectly defaulting to Literal)
    - Predicate Maps now default to `rr:IRI` (was incorrectly defaulting to Literal)
    - Object Maps continue to default to `rr:Literal` (correct)
- This fix ensures generated RDF subjects and predicates are proper IRIs, not literals, making the output conformant
  with RDF standards

### Technical Details

- Modified `_create_ext_expr()` to accept `default_term_type` parameter
- Updated method calls in `parse()` to specify appropriate defaults based on map type
