# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### TODO

- Update documentation to include `ProjectOperator` and `EquiJoinOperator`
- Add examples in README and user guides
- Confirm tests
- Add existing tests from :
    - https://rml.io/test-cases/
    - https://github.com/anuzzolese/pyrml
    - https://github.com/RMLio/rmlmapper-java
    - https://github.com/morph-kgc/morph-kgc

#### Mapping Parser Module (`src/pyhartig/mapping/`)

- **[CRITICAL]** Add support for Referencing Object Maps (Joins): This is the most significant missing feature compared
  to the paper. Algorithm 1 (lines 11-21) explicitly describes how to handle `rr:objectMap` that reference another
  Triples Map (`rr:parentTriplesMap`). This involves instantiating a second `SourceOperator`, extracting join
  conditions, and creating an `EquiJoinOperator`. Currently, the loop treats all `objectMap` as simple extensions (
  `ExtendOperator`). There is no detection of `parentTriplesMap` nor instantiation of `EquiJoinOperator`. **Consequence
  **: The implementation currently only supports "flat" mappings (one source → one graph). It is impossible to link two
  JSON files or a CSV and a JSON together.
- **[CRITICAL]** Remove tight coupling with JSON (Source agnosticism violation): Currently, `JsonSourceOperator` is
  hardcoded (`E_src = JsonSourceOperator(source_data=raw_data, ...)`). If a user provides an RML mapping pointing to a
  CSV (`ql:CSV`), the code will crash or try to parse it as JSON. The paper defines a `SRCANDROOTQUERY` abstraction (
  Definition 14) that should determine the source type (CSV, JSON, XML, SQL) based on `rml:referenceFormulation` and
  instantiate the appropriate operator. **Suggested fix**: Implement a factory pattern or registry to select the correct
  `SourceOperator` based on the reference formulation.
- Incomplete Blank Node handling: Currently, unhandled cases return `Constant(AlgebraIRI("http://error"))`. Algorithm
  3 (line 13) specifies the use of `toBNode` for `rr:termType rr:BlankNode`. If a mapping explicitly requests a Blank
  Node, the current parser returns an error or invalid constant instead of generating a unique identifier or one based
  on a hash function of the columns (Skolemization). **Suggested fix**: Implement `toBNode` function and integrate it
  into the parser for `rr:BlankNode` term type.
- Implement Source Factory: Replace direct `JsonSourceOperator` instantiation with a method that inspects the RDF graph
  for `rml:referenceFormulation` and instantiates the appropriate operator (`JsonSourceOperator`, `CsvSourceOperator`,
  etc.).
- Implement Join support (Algorithm 1): In the `predicateObjectMap` loop, check for `rr:parentTriplesMap`. If present,
  instantiate the parent source pipeline (recursively or via lookup), extract join conditions (`rr:joinCondition`), and
  insert an `EquiJoinOperator` between the current and parent pipelines.
- Improve file error handling: Currently uses `except FileNotFoundError: print(...)`. In production or research, it's
  better to raise the exception to stop the pipeline immediately rather than continuing with `raw_data = {}` (empty
  dict), which produces a silently empty result that is hard to debug.

#### Operators Module (`src/pyhartig/operators/`)

- `Operator.py`: **[CRITICAL]** Switch from Eager to Lazy execution: Currently, `execute()` returns
  `List[MappingTuple]`, meaning each operator must wait for the previous one to finish and store everything in RAM
  before starting. With 1 million rows, the first `SourceOperator` creates a list of 1 million objects in memory, which
  is inefficient. **Suggested fix**: Switch to a Lazy model using Python generators (`Iterator[MappingTuple]` and
  `yield`). This would allow streaming data row by row, significantly reducing memory usage for large datasets.
- `SourceOperator.py` / `JsonSourceOperator.py`: Optimize JSONPath parsing performance: Currently, `parse(query)` is
  called inside `_apply_iterator` and `_apply_extraction` methods. If `_apply_extraction` is called for each object (1M
  times), the query string is re-parsed 1M times. **Suggested fix**: Compile JSONPath expressions in `__init__` and
  store the compiled objects for reuse.
- `UnionOperator.py`: Bag vs Set semantics: Currently uses `merged_results.extend(op.execute())`, which keeps
  duplicates (Bag Semantics). The formal definition (Def 2) describes a relation where `I` is a set, implying no
  duplicates (Set Semantics). In practice (SQL, SPARQL), Bag semantics is almost always used for performance, so the
  current choice is correct for a real engine. However, this is a slight deviation from the strict mathematical
  definition. **Note**: Consider adding an optional `distinct=True` parameter to enable Set semantics when needed.
- `EquiJoinOperator.py`: **[CRITICAL]** Optimize join algorithm: Current implementation uses a Nested Loop Join (double
  `for` loop) with O(N×M) complexity. With 10,000 rows in each source, this results in 100 million comparisons, which is
  unusable for real data. **Suggested fix**: Implement a Hash Join (O(N+M) complexity) by building a hash map (
  dictionary) on the right relation indexed by the join key, then iterating over the left relation and performing direct
  lookups.

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

#### General Improvements

##### CLI / Entry Point (Missing)

- Add CLI entry point: Create `src/pyhartig/__main__.py` or `cli.py` to allow running the tool without writing Python
  code. Should accept command-line arguments:
    - `-m mapping.ttl`: RML mapping file
    - `-o output.nt`: Desired output format/file
    - (Optional) Verbosity level for debugging
- **Why**: Essential for validating real integration tests and allowing others to use the tool.

##### Named Graphs / Graph Maps (Missing)

- Add support for Named Graphs (Quads): RML allows generating Quads (Subject, Predicate, Object, Graph), not just
  Triples. Currently, `MappingParser` and tests focus only on `subject`, `predicate`, `object`. If an RML mapping
  contains `rr:graphMap`, it will be ignored. The algebra is flexible enough - adding a "graph" column is trivial via
  `ExtendOperator`. **Action**: Modify `MappingParser` to detect `rr:graphMap` and add a corresponding `ExtendOperator`
  that generates the `graph` attribute.

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

##### Source Factory (Missing)

- Implement Source Factory pattern: Currently, `JsonSourceOperator` is hardcoded. Need a `SourceFactory` that reads
  `rml:source` and `rml:referenceFormulation` to instantiate the appropriate operator. **Benefit**: To support CSV in
  the future, just add `CsvSourceOperator` and register it in the Factory, without modifying `MappingParser`.

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