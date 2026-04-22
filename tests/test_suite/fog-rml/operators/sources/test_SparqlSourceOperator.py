from __future__ import annotations

from pathlib import Path

import pytest

from fog_rml.operators.sources.SparqlSourceOperator import SparqlSourceOperator


@pytest.mark.coverage_suite
def test_sparql_source_operator_validates_and_emulates_local_rdf(tmp_path: Path):
    resource = tmp_path / "resource1.ttl"
    resource.write_text("@prefix ex: <http://example.org/> . ex:s ex:p ex:o .", encoding="utf-8")

    mappings = SparqlSourceOperator._normalize_attribute_mappings({"s": "s", "name": "$.name"})
    data = SparqlSourceOperator._emulate_from_local_rdf(
        "SELECT ?s WHERE { ?s ?p ?o }",
        tmp_path,
        "http://example.org/base#InputSPARQL1",
    )
    SparqlSourceOperator._validate_sparql_query("SELECT ?s WHERE { ?s ?p ?o }")

    assert mappings["s"] == "s.value"
    assert data["results"]["bindings"]


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_sparql_source_operator_rejects_invalid_queries():
    with pytest.raises(ValueError):
        SparqlSourceOperator._validate_sparql_query("SELECT ?s ?s WHERE { ?s ?p ?o }")


@pytest.mark.coverage_suite
def test_sparql_source_operator_helper_branches_cover_remote_and_binding_paths(monkeypatch, tmp_path: Path):
    mappings = SparqlSourceOperator._normalize_attribute_mappings({"a": "$['name.value']", "b": "$['name']", "c": "$.id"})
    assert mappings["a"] == "name.value"
    assert mappings["b"] == "name.value"
    assert mappings["c"] == "id.value"
    assert SparqlSourceOperator._binding_for_value("literal") == {"type": "literal", "value": "literal"}
    assert SparqlSourceOperator._extract_select_clause("ASK { ?s ?p ?o }") is None
    assert SparqlSourceOperator._apply_empty_where_binding_fallback("SELECT ?s WHERE { }", []) == [{}]
    assert SparqlSourceOperator._query_local_rdf_resource(tmp_path / "missing.ttl", "SELECT * WHERE { ?s ?p ?o }") is None

    class _Row:
        def asdict(self):
            return {"s": None, "o": "x"}

    assert SparqlSourceOperator._build_local_bindings([_Row()]) == [{"o": {"type": "literal", "value": "x"}}]

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b'{"head":{"vars":["s"]},"results":{"bindings":[]}}'

    monkeypatch.setattr(
        "fog_rml.operators.sources.SparqlSourceOperator.request.urlopen",
        lambda req, timeout=10: _Response(),
    )
    remote = SparqlSourceOperator._query_remote_endpoint("http://example.org/sparql", "SELECT ?s WHERE { ?s ?p ?o }")
    assert remote["head"]["vars"] == ["s"]

    monkeypatch.setattr(SparqlSourceOperator, "_emulate_from_local_rdf", classmethod(lambda cls, **kwargs: None))
    monkeypatch.setattr(SparqlSourceOperator, "_query_remote_endpoint", classmethod(lambda cls, **kwargs: None))
    assert SparqlSourceOperator._load_sparql_json_data("http://example.org/sparql", "SELECT ?s WHERE { ?s ?p ?o }", tmp_path, None) == {
        "head": {"vars": []},
        "results": {"bindings": []},
    }


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_sparql_source_operator_rejects_empty_or_invalid_queries_and_exposes_json_explanation(monkeypatch, tmp_path: Path):
    with pytest.raises(ValueError):
        SparqlSourceOperator._validate_sparql_query("")
    with pytest.raises(ValueError):
        SparqlSourceOperator._validate_sparql_query("SELECT ?s WHERE {")

    monkeypatch.setattr(
        SparqlSourceOperator,
        "_load_sparql_json_data",
        classmethod(
            lambda cls, **kwargs: {
                "head": {"vars": ["s"]},
                "results": {"bindings": [{"s": {"type": "uri", "value": "http://example.org/s"}}]},
            }
        ),
    )
    source = SparqlSourceOperator(
        "http://example.org/sparql",
        "SELECT ?s WHERE { ?s ?p ?o }",
        "$.results.bindings[*]",
        {"s": "s"},
        tmp_path,
    )
    assert source.explain_json()["parameters"]["source_type"] == "SPARQL"

