from __future__ import annotations

from pathlib import Path

import pytest
from rdflib import Dataset, URIRef

from fog_rml.sparql.service_call import (
    _aggregate_graph_body_results,
    _aggregate_service_call_results,
    _build_aggregated_row,
    _build_per_graph_query,
    _build_values_insert_text,
    _execute_materialized_query,
    _extract_graph_bodies,
    _extract_prefix_block,
    _extract_select_clause,
    _extract_select_vars,
    _extract_tokens_from_values_clause,
    _find_mapping_for_repo,
    _find_service_call_matches,
    _get_graph,
    _get_row_value,
    _inject_values_clauses,
    _normalize_token_to_name,
    _populate_graph_from_mapping,
    _prepare_service_call_context,
    _process_service_call_match,
    _resolve_service_call_token,
    _token_to_graph_uri,
    execute_query_with_service_call,
)


@pytest.mark.coverage_suite
def test_service_call_helpers_and_rewrite(dataset, tmp_path: Path, monkeypatch):
    query = """
    SELECT ?repo ?g WHERE {
      VALUES ?repo { "demo" }
      BIND SERVICE-CALL(?repo, "mapping.ttl") AS ?g
    }
    """
    assert _extract_tokens_from_values_clause(query, "repo") == ['"demo"']
    assert _normalize_token_to_name("<http://example.org/demo>") == "demo"
    assert _token_to_graph_uri('"demo"') == URIRef("http://example.org/demo")
    assert _find_service_call_matches(query)
    assert "VALUES ?g" in _inject_values_clauses("SELECT * WHERE { }", [("g", "<http://example.org/demo>")])

    mapping_dir = tmp_path / "mappings"
    mapping_dir.mkdir()
    mapping = mapping_dir / "mapping.ttl"
    mapping.write_text("", encoding="utf-8")
    assert _find_mapping_for_repo(mapping_dir, "demo", "mapping.ttl") == mapping

    monkeypatch.setattr("fog_rml.sparql.service_call._find_service_call_matches", lambda _query: [])
    monkeypatch.setattr(dataset, "query", lambda rewritten_query: [("ok", rewritten_query)])
    assert execute_query_with_service_call(dataset, query, tmp_path)[0][0] == "ok"


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_service_call_returns_empty_result_on_query_failure(dataset, monkeypatch, tmp_path: Path):
    monkeypatch.setattr("fog_rml.sparql.service_call._find_service_call_matches", lambda _query: [])
    monkeypatch.setattr(dataset, "query", lambda _query: (_ for _ in ()).throw(RuntimeError("boom")))
    assert execute_query_with_service_call(dataset, "SELECT * WHERE { ?s ?p ?o }", tmp_path) == []


@pytest.mark.coverage_suite
def test_service_call_helper_functions_cover_materialization_and_aggregation(monkeypatch, tmp_path: Path):
    dataset = Dataset()
    graph_uri = URIRef("http://example.org/g")
    assert _get_graph(dataset, graph_uri).identifier == graph_uri

    repo_dir = tmp_path / "demo"
    repo_dir.mkdir()
    mapping_path = repo_dir / "mapping.ttl"
    mapping_path.write_text("", encoding="utf-8")
    assert _find_mapping_for_repo(tmp_path, "demo", "mapping.ttl") == tmp_path / "demo" / "mapping.ttl"
    assert _find_mapping_for_repo(tmp_path, "missing", "mapping.ttl") is None

    class _Parser:
        def __init__(self, path):
            self.path = path

        def parse(self):
            class _Op:
                @staticmethod
                def execute():
                    return [
                        {
                            "subject": URIRef("http://example.org/s"),
                            "predicate": URIRef("http://example.org/p"),
                            "object": URIRef("http://example.org/o"),
                        }
                    ]

            return _Op()

    monkeypatch.setattr("fog_rml.mapping.MappingParser.MappingParser", _Parser)
    monkeypatch.setattr("fog_rml.utils.term_utils.term_to_rdflib", lambda value: value)
    _populate_graph_from_mapping(dataset, mapping_path, graph_uri)
    assert list(dataset.graph(graph_uri))

    query = """
    PREFIX ex: <http://example.org/>
    SELECT ?repo ?g ?s WHERE {
      VALUES ?repo { "demo" "missing" }
      BIND SERVICE-CALL(?repo, "mapping.ttl") AS ?g
      GRAPH ?g { ?s ex:p ?o }
    }
    """
    match = _find_service_call_matches(query)[0]

    monkeypatch.setattr("fog_rml.sparql.service_call._populate_graph_from_mapping", lambda *_args, **_kwargs: None)
    processed = _process_service_call_match(dataset, query, tmp_path, match)
    assert processed[0] == "g"

    modified_query, clauses, var_to_pairs, var_out_to_in = _prepare_service_call_context(dataset, query, tmp_path, [match])
    assert "SERVICE-CALL" not in modified_query
    assert clauses
    assert var_to_pairs["g"][0][0] == '"demo"'
    assert var_out_to_in["g"] == "repo"

    inserted = _build_values_insert_text([("g", "<http://example.org/demo>")])
    assert "VALUES ?g" in inserted
    assert _extract_select_clause(query).strip().startswith("?repo")
    assert _extract_select_vars(query) == ["repo", "g", "s"]
    assert "g" in _extract_graph_bodies(query)
    assert _extract_prefix_block(query).lstrip().startswith("PREFIX")
    assert "GRAPH <http://example.org/demo>" in _build_per_graph_query("PREFIX ex: <x>\n", "http://example.org/demo", "?s ex:p ?o", ["s", "o"])

    row = {"s": URIRef("http://example.org/s")}
    assert _get_row_value(row, "s", ["s"]) == URIRef("http://example.org/s")
    assert _get_row_value(("value",), "s", ["s"]) == "value"
    assert _build_aggregated_row(row, ["repo", "s", "missing"], "repo", '"demo"', ["s"]) == (
        URIRef("http://example.org/demo"),
        URIRef("http://example.org/s"),
        None,
    )

    monkeypatch.setattr(dataset, "query", lambda _query: [row])
    aggregated = _aggregate_graph_body_results(dataset, "SELECT ?s WHERE {}", ["repo", "s"], "repo", '"demo"', ["s"])
    assert aggregated[0][0] == URIRef("http://example.org/demo")

    full_aggregated = _aggregate_service_call_results(dataset, query, {"g": [('"demo"', "http://example.org/demo")]}, {"g": "repo"})
    assert full_aggregated
    monkeypatch.setattr(dataset, "query", lambda _query: (_ for _ in ()).throw(RuntimeError("boom")))
    assert _execute_materialized_query(dataset, "SELECT * WHERE { }") == []


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_service_call_helper_edge_cases(monkeypatch, tmp_path: Path):
    class _ContextOnlyDataset:
        def get_context(self, uri):
            return uri

    assert _get_graph(_ContextOnlyDataset(), URIRef("http://example.org/g")) == URIRef("http://example.org/g")

    class _BrokenDataset:
        pass

    with pytest.raises(AttributeError):
        _get_graph(_BrokenDataset(), URIRef("http://example.org/g"))

    assert _normalize_token_to_name("demo") == "demo"
    assert _token_to_graph_uri("<http://example.org/demo>") == URIRef("http://example.org/demo")
    assert _extract_tokens_from_values_clause("SELECT * WHERE {}", "repo") == []

    dataset = Dataset()
    assert _resolve_service_call_token(dataset, tmp_path, "missing.ttl", '"demo"') is None
