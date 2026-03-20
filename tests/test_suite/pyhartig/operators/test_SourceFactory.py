from __future__ import annotations

import json
from pathlib import Path

import pytest
from rdflib import BNode, Graph, Literal as RDFLiteral, Namespace, URIRef

from pyhartig.operators.SourceFactory import SourceFactory
from pyhartig.operators.sources.CsvSourceOperator import CsvSourceOperator
from pyhartig.operators.sources.JsonSourceOperator import JsonSourceOperator
from pyhartig.operators.sources.SparqlSourceOperator import SparqlSourceOperator
from pyhartig.operators.sources.XmlSourceOperator import XmlSourceOperator


RML = Namespace("http://semweb.mmlab.be/ns/rml#")
QL = Namespace("http://semweb.mmlab.be/ns/ql#")
RR = Namespace("http://www.w3.org/ns/r2rml#")
SD = Namespace("http://www.w3.org/ns/sparql-service-description#")


@pytest.mark.coverage_suite
def test_source_factory_creates_file_backed_sources(tmp_path: Path):
    (tmp_path / "items.json").write_text(json.dumps({"items": [{"id": 1}]}), encoding="utf-8")
    (tmp_path / "items.csv").write_text("id\n1\n", encoding="utf-8")
    (tmp_path / "items.xml").write_text("<root><row><id>1</id></row></root>", encoding="utf-8")

    graph = Graph()
    json_ls = BNode()
    graph.add((json_ls, RML.source, RDFLiteral("items.json")))
    graph.add((json_ls, RML.iterator, RDFLiteral("$.items[*]")))
    graph.add((json_ls, RML.referenceFormulation, QL.JSONPath))

    csv_ls = BNode()
    graph.add((csv_ls, RML.source, RDFLiteral("items.csv")))
    graph.add((csv_ls, RML.referenceFormulation, QL.CSV))

    xml_ls = BNode()
    graph.add((xml_ls, RML.source, RDFLiteral("items.xml")))
    graph.add((xml_ls, RML.referenceFormulation, QL.XPath))
    graph.add((xml_ls, RML.iterator, RDFLiteral(".//row")))

    assert isinstance(SourceFactory.create_source_operator(graph, json_ls, tmp_path, {"id": "$.id"}), JsonSourceOperator)
    assert isinstance(SourceFactory.create_source_operator(graph, csv_ls, tmp_path, {"id": "id"}), CsvSourceOperator)
    assert isinstance(SourceFactory.create_source_operator(graph, xml_ls, tmp_path, {"id": "id"}), XmlSourceOperator)


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_source_factory_supports_sparql_and_rejects_unknown_formulation(tmp_path: Path):
    graph = Graph()
    sparql_source = URIRef("http://example.org/service")
    logical_source = BNode()
    graph.add((logical_source, RML.source, sparql_source))
    graph.add((logical_source, RML.query, RDFLiteral("SELECT ?s WHERE { ?s ?p ?o }")))
    graph.add((sparql_source, SD.endpoint, URIRef("http://example.org/sparql")))

    source = SourceFactory.create_source_operator(graph, logical_source, tmp_path, {"s": "s"})
    assert isinstance(source, SparqlSourceOperator)

    with pytest.raises(ValueError):
        SourceFactory._get_reference_formulation_factory(URIRef("http://example.org/unsupported"))


@pytest.mark.coverage_suite
def test_source_factory_helper_methods_cover_path_resolution_and_database_detection(tmp_path: Path):
    graph = Graph()
    db_node = URIRef("http://example.org/mysql-db")
    logical_source = BNode()
    graph.add((logical_source, RML.source, db_node))
    graph.add((logical_source, RR.tableName, RDFLiteral("demo")))
    graph.add((db_node, URIRef("http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDriver"), RDFLiteral("com.mysql.jdbc.Driver")))
    graph.add((db_node, URIRef("http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDSN"), RDFLiteral("mysql://user:pass@host:3306/db")))

    normalized = SourceFactory._normalize_ref_formulation(RDFLiteral(str(QL.JSONPath)))
    assert isinstance(normalized, URIRef)

    source_str, src_path, uri_like = SourceFactory._resolve_source_path(RDFLiteral("items.json"), tmp_path)
    assert source_str == "items.json"
    assert src_path == tmp_path / "items.json"
    assert uri_like is False

    original_rglob = Path.rglob
    try:
        Path.rglob = lambda self, filename: iter(())  # type: ignore[method-assign]
        assert SourceFactory._search_source_path(tmp_path, "missing.json") is None
    finally:
        Path.rglob = original_rglob  # type: ignore[method-assign]
    metadata = SourceFactory._extract_database_metadata(graph, logical_source, db_node)
    assert metadata["dsn"].startswith("mysql://")
    assert SourceFactory._detect_database_source_class(metadata["jdbc_driver"], metadata["dsn"]).__name__ == "MysqlSourceOperator"


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_source_factory_file_loader_error_paths_and_missing_fallbacks(monkeypatch, tmp_path: Path):
    missing = tmp_path / "missing.json"
    with pytest.raises(FileNotFoundError):
        SourceFactory._create_json_source(missing, RDFLiteral("$"), {})

    monkeypatch.setattr(Path, "exists", lambda self: False)
    monkeypatch.setattr(SourceFactory, "_search_source_path", lambda mapping_dir, filename: None)
    resolved = SourceFactory._resolve_missing_source_path("items.json", tmp_path, tmp_path / "items.json", False)
    assert resolved == tmp_path / "items.json"


@pytest.mark.coverage_suite
def test_source_factory_covers_database_and_lookup_branches(monkeypatch, tmp_path: Path):
    class _DatabaseSource:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    sentinel = object()
    with monkeypatch.context() as patch_ctx:
        patch_ctx.setattr(SourceFactory, "_extract_source_metadata", lambda *args, **kwargs: ("db", tmp_path / "db", False))
        patch_ctx.setattr(SourceFactory, "_resolve_source_path", lambda *args, **kwargs: ("db", tmp_path / "db", False))
        patch_ctx.setattr(SourceFactory, "_log_source_resolution", lambda *args, **kwargs: None)
        patch_ctx.setattr(SourceFactory, "_resolve_missing_source_path", lambda *args, **kwargs: tmp_path / "db")
        patch_ctx.setattr(SourceFactory, "_create_sparql_source_if_applicable", lambda *args, **kwargs: None)
        patch_ctx.setattr(SourceFactory, "_create_database_source_if_applicable", lambda *args, **kwargs: sentinel)
        assert SourceFactory.create_source_operator(Graph(), BNode(), tmp_path, {}) is sentinel

    graph = Graph()
    logical_source = BNode()
    db_node = URIRef("http://example.org/postgres-db")
    graph.add((logical_source, RML.source, db_node))
    graph.add((logical_source, RR.tableName, RDFLiteral("demo")))
    graph.add((db_node, URIRef("http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDriver"), RDFLiteral("org.postgresql.Driver")))
    graph.add((db_node, URIRef("http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#jdbcDSN"), RDFLiteral("postgresql://db")))

    monkeypatch.setattr(SourceFactory, "_detect_database_source_class", lambda driver, dsn: _DatabaseSource)
    db_source = SourceFactory._create_database_source_if_applicable(
        graph,
        logical_source,
        db_node,
        RDFLiteral("$"),
        {"id": "id"},
        tmp_path,
    )
    assert isinstance(db_source, _DatabaseSource)
    assert db_source.kwargs["table_name"] == "demo"

    assert SourceFactory._detect_database_source_class(RDFLiteral("org.postgresql.Driver"), "") is not None
    assert SourceFactory._detect_database_source_class(RDFLiteral("com.microsoft.sqlserver.jdbc.SQLServerDriver"), "") is not None
    assert SourceFactory._get_reference_formulation_factory(None) is SourceFactory._create_json_source


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_source_factory_covers_positive_search_and_generic_loader_failures(monkeypatch, tmp_path: Path):
    target = tmp_path / "items.json"
    target.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(Path, "exists", lambda self: (not self.is_absolute()) and self.name == "items.json")
    assert SourceFactory._resolve_missing_source_path("items.json", tmp_path, tmp_path / "items.json", False) == Path("items.json")

    original_rglob = Path.rglob
    try:
        Path.rglob = lambda self, filename: iter([target])  # type: ignore[method-assign]
        assert SourceFactory._search_source_path(tmp_path, "items.json") == target
    finally:
        Path.rglob = original_rglob  # type: ignore[method-assign]

    class _BrokenGraph:
        def value(self, *args, **kwargs):
            raise RuntimeError("boom")

    assert SourceFactory._lookup_endpoint_by_uri(_BrokenGraph(), URIRef("http://example.org/s")) is None
    assert SourceFactory._create_sparql_source_if_applicable(None, BNode(), None, None, {}, tmp_path) is None

    monkeypatch.setattr(JsonSourceOperator, "from_json_file", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError):
        SourceFactory._create_json_source(target, RDFLiteral("$"), {})

    monkeypatch.setattr("builtins.open", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError):
        SourceFactory._create_csv_source(tmp_path / "items.csv", RDFLiteral("$"), {})

    import xml.etree.ElementTree as ET

    monkeypatch.setattr(ET, "parse", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError):
        SourceFactory._create_xml_source(tmp_path / "items.xml", RDFLiteral("."), {})
