from __future__ import annotations

import json
from pathlib import Path

import pytest
from rdflib import BNode, Graph, Literal as RDFLiteral, Namespace, URIRef

from fog_rml.algebra.Terms import IRI as AlgebraIRI, Literal as AlgebraLiteral
from fog_rml.expressions.Constant import Constant
from fog_rml.expressions.FunctionCall import FunctionCall
from fog_rml.expressions.Reference import Reference
from fog_rml.mapping.MappingParser import FNML, FNO, RDF, RML, RR, MappingParser
from fog_rml.operators.ExtendOperator import ExtendOperator
from fog_rml.operators.ProjectOperator import ProjectOperator
from fog_rml.operators.UnionOperator import UnionOperator


SIMPLE_JSON_MAPPING = """
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix ql: <http://semweb.mmlab.be/ns/ql#> .

<#TM>
  a rr:TriplesMap ;
  rml:logicalSource [
    rml:source "items.json" ;
    rml:referenceFormulation ql:JSONPath ;
    rml:iterator "$.items[*]"
  ] ;
  rr:subjectMap [ rr:template "http://example.org/item/{id}" ] ;
  rr:predicateObjectMap [
    rr:predicateMap [ rr:constant <http://example.org/name> ] ;
    rr:objectMap [ rml:reference "name" ]
  ] .
"""

D2RQ = Namespace("http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#")


def _parser_for(tmp_path: Path, content: str = SIMPLE_JSON_MAPPING) -> MappingParser:
    mapping_path = tmp_path / "mapping.ttl"
    mapping_path.write_text(content, encoding="utf-8")
    return MappingParser(str(mapping_path))


@pytest.mark.coverage_suite
def test_mapping_parser_helpers_and_simple_parse(write_mapping_files, stream_to_list):
    assert ("var", "id") in MappingParser._iter_template_segments("http://ex/{id}/{{raw}}")
    assert MappingParser._extract_single_brace_variables("http://ex/{id}/{name}") == ["id", "name"]

    mapping_path = write_mapping_files(
        SIMPLE_JSON_MAPPING,
        {"items.json": json.dumps({"items": [{"id": 1, "name": "Alice"}]})},
    )
    rows = stream_to_list(MappingParser(str(mapping_path)).parse().execute())
    assert rows[0]["subject"].value == "http://example.org/item/1"


@pytest.mark.coverage_suite
def test_mapping_parser_helper_methods_cover_parsing_and_term_map_construction(tmp_path: Path, monkeypatch):
    parser = _parser_for(tmp_path, '@base <http://example.org/base/> .\n' + SIMPLE_JSON_MAPPING)
    assert parser.base_iri == "http://example.org/base/"
    assert parser._is_simple_identifier("demo_1") is True
    assert parser._is_simple_identifier("1demo") is False
    assert parser._consume_close_brace("}", 0, []) == 1

    parse_calls = []

    def _graph_parse(*args, **kwargs):
        parse_calls.append(kwargs.get("format"))
        if kwargs.get("format") == "n3":
            return None
        raise ValueError("boom")

    monkeypatch.setattr(parser.graph, "parse", _graph_parse)
    assert parser._parse_with_fallback_formats() is True
    assert "n3" in parse_calls

    parser = _parser_for(tmp_path, '"C:\\temp\\demo"\n')
    sanitized_calls = []

    def _sanitized_parse(*args, **kwargs):
        sanitized_calls.append(kwargs)
        if "data" in kwargs:
            return None
        raise ValueError("boom")

    monkeypatch.setattr(parser.graph, "parse", _sanitized_parse)
    parser._parse_with_sanitized_content()
    assert "\\\\temp" in sanitized_calls[-1]["data"]


@pytest.mark.coverage_suite
def test_mapping_parser_graph_and_join_helper_methods(tmp_path: Path, monkeypatch):
    parser = _parser_for(tmp_path, SIMPLE_JSON_MAPPING)
    parser.graph = Graph()
    tm = URIRef("http://example.org/TM")
    ls = BNode()
    sm = BNode()
    pom = BNode()
    om = BNode()
    jc = BNode()

    parser.graph.add((tm, RDF.type, RR.TriplesMap))
    parser.graph.add((tm, RML.logicalSource, ls))
    parser.graph.add((ls, RML.source, RDFLiteral("items.json")))
    parser.graph.add((ls, RML.iterator, RDFLiteral("$.items[*]")))
    parser.graph.add((sm, RR.template, RDFLiteral("http://example.org/item/{id}")))
    parser.graph.add((tm, RR.subjectMap, sm))
    parser.graph.add((tm, RR.predicateObjectMap, pom))
    parser.graph.add((pom, RR.objectMap, om))
    parser.graph.add((pom, RR.predicateMap, BNode("pm")))
    parser.graph.add((om, RR.parentTriplesMap, tm))
    parser.graph.add((om, RR.joinCondition, jc))
    parser.graph.add((jc, RR.child, RDFLiteral("child.id")))
    parser.graph.add((jc, RR.parent, RDFLiteral("{parent_id}")))

    assert tm in parser._collect_triples_maps()
    assert parser._get_logical_source_literal(ls) == "items.json"
    assert parser._normalize_query_name("$.name") == "name"
    child_attrs, parent_attrs, child_queries, parent_queries = parser._extract_join_attributes(om)
    assert child_attrs == ["child.id"]
    assert parent_attrs == ["{parent_id}"]
    assert child_queries == [None]
    assert parent_queries == [None]

    source_mappings = {"id": "$.id"}
    parent_source_mappings, rename_map = parser._prepare_parent_source_mappings(source_mappings, tm, ["id"], ["$.id"])
    assert rename_map["id"] == "parent_id"
    assert "parent_id" in parent_source_mappings

    parser._q4_nojoin_parent = {om: tm}
    parser.graph.set((om, RML.reference, RDFLiteral("id")))
    child_mappings = parser._prepare_child_join_mappings({"id": "$.id", "name": "$.name"}, om, [], [], True)
    assert "id" not in child_mappings

    ref_expr = Reference("id")
    renamed = FunctionCall(lambda value: value, [ref_expr])
    parser._rename_reference_attributes(renamed, {"id": "parent_id"})
    assert ref_expr.attribute_name == "parent_id"

    monkeypatch.setattr("fog_rml.mapping.MappingParser.SourceFactory.create_source_operator", lambda **kwargs: object())
    assert parser._create_source_operator(ls, {"id": "$.id"}, "error") is not None


@pytest.mark.coverage_suite
def test_mapping_parser_expression_builders_cover_constant_reference_template_and_fnml(tmp_path: Path):
    parser = _parser_for(tmp_path, SIMPLE_JSON_MAPPING)
    parser.graph = Graph()

    const_tm = BNode()
    parser.graph.add((const_tm, RR.constant, URIRef("http://example.org/value")))
    assert isinstance(parser._create_constant_ext_expr(const_tm), Constant)

    ref_tm = BNode()
    parser.graph.add((ref_tm, RML.reference, RDFLiteral("name")))
    parser.graph.add((ref_tm, RR.termType, RR.IRI))
    ref_expr = parser._create_reference_ext_expr(ref_tm, "Literal")
    assert isinstance(ref_expr, FunctionCall)

    template_tm = BNode()
    parser.graph.add((template_tm, RR.template, RDFLiteral("http://example.org/{name}")))
    parser.graph.add((template_tm, RR.termType, RR.IRI))
    tmpl_expr = parser._create_template_ext_expr(template_tm, "IRI")
    assert isinstance(tmpl_expr, FunctionCall)

    literal_tm = BNode()
    parser.graph.add((literal_tm, RR.template, RDFLiteral("Name: {name}")))
    parser.graph.add((literal_tm, RR.language, RDFLiteral("fr")))
    literal_expr = parser._create_ext_expr(literal_tm)
    assert isinstance(literal_expr, FunctionCall)

    fn_node = BNode()
    fn_tm = BNode()
    pom_exec = BNode()
    pm_exec = BNode()
    om_exec = BNode()
    pom_arg = BNode()
    pm_arg = BNode()
    om_arg = BNode()
    parser.graph.add((fn_tm, FNML.functionValue, fn_node))
    parser.graph.add((fn_node, RR.predicateObjectMap, pom_exec))
    parser.graph.add((pom_exec, RR.predicateMap, pm_exec))
    parser.graph.add((pom_exec, RR.objectMap, om_exec))
    parser.graph.add((pm_exec, RR.constant, FNO.executes))
    parser.graph.add((om_exec, RR.constant, URIRef("http://example.org/f#join")))
    parser.graph.add((fn_node, RR.predicateObjectMap, pom_arg))
    parser.graph.add((pom_arg, RR.predicateMap, pm_arg))
    parser.graph.add((pom_arg, RR.objectMap, om_arg))
    parser.graph.add((pm_arg, RR.constant, URIRef("http://example.org/f#param_2")))
    parser.graph.add((om_arg, RR.constant, RDFLiteral("hello")))

    fn_expr = parser._create_fnml_ext_expr(fn_tm)
    assert isinstance(fn_expr, FunctionCall)
    assert parser._fnml_param_sort_key((URIRef("http://example.org/f#param_12"), None)) == 12
    assert parser._validated_language_tag(RDFLiteral("en")) == "en"


@pytest.mark.coverage_suite
def test_mapping_parser_resolves_logical_tables_and_explanations(tmp_path: Path, monkeypatch):
    parser = _parser_for(tmp_path, SIMPLE_JSON_MAPPING)
    parser.graph = Graph()

    tm = URIRef("http://example.org/TM")
    logical_table = BNode()
    database = URIRef("http://example.org/db")
    parser.graph.add((database, RDF.type, URIRef(f"{D2RQ}Database")))
    parser.graph.add((tm, RR.logicalTable, logical_table))
    parser.graph.add((logical_table, RR.tableName, RDFLiteral("demo")))
    logical_source = parser._resolve_logical_source(tm)
    assert logical_source is not None
    assert parser.graph.value(logical_source, RR.tableName) == RDFLiteral("demo")

    class _StaticOperator:
        def execute(self):
            return []

        def explain(self, indent: int = 0, prefix: str = "") -> str:
            return "static"

        def explain_json(self):
            return {"type": "static"}

    branch = ProjectOperator(
        ExtendOperator(
            ExtendOperator(_StaticOperator(), "subject", Constant(AlgebraIRI("http://example.org/s"))),
            "predicate",
            Constant(AlgebraIRI("http://example.org/p")),
        ),
        {"subject", "predicate"},
    )
    monkeypatch.setattr(parser, "parse", lambda: branch)
    assert parser.explain()
    assert parser.explain_json()["type"] == "Project"

    output = tmp_path / "explain.json"
    parser.save_explanation(str(output), format="json")
    assert output.exists()


@pytest.mark.coverage_suite
def test_mapping_parser_join_and_parent_resolution_helpers_cover_internal_branches(tmp_path: Path, monkeypatch):
    parser = _parser_for(tmp_path, SIMPLE_JSON_MAPPING)
    parser.graph = Graph()
    child_ls = BNode()
    parser.graph.add((child_ls, RML.source, RDFLiteral("child.json")))
    parent_tm = URIRef("http://example.org/parent")
    parent_ls = BNode()
    parent_sm = BNode()
    parser.graph.add((parent_tm, RML.logicalSource, parent_ls))
    parser.graph.add((parent_ls, RML.source, RDFLiteral("parent.json")))
    parser.graph.add((parent_tm, RR.subjectMap, parent_sm))
    parser.graph.add((parent_sm, RML.reference, RDFLiteral("id")))

    resolved_tm, resolved_ls = parser._resolve_parent_join_context(parent_tm, child_ls, ["id"], {parent_tm})
    assert resolved_tm == parent_tm
    assert resolved_ls == parent_ls
    assert parser._find_candidate_parent_triples_map(child_ls, ["id"], {parent_tm}) == parent_tm
    assert parser._candidate_matches_parent_attrs(parent_tm, ["id"]) is True
    assert parser._prefer_candidate_source("child.json", "parent.json") is True

    clone_parent_tm = URIRef("http://example.org/parent-clone")
    parser.graph.add((clone_parent_tm, RR.subjectMap, parent_sm))
    parser._q5_tm_clones = {clone_parent_tm: [(parent_ls, parent_sm, BNode())]}
    clone_tm, clone_ls = parser._resolve_parent_join_context(clone_parent_tm, child_ls, ["id"], {clone_parent_tm})
    assert clone_ls is not None
    assert clone_tm in parser._q5_tm_runtime_nodes.values()

    om = BNode()
    parser.graph.add((om, RR.template, RDFLiteral("http://example.org/{id}")))
    stripped = {"id": "$.id", "name": "$.name"}
    parser._strip_nojoin_child_mappings(stripped, om, [], True)
    assert "id" not in stripped

    added = {}
    parser._add_missing_child_join_mappings(added, ["id"], [None])
    assert added["id"] == "id"

    monkeypatch.setattr(parser, "_resolve_parent_join_context", lambda *args, **kwargs: (parent_tm, parent_ls))
    monkeypatch.setattr(parser, "_prepare_parent_source_mappings", lambda *args, **kwargs: ({"parent_id": "$.id"}, {"id": "parent_id"}))
    monkeypatch.setattr(parser, "_create_source_operator", lambda *args, **kwargs: object())
    monkeypatch.setattr(parser, "_build_parent_subject_expr", lambda *args, **kwargs: Constant(AlgebraIRI("http://example.org/parent/1")))
    monkeypatch.setattr(parser, "_prepare_child_join_mappings", lambda *args, **kwargs: {"id": "$.id"})
    monkeypatch.setattr("fog_rml.mapping.MappingParser.EquiJoinOperator", lambda *args, **kwargs: object())

    branch = parser._build_referencing_object_map_branch(
        URIRef("http://example.org/child"),
        om,
        child_ls,
        Constant(AlgebraIRI("http://example.org/child/1")),
        Constant(AlgebraIRI("http://example.org/p")),
        {"id": "$.id"},
        {parent_tm},
        parent_tm,
        ["id"],
        ["id"],
        ["$.id"],
        ["$.id"],
        False,
    )
    assert isinstance(branch, ExtendOperator)


@pytest.mark.coverage_suite
def test_mapping_parser_term_map_helper_methods_cover_remaining_builders(tmp_path: Path):
    parser = _parser_for(tmp_path, SIMPLE_JSON_MAPPING)
    parser.graph = Graph()

    tm = BNode()
    parser.graph.add((tm, RR.constant, RDFLiteral("bonjour", lang="fr")))
    assert parser._create_constant_ext_expr(tm).value.language == "fr"

    typed_tm = BNode()
    parser.graph.add((typed_tm, RR.constant, RDFLiteral("1", datatype=URIRef("http://www.w3.org/2001/XMLSchema#integer"))))
    assert parser._create_constant_ext_expr(typed_tm).value.datatype_iri.endswith("#integer")

    ref_tm = BNode()
    parser.graph.add((ref_tm, RML.reference, RDFLiteral("name")))
    parser.graph.add((ref_tm, RR.termType, RR.BlankNode))
    assert parser._create_reference_ext_expr(ref_tm, "Literal").function.__name__ == "to_bnode"

    template_tm = BNode()
    parser.graph.add((template_tm, RR.template, RDFLiteral("{name}")))
    parser.graph.add((template_tm, RR.termType, RR.BlankNode))
    assert parser._create_template_ext_expr(template_tm, "Literal").function.__name__ == "to_bnode"

    expr = parser._create_ext_expr(BNode())
    assert isinstance(expr, Constant)
    assert expr.value.value == "http://error"

    direct_fn_node = BNode()
    parser.graph.add((direct_fn_node, FNO.executes, URIRef("http://example.org/f#exec")))
    assert parser._resolve_fnml_function_uri(direct_fn_node) == "http://example.org/f#exec"
    assert parser._extract_fnml_param(BNode()) is None

    failing_pom = BNode()
    pm = BNode()
    om = BNode()
    parser.graph.add((failing_pom, RR.predicateMap, pm))
    parser.graph.add((failing_pom, RR.objectMap, om))
    parser.graph.add((pm, RR.constant, URIRef("http://example.org/f#arg_1")))
    parser.graph.add((om, RR.constant, RDFLiteral("x")))
    assert parser._extract_fnml_param(failing_pom)[0] == URIRef("http://example.org/f#arg_1")

    conflict_tm = BNode()
    parser.graph.add((conflict_tm, RR.language, RDFLiteral("fr")))
    parser.graph.add((conflict_tm, RR.datatype, URIRef("http://www.w3.org/2001/XMLSchema#string")))
    with pytest.raises(ValueError):
        parser._resolve_term_map_type_info(conflict_tm, "Literal")

    parser.base_iri = "http://example.org/base/"
    assert len(parser._build_reference_iri_expr(Reference("id")).arguments) == 3
    assert parser._build_template_concat_expr("").value.lexical_form == ""
    assert len(parser._build_template_iri_expr(Constant(AlgebraLiteral("x"))).arguments) == 2
    assert parser._build_literal_expr(Reference("name"), URIRef("http://www.w3.org/2001/XMLSchema#string"), None).function.__name__ == "to_literal"
    assert parser._build_literal_expr(Reference("name"), None, RDFLiteral("fr")).function.__name__ == "to_literal_lang"


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_mapping_parser_additional_edge_cases_cover_errors_and_text_save(tmp_path: Path, monkeypatch):
    parser = _parser_for(tmp_path, SIMPLE_JSON_MAPPING)
    parser.graph = Graph()

    with pytest.raises(ValueError):
        parser._build_base_subject_operator(URIRef("http://example.org/tm"), object())

    tm = URIRef("http://example.org/tm")
    ls = BNode()
    sm = BNode()
    pom = BNode()
    om = BNode()
    gm = BNode()
    parser.graph.add((tm, RR.subjectMap, sm))
    parser.graph.add((sm, RR.graphMap, gm))
    parser.graph.add((gm, RR.constant, URIRef("http://example.org/g")))
    projected = parser._apply_graph_and_project(ExtendOperator(object(), "object", Constant(AlgebraLiteral("x"))), pom, sm, tm)  # type: ignore[arg-type]
    assert isinstance(projected, ProjectOperator)

    parser.graph.add((om, RR.template, RDFLiteral("{complex.name}")))
    assert parser._extract_join_operand(om) == ("complex.name", "$['complex.name']")
    assert parser._extract_join_operand(None) == (None, None)

    assert parser._normalize_query_name("") == ""
    queries = {}
    parser._register_template_query_variables("{complex.name}", queries)
    assert queries["complex.name"] == "$['complex.name']"

    text_output = tmp_path / "explain.txt"
    monkeypatch.setattr(parser, "parse", lambda: type("P", (), {"explain": lambda self: "plan", "explain_json": lambda self: {"type": "plan"}})())
    parser.save_explanation(str(text_output), format="text")
    assert text_output.read_text(encoding="utf-8") == "plan"
@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_mapping_parser_handles_invalid_shapes_and_multiple_branches(write_mapping_files):
    invalid_mapping = SIMPLE_JSON_MAPPING.replace(
        'rr:subjectMap [ rr:template "http://example.org/item/{id}" ] ;',
        'rr:subjectMap [ rr:template "http://example.org/item/{id}" ] ;\n'
        '  rr:subjectMap [ rr:template "http://example.org/item/{name}" ] ;',
    )
    mapping_path = write_mapping_files(
        invalid_mapping,
        {"items.json": json.dumps({"items": [{"id": 1, "name": "Alice"}]})},
    )
    with pytest.raises(ValueError):
        MappingParser(str(mapping_path)).parse()

    multi_mapping_path = write_mapping_files(
        SIMPLE_JSON_MAPPING
        + """
<#TM2>
  a rr:TriplesMap ;
  rml:logicalSource [
    rml:source "items.json" ;
    rml:referenceFormulation ql:JSONPath ;
    rml:iterator "$.items[*]"
  ] ;
  rr:subjectMap [ rr:template "http://example.org/item/{id}" ] ;
  rr:predicateObjectMap [
    rr:predicateMap [ rr:constant <http://example.org/id> ] ;
    rr:objectMap [ rml:reference "id" ]
  ] .
""",
        {"items.json": json.dumps({"items": [{"id": 1, "name": "Alice"}]})},
    )
    assert isinstance(MappingParser(str(multi_mapping_path)).parse(), UnionOperator)


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_mapping_parser_edge_cases_cover_validation_and_error_branches(tmp_path: Path, monkeypatch):
    parser = _parser_for(tmp_path, SIMPLE_JSON_MAPPING)
    parser.graph = Graph()
    tm = URIRef("http://example.org/TM")
    sm1 = BNode()
    sm2 = BNode()
    parser.graph.add((tm, RDF.type, RR.TriplesMap))
    parser.graph.add((tm, RR.subjectMap, sm1))
    parser.graph.add((tm, RR.subjectMap, sm2))
    with pytest.raises(ValueError):
        parser._validate_raw_triples_maps()

    missing = MappingParser(str(tmp_path / "missing.ttl"))
    with pytest.raises(FileNotFoundError):
        missing._load_mapping_graph()

    parser = _parser_for(tmp_path, SIMPLE_JSON_MAPPING)
    parser.graph = Graph()
    term_map = BNode()
    parser.graph.add((term_map, RR.termType, RR.Literal))
    with pytest.raises(ValueError):
        parser._resolve_term_map_type_info(term_map, "IRI")

    parser.graph = Graph()
    parser.graph.add((term_map, RR.datatype, RDFLiteral("not-an-iri")))
    with pytest.raises(ValueError):
        parser._build_literal_expr(Reference("x"), RDFLiteral("not-an-iri"), None)

    with pytest.raises(ValueError):
        parser._validated_language_tag(RDFLiteral("fr_42"))

    parser = _parser_for(tmp_path, SIMPLE_JSON_MAPPING)
    monkeypatch.setattr(parser, "_collect_triples_maps", lambda: set())
    monkeypatch.setattr(parser, "_load_mapping_graph", lambda: None)
    with pytest.raises(ValueError):
        parser.parse()

