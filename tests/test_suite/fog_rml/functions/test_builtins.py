from __future__ import annotations

import pytest

import fog_rml.functions.builtins as builtins_module
from fog_rml.algebra.Terms import BlankNode, IRI, Literal
from fog_rml.algebra.Tuple import EPSILON
from fog_rml.functions.builtins import (
    _build_absolute_iri,
    _is_absolute_iri,
    _join_reference_iri,
    _percent_encode,
    _resolve_with_base,
    _resolve_without_base,
    _to_string,
    concat,
    percent_encode_component,
    to_bnode,
    to_iri,
    to_literal,
    to_literal_lang,
)
from fog_rml.namespaces import XSD_INTEGER


@pytest.mark.coverage_suite
def test_builtins_create_terms():
    assert to_iri("http://example.org/a").value == "http://example.org/a"
    assert to_literal(7, XSD_INTEGER.value).lexical_form == "7"
    assert to_literal_lang("bonjour", "fr").language == "fr"
    assert concat("a", 2).lexical_form == "a2"
    assert percent_encode_component("a/b").lexical_form == "a%2Fb"


@pytest.mark.coverage_suite
def test_builtin_helpers_cover_string_and_iri_resolution_paths():
    assert _to_string(Literal("hello")) == "hello"
    assert _to_string(IRI("http://example.org/a")) == "http://example.org/a"
    assert _to_string(True) == "true"
    assert _to_string(BlankNode("b1")) is None
    assert _to_string(object()) is None

    assert _is_absolute_iri("http://example.org/a") is True
    assert _is_absolute_iri("not valid") is False
    assert _is_absolute_iri(12) is False

    assert _percent_encode("http://example.org/a path").endswith("a%20path")
    assert _percent_encode("segment value").endswith("segment%20value")
    assert _join_reference_iri("http://example.org/", "x") == "http://example.org/x"
    assert _build_absolute_iri("http://example.org/b").value == "http://example.org/b"
    assert _resolve_with_base("http://example.org/base/", "child", template_mode=False).value.endswith("/base/child")
    assert _resolve_with_base("http://example.org/base/", "a b", template_mode=True).value.endswith("/a%20b")
    assert _resolve_without_base("a b", template_mode=False) == EPSILON
    assert _resolve_without_base("urn:test:ok", template_mode=True).value == "urn:test:ok"


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_builtins_return_epsilon_or_hashed_blank_nodes_on_invalid_input():
    assert to_iri("not a valid iri", template_mode=False) == EPSILON
    assert to_bnode("contains space").identifier.startswith("b")


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_builtins_cover_epsilon_shortcuts_and_existing_literals():
    literal = Literal("hello")
    assert to_literal(literal, "http://www.w3.org/2001/XMLSchema#string") is literal
    assert to_literal(EPSILON, XSD_INTEGER.value) == EPSILON
    assert percent_encode_component(EPSILON) == EPSILON
    assert to_literal_lang(EPSILON, "fr") == EPSILON
    assert to_bnode(EPSILON) == EPSILON
    assert concat("a", EPSILON) == EPSILON


@pytest.mark.coverage_suite
def test_builtins_cover_remaining_resolution_and_exception_branches(monkeypatch):
    assert _is_absolute_iri("http://exa mple.org/a") is False
    monkeypatch.setattr(
        builtins_module.urllib.parse,
        "urlparse",
        lambda value: (_ for _ in ()).throw(ValueError("boom")),
    )
    assert _is_absolute_iri("http://example.org/a") is False

    monkeypatch.undo()
    assert _percent_encode("data:text/plain,a b").startswith("data:text/plain,")
    monkeypatch.setattr(
        builtins_module.urllib.parse,
        "urlsplit",
        lambda value: (_ for _ in ()).throw(ValueError("boom")),
    )
    assert _percent_encode("a b") == "a%20b"

    monkeypatch.setattr(builtins_module, "IRI", lambda value: (_ for _ in ()).throw(ValueError("boom")))
    assert _build_absolute_iri("http://example.org/bad") == EPSILON
    assert _resolve_without_base("urn:test:ok", template_mode=True) == EPSILON

    monkeypatch.undo()
    monkeypatch.setattr(builtins_module.urllib.parse, "urljoin", lambda base, lex: "joined")
    assert _join_reference_iri(object(), "child") == "joined"

    monkeypatch.setattr(builtins_module, "_join_reference_iri", lambda base, lex: None)
    assert _resolve_with_base("http://example.org/base/", "child", template_mode=False) == EPSILON

    monkeypatch.undo()
    monkeypatch.setattr(
        builtins_module.urllib.parse,
        "urljoin",
        lambda base, lex: (_ for _ in ()).throw(ValueError("boom")),
    )
    assert _resolve_with_base("http://example.org/base/", "a b", template_mode=True) == EPSILON


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_builtins_cover_base_iri_and_stable_blank_node_cases():
    assert to_iri(BlankNode("b1")) == EPSILON
    assert to_iri("child", base="http://example.org/base/", template_mode=False).value == "http://example.org/base/child"
    assert to_literal(BlankNode("b1"), XSD_INTEGER.value) == EPSILON
    assert to_literal_lang(BlankNode("b1"), "fr") == EPSILON
    assert to_bnode("simple_name").identifier == "simple_name"
