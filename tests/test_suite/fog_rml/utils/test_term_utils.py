from __future__ import annotations

import pytest
from rdflib import BNode as RDFBNode, Literal as RDFLiteral, URIRef

from fog_rml.algebra.Terms import BlankNode, IRI, Literal
from fog_rml.algebra.Tuple import EPSILON
from fog_rml.utils.term_utils import term_to_rdflib


@pytest.mark.coverage_suite
def test_term_utils_converts_terms_to_rdflib():
    assert term_to_rdflib(IRI("http://example.org/s")) == URIRef("http://example.org/s")
    assert term_to_rdflib(Literal("hello")) == RDFLiteral("hello", datatype=URIRef("http://www.w3.org/2001/XMLSchema#string"))
    assert term_to_rdflib(BlankNode("b1")) == RDFBNode("b1")


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_term_utils_handles_none_and_epsilon():
    assert term_to_rdflib(None) is None
    assert term_to_rdflib(EPSILON) is None
