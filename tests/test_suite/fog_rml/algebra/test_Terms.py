from __future__ import annotations

import pytest

from fog_rml.algebra.Terms import BlankNode, IRI, InvalidIRIError, InvalidLanguageTagError, Literal


@pytest.mark.coverage_suite
def test_terms_repr_and_language_handling():
    iri = IRI("http://example.org/resource")
    literal = Literal("bonjour", language="fr")
    blank = BlankNode("b1")

    assert repr(iri) == "<http://example.org/resource>"
    assert repr(literal) == '"bonjour"@fr'
    assert repr(blank) == "_:b1"


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_terms_reject_invalid_iri_and_language_tag():
    with pytest.raises(InvalidIRIError):
        IRI("not valid")
    with pytest.raises(InvalidLanguageTagError):
        Literal("bonjour", language="fr_42")
