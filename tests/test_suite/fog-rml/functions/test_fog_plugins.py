import hashlib
from fog_rml.functions.fog_plugins import subject_for_row, graph_for_source
from fog_rml.algebra.Terms import IRI


def test_subject_for_row_empty_returns_hashed_iri():
    res = subject_for_row(None, None, None)
    assert isinstance(res, IRI)
    assert res.value.startswith('https://fog-rml.org/id/')
    # ensure deterministic length (sha256 hex)
    h = res.value.split('/')[-1]
    assert len(h) == 64


def test_subject_for_row_error_marker_treated_as_missing():
    res = subject_for_row('http://error', None, None)
    assert isinstance(res, IRI)
    assert res.value.startswith('https://fog-rml.org/id/')


def test_subject_for_row_doi_produces_doi_org():
    res = subject_for_row('10.1234/ABC', None, None)
    assert isinstance(res, IRI)
    assert res.value == 'https://doi.org/10.1234/ABC'


def test_subject_for_row_url_preserved():
    url = 'https://example.org/foo bar'
    res = subject_for_row(url, None, None)
    assert isinstance(res, IRI)
    assert res.value.startswith('https://')


def test_graph_for_source_filters_error_markers():
    g = graph_for_source('openalex', 'http://error', '<error>')
    assert isinstance(g, IRI)
    assert g.value == 'https://fog-rml.org/graph/openalex'

