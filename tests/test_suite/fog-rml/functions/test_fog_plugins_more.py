import types
import urllib.parse
from fog_rml.functions import fog_plugins
from fog_rml.algebra.Terms import IRI, BlankNode


def test_to_py_str_handles_term_like_objects():
    class T:
        def __init__(self, v):
            self.value = v

    # use a term-like object as doi_val
    t = T('10.1000/xyz')
    res = fog_plugins.subject_for_row(t, None, None)
    assert isinstance(res, IRI)
    assert res.value == 'https://doi.org/10.1000/xyz'


def test_strip_angle_and_safe_quote():
    # angle bracket should be stripped
    val = '<Some Identifier/with spaces>'
    res = fog_plugins.subject_for_row(val, None, None)
    assert isinstance(res, IRI)
    assert 'Some%20Identifier' in res.value


def test_graph_for_source_fallback_on_iri_error(monkeypatch):
    # force IRI constructor to raise to hit fallback branch
    monkeypatch.setattr(fog_plugins, 'IRI', lambda v: (_ for _ in ()).throw(ValueError('bad')))
    g = fog_plugins.graph_for_source('src', 'id', '2020')
    # after IRI fails, should try fallback and eventually return BlankNode or IRI
    assert isinstance(g, (IRI, BlankNode))

