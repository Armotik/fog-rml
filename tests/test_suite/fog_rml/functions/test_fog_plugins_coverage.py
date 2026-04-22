import importlib
import hashlib
import types

import pytest

import fog_rml.functions.fog_plugins as fp_mod
from fog_rml.functions import registry as reg_mod


def test__to_py_str_handles_value_property_exception():
    class BadVal:
        @property
        def value(self):
            raise RuntimeError('boom')

        def __str__(self):
            return 'badval'

    assert fp_mod._to_py_str(BadVal()) == 'badval'


def test_subject_for_row_empty_deterministic():
    iri = fp_mod.subject_for_row(None, None, None)
    assert hasattr(iri, 'value')
    assert str(iri.value).startswith('https://fog-rml.org/id/')


def test_subject_for_row_iri_raising_falls_back_to_hash(monkeypatch):
    # Make IRI raise for inputs that contain the candidate, but accept hash-based ids
    candidate = '10.1234/XYZ'

    def fake_iri_constructor(val):
        s = str(val)
        if 'fog-rml.org/id/' in s:
            # allow hash-based ids (assume 64 hex chars)
            tail = s.split('fog-rml.org/id/')[-1]
            if len(tail) == 64:
                obj = types.SimpleNamespace(value=s)
                return obj
        # otherwise simulate constructor failure
        raise RuntimeError('IRI failed')

    monkeypatch.setattr(fp_mod, 'IRI', fake_iri_constructor)

    # This will try DOI path (which raises), then safe path (raises), then hash fallback (allowed)
    out = fp_mod.subject_for_row(candidate, None, None)
    assert hasattr(out, 'value')
    assert out.value.startswith('https://fog-rml.org/id/')
    # ensure suffix is a 64-hex hash
    suffix = out.value.split('/')[-1]
    assert len(suffix) == 64


def test_graph_for_source_fallbacks(monkeypatch):
    # Case 1: primary IRI fails, fallback IRI(src) succeeds
    calls = []

    def fake_iri(val):
        s = str(val)
        calls.append(s)
        # first call (full iri) should fail
        if s.startswith('https://fog-rml.org/graph/') and '/' in s[len('https://fog-rml.org/graph/'):]:
            raise RuntimeError('fail full')
        # second call (graph/src) should succeed
        return types.SimpleNamespace(value=s)

    monkeypatch.setattr(fp_mod, 'IRI', fake_iri)
    out = fp_mod.graph_for_source('srcname', 'ID123', '2020')
    assert hasattr(out, 'value')
    # ensure fallback to graph/src happened (no id/year parts)
    assert out.value == 'https://fog-rml.org/graph/srcname'

    # Case 2: both IRI attempts fail -> get BlankNode
    def always_fail(val):
        raise RuntimeError('always')

    monkeypatch.setattr(fp_mod, 'IRI', always_fail)
    out2 = fp_mod.graph_for_source('src2', 'id', 'y')
    # should be a BlankNode-like (has no 'value' but has identifier via str)
    assert out2.__class__.__name__ in ('BlankNode', 'SimpleNamespace') or hasattr(out2, 'value') is False


def test_register_try_block(monkeypatch):
    # Force FunctionRegistry.register to raise during module import; reload module
    original_register = reg_mod.FunctionRegistry.register

    def raising_register(*a, **k):
        raise RuntimeError('registry fail')

    monkeypatch.setattr(reg_mod.FunctionRegistry, 'register', raising_register)

    # reload the module - it must not raise despite register failing
    importlib.reload(fp_mod)

    # restore original behavior for cleanliness
    monkeypatch.setattr(reg_mod.FunctionRegistry, 'register', original_register)
