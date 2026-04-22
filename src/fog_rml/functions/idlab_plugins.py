import os
from typing import Any

from fog_rml.functions.registry import FunctionRegistry
from fog_rml.functions import builtins
from fog_rml.namespaces import IDLAB_FUNCTIONS_BASE


def _to_py_str(v: Any) -> str:
    """
    Converts an algebraic value to a plain Python string for plugin evaluation.
    """
    try:
        # reuse builtins helper if available
        return builtins._to_string(v) or ""
    except Exception:
        try:
            return str(v)
        except Exception:
            return ""


def true_condition(*args) -> bool:
    """
    Returns True when at least one argument has a non-empty lexical form.
    """
    # Return True if any non-empty argument (simple truthiness check)
    for a in args:
        if _to_py_str(a):
            return True
    return False


def equal(a, b) -> bool:
    """
    Compares two values by their normalized Python string representation.
    """
    return _to_py_str(a) == _to_py_str(b)


def not_equal(a, b) -> bool:
    """
    Returns True when two values differ after string normalization.
    """
    return _to_py_str(a) != _to_py_str(b)


def get_mime_type(filename) -> str:
    """
    Infers a MIME type from a filename extension.
    """
    name = _to_py_str(filename)
    _, ext = os.path.splitext(name.lower())
    return {
        '.ttl': 'text/turtle',
        '.nq': 'application/n-quads',
        '.nt': 'application/n-triples',
        '.csv': 'text/csv',
        '.json': 'application/json',
        '.jsonld': 'application/ld+json',
        '.xml': 'application/xml'
    }.get(ext, 'application/octet-stream')


# Register these sample implementations under the idlab function URIs
try:
    FunctionRegistry.register(f"{IDLAB_FUNCTIONS_BASE}trueCondition", true_condition)
    FunctionRegistry.register(f"{IDLAB_FUNCTIONS_BASE}equal", equal)
    FunctionRegistry.register(f"{IDLAB_FUNCTIONS_BASE}notEqual", not_equal)
    FunctionRegistry.register(f"{IDLAB_FUNCTIONS_BASE}getMIMEType", get_mime_type)
except Exception:
    pass

