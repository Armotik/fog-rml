import os
from typing import Any

from pyhartig.functions.registry import FunctionRegistry
from pyhartig.functions import builtins


def _to_py_str(v: Any) -> str:
    try:
        # reuse builtins helper if available
        return builtins._to_string(v) or ""
    except Exception:
        try:
            return str(v)
        except Exception:
            return ""


def trueCondition(*args) -> bool:
    # Return True if any non-empty argument (simple truthiness check)
    for a in args:
        if _to_py_str(a):
            return True
    return False


def equal(a, b) -> bool:
    return _to_py_str(a) == _to_py_str(b)


def notEqual(a, b) -> bool:
    return _to_py_str(a) != _to_py_str(b)


def getMIMEType(filename) -> str:
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
    FunctionRegistry.register('http://example.com/idlab/function/trueCondition', trueCondition)
    FunctionRegistry.register('http://example.com/idlab/function/equal', equal)
    FunctionRegistry.register('http://example.com/idlab/function/notEqual', notEqual)
    FunctionRegistry.register('http://example.com/idlab/function/getMIMEType', getMIMEType)
except Exception:
    pass
