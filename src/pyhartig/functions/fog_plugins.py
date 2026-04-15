import hashlib
import urllib.parse

from pyhartig.functions.registry import FunctionRegistry
from pyhartig.functions import builtins
from pyhartig.algebra.Terms import IRI, BlankNode, Literal


def _to_py_str(v):
    try:
        # If v is an IRI/Literal/BlankNode-like term, prefer its raw value
        if hasattr(v, 'value'):
            return getattr(v, 'value') or ""
        if hasattr(v, 'lexical_form'):
            return getattr(v, 'lexical_form') or ""
        return builtins._to_string(v) or ""
    except Exception:
        try:
            return str(v)
        except Exception:
            return ""


def subject_for_row(doi_val, id_val, fallback_val):
    # prefer DOI, then id/url, then fallback; return IRI or BlankNode
    doi = _to_py_str(doi_val)
    ident = _to_py_str(id_val)
    fb = _to_py_str(fallback_val)

    # strip angle-bracketed IRIs often produced by some serializations
    def _strip_angle(s: str) -> str:
        if s and s.startswith('<') and s.endswith('>'):
            return s[1:-1]
        return s

    doi = _strip_angle(doi)
    ident = _strip_angle(ident)
    fb = _strip_angle(fb)

    candidate = doi or ident or fb
    if not candidate:
        # deterministic fallback IRI based on inputs (empty candidate)
        combined = f"{doi}|{ident}|{fb}"
        h = hashlib.sha256(combined.encode('utf-8')).hexdigest()
        return IRI(f"https://fog-rml.org/id/{h}")

    # treat explicit error markers as missing
    def _is_error_marker(s: str) -> bool:
        if not s:
            return True
        low = s.lower().strip()
        return low in ('http://error', 'http://error>', '<http://error>', 'error', '<error>')

    if _is_error_marker(candidate):
        candidate = ''

    if not candidate:
        # deterministic fallback IRI based on original inputs
        combined = f"{doi}|{ident}|{fb}"
        h = hashlib.sha256(combined.encode('utf-8')).hexdigest()
        return IRI(f"https://fog-rml.org/id/{h}")

    # if candidate looks like a URL, return as IRI
    if candidate.startswith("http://") or candidate.startswith("https://"):
        try:
            return IRI(candidate)
        except Exception:
            pass

    # if candidate looks like a DOI (starts with 10.) produce doi.org IRI
    if candidate.startswith("10."):
        iri = f"https://doi.org/{candidate}"
        try:
            return IRI(iri)
        except Exception:
            pass

    # otherwise create a stable IRI-safe path under fog-rml
    safe = urllib.parse.quote(candidate, safe="")
    try:
        return IRI(f"https://fog-rml.org/id/{safe}")
    except Exception:
        # deterministic fallback IRI using candidate hash
        h = hashlib.sha256(candidate.encode('utf-8')).hexdigest()
        return IRI(f"https://fog-rml.org/id/{h}")


def graph_for_source(source_name, id_val, year_val):
    src = _to_py_str(source_name) or "unknown"
    id_s = _to_py_str(id_val) or ""
    year_s = _to_py_str(year_val) or ""
    # filter error markers from id/year so graph URIs don't end with error
    if id_s and id_s.lower().strip() in ('http://error', '<http://error>', 'error', '<error>'):
        id_s = ''
    if year_s and year_s.lower().strip() in ('http://error', '<http://error>', 'error', '<error>'):
        year_s = ''
    parts = [src]
    if id_s:
        parts.append(urllib.parse.quote(id_s, safe=""))
    if year_s:
        parts.append(urllib.parse.quote(year_s, safe=""))
    iri = "https://fog-rml.org/graph/" + "/".join(parts)
    try:
        return IRI(iri)
    except Exception:
        # best-effort fallback
        try:
            return IRI(f"https://fog-rml.org/graph/{src}")
        except Exception:
            return BlankNode(f"b{hashlib.sha256(iri.encode('utf-8')).hexdigest()}")


try:
    FunctionRegistry.register("http://fog-rml.org/functions#subject_for_row", subject_for_row)
    FunctionRegistry.register("http://fog-rml.org/functions#graph_for_source", graph_for_source)
except Exception:
    pass
