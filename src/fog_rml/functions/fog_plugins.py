import hashlib
import urllib.parse

from fog_rml.functions.registry import FunctionRegistry
from fog_rml.functions import builtins
from fog_rml.algebra.Terms import IRI, BlankNode, Literal
from fog_rml.namespaces import FOG_RML_ERROR_URI

ERROR_MARKER = "error"
BRACKETED_ERROR_MARKER = "<error>"
BRACKETED_ERROR_URI = f"<{FOG_RML_ERROR_URI}>"
TRAILING_BRACKET_ERROR_URI = f"{FOG_RML_ERROR_URI}>"
ERROR_MARKERS = frozenset((
    FOG_RML_ERROR_URI,
    TRAILING_BRACKET_ERROR_URI,
    BRACKETED_ERROR_URI,
    ERROR_MARKER,
    BRACKETED_ERROR_MARKER,
))


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


def _strip_angle(s: str) -> str:
    """
    Removes surrounding angle brackets from serialized RDF terms.
    :param s: Candidate string.
    :return: Unwrapped string when bracketed, otherwise the original value.
    """
    if s and s.startswith('<') and s.endswith('>'):
        return s[1:-1]
    return s


def _is_error_marker(s: str) -> bool:
    """
    Detects explicit error marker values used as missing identifiers.
    :param s: Candidate string.
    :return: True when the value is an error marker.
    """
    if not s:
        return True
    return s.lower().strip() in ERROR_MARKERS


def _fallback_id_iri(*parts: str) -> IRI:
    """
    Builds a deterministic fallback identifier IRI from source values.
    :param parts: Identifier source values.
    :return: Stable fog-rml identifier IRI.
    """
    combined = "|".join(parts)
    h = hashlib.sha256(combined.encode('utf-8')).hexdigest()
    return IRI(f"https://fog-rml.org/id/{h}")


def subject_for_row(doi_val, id_val, fallback_val):
    # prefer DOI, then id/url, then fallback; return IRI or BlankNode
    doi = _strip_angle(_to_py_str(doi_val))
    ident = _strip_angle(_to_py_str(id_val))
    fb = _strip_angle(_to_py_str(fallback_val))

    candidate = doi or ident or fb
    if not candidate:
        return _fallback_id_iri(doi, ident, fb)

    if _is_error_marker(candidate):
        candidate = ''

    if not candidate:
        return _fallback_id_iri(doi, ident, fb)

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
        return _fallback_id_iri(candidate)


def graph_for_source(source_name, id_val, year_val):
    src = _to_py_str(source_name) or "unknown"
    id_s = _to_py_str(id_val) or ""
    year_s = _to_py_str(year_val) or ""
    # filter error markers from id/year so graph URIs don't end with error
    if _is_error_marker(id_s):
        id_s = ''
    if _is_error_marker(year_s):
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
