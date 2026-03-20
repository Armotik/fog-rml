from typing import Union
import urllib.parse
import hashlib
import re

from pyhartig.algebra.Tuple import EPSILON, _Epsilon, AlgebraicValue
from pyhartig.algebra.Terms import IRI, Literal, BlankNode
from pyhartig.functions.registry import FunctionRegistry
from pyhartig.namespaces import PYHARTIG_FUNCTIONS_BASE, XSD_STRING


def _to_string(value: AlgebraicValue) -> Union[str, None]:
    """
    Extract lexical string from AlgebraicValue or None if not representable.
    """
    if value == EPSILON or value is None:
        return None

    if isinstance(value, Literal):
        return value.lexical_form

    if isinstance(value, IRI):
        return value.value

    if isinstance(value, BlankNode):
        return None

    if isinstance(value, bool):
        return str(value).lower()

    if isinstance(value, (int, float)):
        return str(value)

    if isinstance(value, str):
        return value

    return None


def _is_absolute_iri(s: str) -> bool:
    """
    Checks whether a string is a syntactically valid absolute IRI candidate.
    """
    # Basic syntactic check: must have a scheme and a path or netloc, and must not
    # contain illegal characters such as spaces or control characters.
    if not isinstance(s, str):
        return False
    if any(ch.isspace() for ch in s):
        return False
    # Disallow common illegal/simple characters in IRIs
    illegal = set('<>"{}|\\^`')
    if any((c in illegal) for c in s):
        return False
    try:
        p = urllib.parse.urlparse(s)
        return bool(p.scheme and (p.netloc or p.path))
    except Exception:
        return False


def _percent_encode(s: str, preserve_percent: bool = False) -> str:
    """
    Percent-encodes an IRI string while preserving scheme-specific safe characters.
    """
    # Percent-encode but preserve IRI scheme and netloc when present.
    # When `preserve_percent` is True, existing percent-encodings ('%') are
    # treated as safe and will not be encoded again. This avoids double-
    # encoding when template insertion already produced percent-encoded
    # components.
    try:
        parts = urllib.parse.urlsplit(s)
        if parts.scheme:
            # Build safe character sets; include '%' when preserving percent
            path_safe = '/-._~!$&\'"()*+,;=:@%'
            query_safe = '=&?/+-._~!$&\'"()*+,;:@%'
            frag_safe = '%' if preserve_percent else ''
            if not preserve_percent:
                path = urllib.parse.quote(parts.path, safe='/-._~!$&\'"()*+,;=:@')
                query = urllib.parse.quote(parts.query, safe='=&?/+-._~!$&\'"()*+,;:@')
                fragment = urllib.parse.quote(parts.fragment, safe='')
            else:
                path = urllib.parse.quote(parts.path, safe=path_safe)
                query = urllib.parse.quote(parts.query, safe=query_safe)
                fragment = urllib.parse.quote(parts.fragment, safe=frag_safe)
            if parts.scheme.lower() == 'data':
                path = path.replace('%2C', ',').replace('%2c', ',')
            return urllib.parse.urlunsplit((parts.scheme, parts.netloc, path, query, fragment))
    except Exception:
        pass
    safe = '/-._~%'
    if not preserve_percent:
        safe = '/-._~'
    return urllib.parse.quote(s, safe=safe)


def _build_absolute_iri(value: str) -> Union[IRI, _Epsilon]:
    """
    Builds an IRI term from an absolute IRI string, or EPSILON on failure.
    """
    if not _is_absolute_iri(value):
        return EPSILON
    try:
        return IRI(value)
    except Exception:
        return EPSILON


def _join_reference_iri(base: str, lex: str) -> Union[str, None]:
    """
    Resolves a reference lexical form against a base IRI.
    """
    try:
        if isinstance(base, str):
            return base + lex
        return urllib.parse.urljoin(base, lex)
    except Exception:
        return None


def _resolve_with_base(base: str, lex: str, template_mode: bool) -> Union[IRI, _Epsilon]:
    """
    Resolves a lexical form to an IRI when a base IRI is available.
    """
    if not template_mode:
        joined = _join_reference_iri(base, lex)
        if not joined:
            return EPSILON
        return _build_absolute_iri(joined)

    try:
        safe = _percent_encode(lex, preserve_percent=True)
        return IRI(urllib.parse.urljoin(base, safe))
    except Exception:
        return EPSILON


def _resolve_without_base(lex: str, template_mode: bool) -> Union[IRI, _Epsilon]:
    """
    Resolves a lexical form to an IRI when no base IRI is available.
    """
    if not template_mode:
        return EPSILON
    try:
        return IRI(_percent_encode(lex, preserve_percent=True))
    except Exception:
        return EPSILON


def to_iri(value: AlgebraicValue, base: str = None, template_mode: bool = True) -> Union[IRI, _Epsilon]:
    """
    Convert an AlgebraicValue to an IRI.

    Behavior follows R2RML/RML:
    - template_mode=True: perform IRI-safe percent-encoding for template-valued term maps.
    - template_mode=False: for reference-valued term maps do NOT percent-encode; only accept
      absolute IRI or base+value that yields an absolute IRI; otherwise return EPSILON (data error).
    """
    lex = _to_string(value)
    if lex is None:
        return EPSILON

    absolute_iri = _build_absolute_iri(lex)
    if absolute_iri is not EPSILON:
        return absolute_iri

    if base:
        return _resolve_with_base(base, lex, template_mode)

    return _resolve_without_base(lex, template_mode)


def to_literal(value: AlgebraicValue, datatype: str) -> Union[Literal, _Epsilon]:
    """
    Converts an algebraic value to a typed RDF literal.
    """
    if value == EPSILON or value is None:
        return EPSILON
    if isinstance(value, Literal) and datatype == XSD_STRING.value:
        return value
    lex = _to_string(value)
    if lex is None:
        return EPSILON
    return Literal(lex, datatype)


def percent_encode_component(value: AlgebraicValue) -> Union[Literal, _Epsilon]:
    """
    Percent-encode a template insertion component. This encodes characters that
    would otherwise break IRIs (including ':' and '/'), leaving only the
    unreserved characters intact. Returns a Literal with the encoded lexical.
    """
    if value == EPSILON or value is None:
        return EPSILON
    lex = _to_string(value)
    if lex is None:
        return EPSILON
    # conservative safe set: unreserved characters per RFC3986
    encoded = urllib.parse.quote(lex, safe='-._~A-Za-z0-9')
    return Literal(encoded, XSD_STRING.value)


def to_literal_lang(value: AlgebraicValue, lang: str) -> Union[Literal, _Epsilon]:
    """
    Converts an algebraic value to a language-tagged RDF literal.
    """
    if value == EPSILON or value is None:
        return EPSILON
    lex = _to_string(value)
    if lex is None:
        return EPSILON
    return Literal(lex, language=lang)


def to_bnode(value: AlgebraicValue) -> Union[BlankNode, _Epsilon]:
    """
    Converts an algebraic value to a blank node, hashing when needed for a stable identifier.
    """
    if value == EPSILON or value is None:
        return EPSILON
    lex = _to_string(value)
    if lex is None:
        return EPSILON
    if isinstance(lex, str):
        candidate = lex.strip()
        if candidate and re.match(r"^[A-Za-z_]\w*$", candidate, re.ASCII):
            return BlankNode(candidate)
    hash_object = hashlib.sha256(lex.encode('utf-8'))
    bnode_id = f"b{hash_object.hexdigest()}"
    return BlankNode(bnode_id)


def concat(*args: AlgebraicValue) -> Union[Literal, _Epsilon]:
    """
    Concatenates algebraic values into a single xsd:string literal.
    """
    result = ""
    for v in args:
        s = _to_string(v)
        if s is None:
            return EPSILON
        result += s
    return Literal(result, XSD_STRING.value)


try:
    FunctionRegistry.register(f"{PYHARTIG_FUNCTIONS_BASE}concat", concat)
    FunctionRegistry.register(f"{PYHARTIG_FUNCTIONS_BASE}toIRI", to_iri)
    FunctionRegistry.register(f"{PYHARTIG_FUNCTIONS_BASE}toLiteral", to_literal)
    FunctionRegistry.register(f"{PYHARTIG_FUNCTIONS_BASE}toBNode", to_bnode)
    FunctionRegistry.register(f"{PYHARTIG_FUNCTIONS_BASE}toLiteralLang", to_literal_lang)
    FunctionRegistry.register(f"{PYHARTIG_FUNCTIONS_BASE}percentEncodeComponent", percent_encode_component)
except Exception:
    pass
