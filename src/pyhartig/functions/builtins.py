from typing import Union
import urllib.parse
import hashlib
import re

from pyhartig.algebra.Tuple import EPSILON, _Epsilon, AlgebraicValue
from pyhartig.algebra.Terms import IRI, Literal, BlankNode


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


def to_iri(value: AlgebraicValue, base: str = None, template_mode: bool = True) -> Union[IRI, _Epsilon]:
    """
    Convert an AlgebraicValue to an IRI.

    Behavior follows R2RML/RML:
    - template_mode=True: perform IRI-safe percent-encoding for template-valued term maps.
    - template_mode=False: for reference-valued term maps do NOT percent-encode; only accept
      absolute IRI or base+value that yields an absolute IRI; otherwise return EPSILON (data error).
    """
    if value == EPSILON or value is None:
        return EPSILON

    # obtain lexical form
    lex = _to_string(value)
    if lex is None:
        return EPSILON

    # If lex looks like absolute IRI, accept it
    if _is_absolute_iri(lex):
        try:
            return IRI(lex)
        except Exception:
            return EPSILON

    # If base provided, try to resolve by simple concatenation (urljoin)
    if base:
        try:
            # For reference-valued term maps we MUST NOT percent-encode; resolve
            # by simple string concatenation so that path segments like "path/../x"
            # are preserved rather than normalized by urljoin.
            if not template_mode:
                if isinstance(base, str):
                    if base.endswith('/'):
                        joined = base + lex
                    else:
                        joined = base + lex
                else:
                    joined = urllib.parse.urljoin(base, lex)
                if _is_absolute_iri(joined):
                    return IRI(joined)

            # Template mode: percent-encode the assembled lexical and resolve
            # using urljoin (preserves intended semantics for templates).
            if template_mode:
                # When templates already percent-encode inserted components
                # we want to avoid double-encoding '%' characters.
                safe = _percent_encode(lex, preserve_percent=True)
                joined_safe = urllib.parse.urljoin(base, safe)
                try:
                    return IRI(joined_safe)
                except Exception:
                    return EPSILON
        except Exception:
            pass

        # reference-mode and base did not produce absolute IRI => data error
        return EPSILON

    # No base provided
    if template_mode:
        try:
            # Preserve existing percent-encodings when converting template
            # lexical values into IRIs to avoid double-encoding.
            return IRI(_percent_encode(lex, preserve_percent=True))
        except Exception:
            return EPSILON

    # Reference-mode without base cannot produce an IRI from a non-absolute value
    return EPSILON


def to_literal(value: AlgebraicValue, datatype: str) -> Union[Literal, _Epsilon]:
    if value == EPSILON or value is None:
        return EPSILON
    if isinstance(value, Literal) and datatype == "http://www.w3.org/2001/XMLSchema#string":
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
    return Literal(encoded, "http://www.w3.org/2001/XMLSchema#string")


def to_literal_lang(value: AlgebraicValue, lang: str) -> Union[Literal, _Epsilon]:
    if value == EPSILON or value is None:
        return EPSILON
    lex = _to_string(value)
    if lex is None:
        return EPSILON
    return Literal(lex, language=lang)


def to_bnode(value: AlgebraicValue) -> Union[BlankNode, _Epsilon]:
    if value == EPSILON or value is None:
        return EPSILON
    lex = _to_string(value)
    if lex is None:
        return EPSILON
    if isinstance(lex, str):
        candidate = lex.strip()
        if candidate and re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", candidate):
            return BlankNode(candidate)
    hash_object = hashlib.sha1(lex.encode('utf-8'))
    bnode_id = f"b{hash_object.hexdigest()}"
    return BlankNode(bnode_id)


def concat(*args: AlgebraicValue) -> Union[Literal, _Epsilon]:
    result = ""
    for v in args:
        s = _to_string(v)
        if s is None:
            return EPSILON
        result += s
    return Literal(result, "http://www.w3.org/2001/XMLSchema#string")