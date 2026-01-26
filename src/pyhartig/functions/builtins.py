from typing import Union
import urllib.parse
import hashlib

from pyhartig.algebra.Tuple import EPSILON, _Epsilon, AlgebraicValue
from pyhartig.algebra.Terms import IRI, Literal, BlankNode


def _to_string(value: AlgebraicValue) -> Union[str, None]:
    """
    Extracts the lexical string representation from an AlgebraicValue.
    :param value: Value to convert
    :return: String representation or None if not convertible
    """
    if value == EPSILON or value is None:
        return None

    if isinstance(value, Literal):
        return value.lexical_form

    if isinstance(value, IRI):
        return value.value
    
    if isinstance(value, BlankNode):
        return None

    # Accept native Python primitives as input as well
    if isinstance(value, bool):
        return str(value).lower()

    if isinstance(value, (int, float)):
        return str(value)

    if isinstance(value, str):
        return value

    # BlankNodes do not have a standard "string" representation here
    return None


def to_iri(value: AlgebraicValue, base: str = None) -> Union[IRI, _Epsilon]:
    """
    Convert a AlgebraicValue to an IRI.
    :param value: Value to convert
    :param base: Optional base IRI for resolution
    :return: IRI or EPSILON if conversion is not possible
    """
    if value == EPSILON or value is None:
        return EPSILON

    # Get string representation
    lex = _to_string(value)
    if lex is None:
        return EPSILON

    # Resolve against base if provided
    if base:
        lex = urllib.parse.urljoin(base, lex)

    # Create IRI
    try:
        return IRI(lex)
    except ValueError:
        return EPSILON

def to_literal(value: AlgebraicValue, datatype: str) -> Union[Literal, _Epsilon]:
    """
    Convert an AlgebraicValue to a Literal with the specified datatype.
    :param value: Value to convert
    :param datatype: Datatype IRI for the Literal
    :return: Literal or EPSILON if conversion is not possible
    """
    if value == EPSILON or value is None:
        return EPSILON

    lex = _to_string(value)

    if lex is None:
        return EPSILON

    return Literal(lex, datatype)

def to_bnode(value: AlgebraicValue) -> Union[BlankNode, _Epsilon]:
    """
    Converts a value to a deterministic Blank Node (Skolemization)
    :param value: The input value (IRI or Literal) used to seed the Blank Node ID.
    :return: A BlankNode with a deterministic identifier, or EPSILON
    """
    if value == EPSILON or value is None:
        return EPSILON

    lex = _to_string(value)

    if lex is None:
        return EPSILON

    # Create a stable hash of the lexical form
    # We prefix with "b" to ensure it looks like a standard BNode ID (e.g., _:b5ea...)
    hash_object = hashlib.sha1(lex.encode('utf-8'))
    bnode_id = f"b{hash_object.hexdigest()}"

    return BlankNode(bnode_id)


def concat(*args: AlgebraicValue) -> Union[Literal, _Epsilon]:
    """
    Concatenate multiple AlgebraicValues into a single string Literal.
    Logic:
    - If value is valid: generate _:hash(value)
    - If value is EPSILON/None: return EPSILON

    This determinism allows strictly equivalent Blank Nodes to be generated
    from different pipeline branches, enabling Joins.

    :param args: Values to concatenate
    :return: Literal with concatenated string or EPSILON if conversion is not possible
    """
    result_str = ""
    for val in args:
        s = _to_string(val)
        if s is None:
            # If any argument is invalid/Epsilon, propagate error
            return EPSILON
        result_str += s

    return Literal(result_str, "http://www.w3.org/2001/XMLSchema#string")