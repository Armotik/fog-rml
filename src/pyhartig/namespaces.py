from typing import Any

from pyhartig.algebra.Terms import IRI

# Base Namespaces (Strings)
_RDF_BASE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
_RDFS_BASE = "http://www.w3.org/2000/01/rdf-schema#"
_XSD_BASE = "http://www.w3.org/2001/XMLSchema#"
_FOAF_BASE = "http://xmlns.com/foaf/0.1/"
_RML_BASE = "http://semweb.mmlab.be/ns/rml#"
_RR_BASE = "http://www.w3.org/ns/r2rml#"

# XSD Datatypes (Typed as IRI objects)
XSD_STRING = IRI(f"{_XSD_BASE}string")
XSD_INTEGER = IRI(f"{_XSD_BASE}integer")
XSD_DOUBLE = IRI(f"{_XSD_BASE}double")
XSD_BOOLEAN = IRI(f"{_XSD_BASE}boolean")
XSD_DECIMAL = IRI(f"{_XSD_BASE}decimal")
XSD_DATE = IRI(f"{_XSD_BASE}date")
XSD_DATETIME = IRI(f"{_XSD_BASE}dateTime")

# RDF Syntax
RDFS_LABEL = IRI(f"{_RDFS_BASE}label")
RDFS_COMMENT = IRI(f"{_RDFS_BASE}comment")
RDFS_CLASS = IRI(f"{_RDFS_BASE}Class")
RDFS_SUBCLASS = IRI(f"{_RDFS_BASE}subClassOf")

# RML & R2RML
RML_SOURCE = IRI(f"{_RML_BASE}source")
RML_REFERENCE_FORM = IRI(f"{_RML_BASE}referenceFormulation")
RML_ITERATOR = IRI(f"{_RML_BASE}iterator")

RR_SUBJECT_MAP = IRI(f"{_RR_BASE}subjectMap")
RR_PREDICATE_MAP = IRI(f"{_RR_BASE}predicateMap")
RR_OBJECT_MAP = IRI(f"{_RR_BASE}objectMap")
RR_CLASS = IRI(f"{_RR_BASE}class")
RR_CONSTANT = IRI(f"{_RR_BASE}constant")
RR_TEMPLATE = IRI(f"{_RR_BASE}template")
RR_REFERENCE = IRI(f"{_RR_BASE}reference")
RR_TERM_TYPE = IRI(f"{_RR_BASE}termType")
RR_IRI = IRI(f"{_RR_BASE}IRI")
RR_LITERAL = IRI(f"{_RR_BASE}Literal")
RR_BLANK_NODE = IRI(f"{_RR_BASE}BlankNode")


# Utilities
def get_xsd_from_python_type(value: Any) -> IRI:
    """
    Returns the appropriate XSD datatype IRI for a given Python value.
    Defaults to XSD_STRING.
    :param value: Python value
    :return: Corresponding XSD datatype IRI
    """
    if isinstance(value, bool):
        return XSD_BOOLEAN
    elif isinstance(value, int):
        return XSD_INTEGER
    elif isinstance(value, float):
        return XSD_DOUBLE
    return XSD_STRING
