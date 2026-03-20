from typing import Any

from pyhartig.algebra.Terms import IRI

# Base Namespaces (Strings)
# These are RDF vocabulary namespace IRIs, not runtime HTTP endpoints. # NOSONAR
RDF_BASE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"  # NOSONAR
RDFS_BASE = "http://www.w3.org/2000/01/rdf-schema#"  # NOSONAR
XSD_BASE = "http://www.w3.org/2001/XMLSchema#"  # NOSONAR
FOAF_BASE = "http://xmlns.com/foaf/0.1/"  # NOSONAR
RML_BASE = "http://semweb.mmlab.be/ns/rml#"  # NOSONAR
RR_BASE = "http://www.w3.org/ns/r2rml#"  # NOSONAR
QL_BASE   = "http://semweb.mmlab.be/ns/ql#"  # NOSONAR
SD_BASE = "http://www.w3.org/ns/sparql-service-description#"  # NOSONAR
D2RQ_BASE = "http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#"  # NOSONAR
FNML_BASE = "http://semweb.mmlab.be/ns/fnml#"  # NOSONAR
FNO_BASE = "https://w3id.org/function/ontology#"

# Intentional non-vocabulary IRIs centralized to document their usage. # NOSONAR
PYHARTIG_FUNCTIONS_BASE = "http://pyhartig.org/functions#"  # NOSONAR
IDLAB_FUNCTIONS_BASE = "http://example.com/idlab/function/"  # NOSONAR
EXAMPLE_ORG_BASE = "http://example.org/"  # NOSONAR
PYHARTIG_ERROR_URI = "http://error"  # NOSONAR

# XSD Datatypes (Typed as IRI objects)
XSD_STRING = IRI(f"{XSD_BASE}string")
XSD_INTEGER = IRI(f"{XSD_BASE}integer")
XSD_DOUBLE = IRI(f"{XSD_BASE}double")
XSD_BOOLEAN = IRI(f"{XSD_BASE}boolean")
XSD_DECIMAL = IRI(f"{XSD_BASE}decimal")
XSD_DATE = IRI(f"{XSD_BASE}date")
XSD_DATETIME = IRI(f"{XSD_BASE}dateTime")

# RDF Syntax
RDF_LANGSTRING = IRI(f"{RDF_BASE}langString")
RDF_TYPE = IRI(f"{RDF_BASE}type")
RDFS_LABEL = IRI(f"{RDFS_BASE}label")
RDFS_COMMENT = IRI(f"{RDFS_BASE}comment")
RDFS_CLASS = IRI(f"{RDFS_BASE}Class")
RDFS_SUBCLASS = IRI(f"{RDFS_BASE}subClassOf")

# Common FOAF terms
FOAF_PERSON = IRI(f"{FOAF_BASE}Person")
FOAF_NAME = IRI(f"{FOAF_BASE}name")

# RML & R2RML
RML_SOURCE = IRI(f"{RML_BASE}source")
RML_REFERENCE_FORM = IRI(f"{RML_BASE}referenceFormulation")
RML_ITERATOR = IRI(f"{RML_BASE}iterator")

RR_SUBJECT_MAP = IRI(f"{RR_BASE}subjectMap")
RR_PREDICATE_MAP = IRI(f"{RR_BASE}predicateMap")
RR_OBJECT_MAP = IRI(f"{RR_BASE}objectMap")
RR_CLASS = IRI(f"{RR_BASE}class")
RR_CONSTANT = IRI(f"{RR_BASE}constant")
RR_TEMPLATE = IRI(f"{RR_BASE}template")
RR_REFERENCE = IRI(f"{RR_BASE}reference")
RR_TERM_TYPE = IRI(f"{RR_BASE}termType")
RR_IRI = IRI(f"{RR_BASE}IRI")
RR_LITERAL = IRI(f"{RR_BASE}Literal")
RR_BLANK_NODE = IRI(f"{RR_BASE}BlankNode")
RR_DEFAULT_GRAPH = IRI(f"{RR_BASE}defaultGraph")
FNML_FUNCTION_VALUE = IRI(f"{FNML_BASE}functionValue")
FNO_EXECUTES = IRI(f"{FNO_BASE}executes")


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
