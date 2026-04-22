from rdflib import URIRef, Literal as RDFLiteral, BNode as RDFBNode


def term_to_rdflib(term):
    """Convert fog_rml algebra terms to rdflib terms."""
    if term is None:
        return None
    from fog_rml.algebra.Tuple import EPSILON
    from fog_rml.algebra.Terms import IRI, Literal, BlankNode

    if term == EPSILON:
        return None
    if isinstance(term, IRI):
        return URIRef(term.value)
    if isinstance(term, Literal):
        if term.language:
            return RDFLiteral(term.lexical_form, lang=term.language)
        return RDFLiteral(term.lexical_form, datatype=URIRef(term.datatype_iri))
    if isinstance(term, BlankNode):
        return RDFBNode(term.identifier)
    return term

