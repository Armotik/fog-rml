"""Demo: emulate SERVICE-CALL behavior by loading per-repo named graphs

Run:
    python pyhartig/examples/multi_repo_service_demo.py

This script loads TTL files for repos r1,r2,r3 into named graphs and
executes a SPARQL query that selects triples from the graph bound to
each repo. It demonstrates the equivalent of binding a graph via
`SERVICE-CALL(... ) as ?g` by using named graphs in an rdflib dataset.
"""
from pathlib import Path
import sys
from rdflib import Dataset, Namespace, URIRef, Graph as RDFGraph, Literal as RDFLiteral, BNode as RDFBNode

EX = Namespace("http://example.org/")


def _ensure_src_on_path():
    # Ensure `src/` is on sys.path so imports like `pyhartig.mapping` work
    here = Path(__file__).resolve()
    project_root = here.parent.parent
    src_dir = project_root / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


def term_to_rdflib(term):
    """Convert pyhartig algebra terms to rdflib terms."""
    if term is None:
        return None
    from pyhartig.algebra.Tuple import EPSILON
    from pyhartig.algebra.Terms import IRI, Literal, BlankNode

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
    # Already an rdflib term
    return term


def main():
    _ensure_src_on_path()

    # local imports after ensuring src is on path
    from pyhartig.mapping.MappingParser import MappingParser

    base = Path(__file__).parent / "data"
    cg = Dataset()

    repos = ["r1", "r2", "r3"]
    graph_uris = []

    # For each repo: if a mapping file exists use MappingParser to run the mapping
    for r in repos:
        g_uri = URIRef(f"http://example.org/{r}")
        graph_uris.append(g_uri)
        ctx = cg.graph(g_uri)

        mapping_path = base / f"{r}_mapping.ttl"
        ttl_path = base / f"{r}.ttl"

        if mapping_path.exists():
            print(f"Running mapping for {r} from {mapping_path}")
            parser = MappingParser(str(mapping_path))
            op = parser.parse()
            for mt in op.execute():
                s = term_to_rdflib(mt.get("subject"))
                p = term_to_rdflib(mt.get("predicate"))
                o = term_to_rdflib(mt.get("object"))
                if s is not None and p is not None and o is not None:
                    ctx.add((s, p, o))
        elif ttl_path.exists():
            ctx.parse(str(ttl_path), format="turtle")
        else:
            # fallback sample
            ctx.add((EX[f"issue_{r}_1"], EX.issue, EX[f"node_{r}_1"]))
            ctx.add((EX[f"node_{r}_1"], EX.title, RDFLiteral(f"Title {r} 1")))

    # construct VALUES clause listing the graph URIs (for display only)
    values = " ".join(f"<{g}>" for g in graph_uris)

    query = f"""
    PREFIX ex: <http://example.org/>
    SELECT ?repo ?x ?y ?title WHERE {{
        VALUES ?repo {{ {values} }}
        BIND SERVICE-CALL(?repo, "mapping.ttl") AS ?g
        GRAPH ?g {{
            ?x ex:issue ?y .
            ?y ex:title ?title .
        }}
    }}
    """

    print("Running query with SERVICE-CALL support:\n", query)
    # use the SERVICE-CALL handler to execute the query -- it will run mappings
    from pyhartig.sparql.service_call import execute_query_with_service_call

    # We executed mappings into the dataset above. Print resulting quads
    # (graph, subject, predicate, object) so callers see the materialized data.
    print('\nResulting quads:')
    try:
        for g, s, p, o in cg.quads((None, None, None, None)):
            g_label = str(g) if g is not None else 'urn:x-rdflib:default'
            try:
                s_txt = s.n3()
                p_txt = p.n3()
                o_txt = o.n3()
            except Exception:
                s_txt = str(s)
                p_txt = str(p)
                o_txt = str(o)
            print(f"Graph: {g_label}  |  {s_txt} {p_txt} {o_txt}")
    except Exception:
        # Fallback for older rdflib versions: iterate graphs and triples
        for g in cg.graphs():
            g_label = str(g.identifier)
            for s, p, o in g:
                try:
                    s_txt = s.n3()
                    p_txt = p.n3()
                    o_txt = o.n3()
                except Exception:
                    s_txt = str(s)
                    p_txt = str(p)
                    o_txt = str(o)
                print(f"Graph: {g_label}  |  {s_txt} {p_txt} {o_txt}")


if __name__ == '__main__':
    main()
