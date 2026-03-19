"""SERVICE-CALL handler for pyhartig demos

This module provides `execute_query_with_service_call(dataset, query, mapping_dir)` which:
- detects `BIND SERVICE-CALL(?var, "mapping.ttl") AS ?g` patterns
- finds a `VALUES ?var { ... }` clause to enumerate repo identifiers
- for each repo value, resolves candidate mapping files and runs the mapping via `MappingParser`
- loads resulting triples into named graphs (graph URI = http://example.org/<repo> or the provided URI)
- rewrites the query by removing the BIND SERVICE-CALL(...) clauses and inserting `VALUES ?g { <...> }`
- executes the rewritten query on the provided rdflib dataset and returns the result iterator

This is a pragmatic handler (query preprocessor + mapping execution). It demonstrates how
`SERVICE-CALL` can materialize virtual graphs for SPARQL queries using the project's mapping engine.
"""
from pathlib import Path
import re
from typing import List
from rdflib import URIRef, Literal as RDFLiteral, BNode as RDFBNode


def _get_graph(dataset, graph_uri):
    """Return a writable graph for the given dataset and graph IRI.

    Prefers `Dataset.graph(iri)` when available, falls back to
    `ConjunctiveGraph.get_context(iri)` for older usage.
    """
    if hasattr(dataset, "graph"):
        return dataset.graph(graph_uri)
    if hasattr(dataset, "get_context"):
        return dataset.get_context(graph_uri)
    raise AttributeError("dataset has no graph/get_context method")


def _extract_tokens_from_values_clause(query: str, var_name: str) -> List[str]:
    # match VALUES ?var { ... }
    pat = re.compile(r"VALUES\s+\?" + re.escape(var_name) + r"\s*\{([^}]*)\}", re.IGNORECASE | re.DOTALL)
    m = pat.search(query)
    if not m:
        return []
    inner = m.group(1)
    # split by whitespace and filter empties
    toks = [t.strip() for t in re.split(r"\s+", inner) if t.strip()]
    return toks


def _normalize_token_to_name(tok: str) -> str:
    tok = tok.strip()
    # remove angle brackets
    if tok.startswith("<") and tok.endswith(">"):
        uri = tok[1:-1]
        # take last path segment as name
        return uri.rstrip('/').split('/')[-1]
    # remove surrounding quotes
    if tok.startswith('"') and tok.endswith('"'):
        return tok[1:-1]
    # otherwise assume simple local name
    return tok


def _token_to_graph_uri(tok: str) -> URIRef:
    if tok.startswith("<") and tok.endswith(">"):
        return URIRef(tok[1:-1])
    name = _normalize_token_to_name(tok)
    return URIRef(f"http://example.org/{name}")


def _find_mapping_for_repo(mapping_dir: Path, repo_name: str, mapping_filename: str) -> Path:
    # Try a few sensible candidates
    candidates = [
        mapping_dir / mapping_filename,
        mapping_dir / f"{repo_name}_{mapping_filename}",
        mapping_dir / repo_name / mapping_filename,
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


def execute_query_with_service_call(dataset, query: str, mapping_dir: Path):
    """Execute a SPARQL query on `dataset` supporting SERVICE-CALL bindings.

    :param dataset: rdflib ConjunctiveGraph or Dataset
    :param query: SPARQL query string
    :param mapping_dir: base dir to resolve mapping files
    :return: iterator of query results (same as rdflib's QueryResult)
    """
    # find all BIND SERVICE-CALL occurrences
    bind_pat = re.compile(r"BIND\s+SERVICE-CALL\(\s*\?(?P<in>\w+)\s*,\s*\"(?P<mfile>[^\"]+)\"\s*\)\s+AS\s+\?(?P<out>\w+)", re.IGNORECASE)
    matches = list(bind_pat.finditer(query))
    if not matches:
        # nothing to do
        return dataset.query(query)

    # ensure we can import MappingParser lazily
    from pyhartig.mapping.MappingParser import MappingParser
    from pyhartig.utils.term_utils import term_to_rdflib

    new_values_clauses = []
    # mapping from output var -> list of (repo_token, graph_uri_string)
    var_to_pairs = {}
    # mapping from output var -> input var name (e.g. 'repo')
    var_out_to_in = {}
    modified_query = query

    for m in matches:
        var_in = m.group('in')
        mapping_file = m.group('mfile')
        var_out = m.group('out')

        tokens = _extract_tokens_from_values_clause(query, var_in)
        if not tokens:
            # no explicit VALUES for var_in: can't enumerate, skip
            continue

        graph_uris = []
        mapping_entries = []
        for tok in tokens:
            repo_name = _normalize_token_to_name(tok)
            graph_uri = _token_to_graph_uri(tok)

            # find mapping path candidates
            mp = _find_mapping_for_repo(Path(mapping_dir), repo_name, mapping_file)
            if mp is None:
                # no mapping file found; skip mapping execution
                continue

            # run mapping parser and populate the named graph
            parser = MappingParser(str(mp))
            op = parser.parse()
            ctx = _get_graph(dataset, graph_uri)
            for mt in op.execute():
                s = term_to_rdflib(mt.get("subject"))
                p = term_to_rdflib(mt.get("predicate"))
                o = term_to_rdflib(mt.get("object"))
                if s is not None and p is not None and o is not None:
                    ctx.add((s, p, o))

            graph_uris.append(graph_uri)
            # preserve the original token (as appeared in VALUES) and graph uri string
            mapping_entries.append((tok, str(graph_uri)))

        if graph_uris:
            uris_str = " ".join(f"<{u}>" for u in graph_uris)
            new_values_clauses.append((var_out, uris_str))
            var_to_pairs[var_out] = mapping_entries
            var_out_to_in[var_out] = var_in

        # remove the BIND SERVICE-CALL(...) AS ?out text from query
        modified_query = modified_query.replace(m.group(0), "")

    # inject VALUES clauses inside the WHERE { ... } group (after the opening brace)
    if new_values_clauses:
        insert_text = "\n"
        for var_out, uris_str in new_values_clauses:
            insert_text += f"  VALUES ?{var_out} {{ {uris_str} }}\n"

        insert_pos = modified_query.lower().find("where")
        if insert_pos != -1:
            brace_pos = modified_query.find("{", insert_pos)
            if brace_pos != -1:
                insert_at = brace_pos + 1
                modified_query = modified_query[:insert_at] + insert_text + modified_query[insert_at:]
        else:
            # as a fallback, append to start
            modified_query = insert_text + modified_query

        # collapse excessive blank lines
        modified_query = re.sub(r"\n{3,}", "\n\n", modified_query)

    # (Removed temporary union/FILTER-based rewrite logic.)
    # We leave the injected VALUES ?g clauses in place and rely on
    # direct SPARQL execution; the per-graph fallback will run when
    # the SPARQL result is empty or execution fails.

    # Try executing the rewritten query directly. If rdflib's Dataset/SPARQL
    # implementation doesn't return expected results for GRAPH ?var patterns,
    # fall back to evaluating the inner GRAPH pattern per-graph using the
    # concrete named graphs and aggregating results.
    # attempt to execute the rewritten query
    try:
        res = dataset.query(modified_query)
        # try to materialize rows to detect empty result
        try:
            sample_rows = list(res)
        except Exception:
            sample_rows = None
        if sample_rows:
            # return an iterator over the existing rows
            return iter(sample_rows)
    except Exception:
        # parsing/execution error; fall back
        res = None

    # Fallback: evaluate the GRAPH pattern per concrete named graph
    aggregated = []

    # extract SELECT variable order from the original query
    sel_match = re.search(r"SELECT\s+(.*?)\s+WHERE", query, re.IGNORECASE | re.DOTALL)
    if sel_match:
        sel_text = sel_match.group(1)
        sel_vars = re.findall(r"\?(\w+)", sel_text)
    else:
        sel_vars = []

    # collect graph bodies for graph variables from the original query
    body_pat = re.compile(r"GRAPH\s+\?(?P<var>\w+)\s*\{(?P<body>.*?)\}", re.IGNORECASE | re.DOTALL)
    bodies = {}
    for mm in body_pat.finditer(query):
        bodies.setdefault(mm.group('var'), []).append(mm.group('body'))

    # prefix block (everything before SELECT) to preserve PREFIX declarations
    select_pos = query.lower().find('select')
    prefixes = query[:select_pos] if select_pos != -1 else ''

    for var_out, pairs in var_to_pairs.items():
        in_var = var_out_to_in.get(var_out)
        var_bodies = bodies.get(var_out, [])
        for repo_tok, guri in pairs:
            for body in var_bodies:
                # determine variables used in the body
                body_vars = re.findall(r"\?(\w+)", body)
                if not body_vars:
                    continue
                per_select = " ".join("?" + v for v in body_vars)
                per_q = f"{prefixes}SELECT {per_select} WHERE {{ GRAPH <{guri}> {{ {body} }} }}"
                try:
                    per_res = dataset.query(per_q)
                except Exception:
                    continue
                for row in per_res:
                    out_row = []
                    for v in sel_vars:
                        if v == in_var:
                            out_row.append(_token_to_graph_uri(repo_tok))
                        elif v in body_vars:
                            # try name-based access, fall back to positional
                            try:
                                val = row[v]
                            except Exception:
                                try:
                                    idx = body_vars.index(v)
                                    val = row[idx]
                                except Exception:
                                    val = None
                            out_row.append(val)
                        else:
                            out_row.append(None)
                    aggregated.append(tuple(out_row))

    return aggregated
