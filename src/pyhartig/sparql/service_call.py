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
from typing import Dict, List, Optional, Tuple
from rdflib import URIRef, Literal as RDFLiteral, BNode as RDFBNode

from pyhartig.namespaces import EXAMPLE_ORG_BASE


def _get_graph(dataset, graph_uri):
    """Return a writable graph for the given dataset and graph IRI.

    :param dataset: rdflib dataset-like object.
    :param graph_uri: Named graph IRI to resolve.
    :return: Writable graph context for the given IRI.
    """
    if hasattr(dataset, "graph"):
        return dataset.graph(graph_uri)
    if hasattr(dataset, "get_context"):
        return dataset.get_context(graph_uri)
    raise AttributeError("dataset has no graph/get_context method")


def _extract_tokens_from_values_clause(query: str, var_name: str) -> List[str]:
    """Extract the raw tokens declared in a `VALUES ?var { ... }` clause.

    :param query: SPARQL query string.
    :param var_name: Variable name used in the VALUES clause.
    :return: List of raw VALUES tokens.
    """
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
    """Normalize a VALUES token to the repository name used for mapping lookup.

    :param tok: Raw VALUES token.
    :return: Repository name derived from the token.
    """
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
    """Convert a VALUES token to the named-graph URI used for materialization.

    :param tok: Raw VALUES token.
    :return: Named-graph URI for the token.
    """
    if tok.startswith("<") and tok.endswith(">"):
        return URIRef(tok[1:-1])
    name = _normalize_token_to_name(tok)
    return URIRef(f"{EXAMPLE_ORG_BASE}{name}")


def _find_mapping_for_repo(mapping_dir: Path, repo_name: str, mapping_filename: str) -> Path|None:
    """Resolve the most plausible mapping path for one repository token.

    :param mapping_dir: Base directory containing candidate mappings.
    :param repo_name: Repository name extracted from the query token.
    :param mapping_filename: Mapping filename requested by SERVICE-CALL.
    :return: Matching mapping path, or None when none exists.
    """
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


def _find_service_call_matches(query: str) -> List[re.Match[str]]:
    """Find all `BIND SERVICE-CALL(... ) AS ?var` occurrences in a query.

    :param query: SPARQL query string.
    :return: List of regex matches for SERVICE-CALL bindings.
    """
    bind_pat = re.compile(
        r'BIND\s+SERVICE-CALL\(\s*\?(?P<in>\w+)\s*,\s*"(?P<mfile>[^"]+)"\s*\)\s+AS\s+\?(?P<out>\w+)',
        re.IGNORECASE,
    )
    return list(bind_pat.finditer(query))


def _populate_graph_from_mapping(dataset, mapping_path: Path, graph_uri: URIRef) -> None:
    """Execute a mapping and load its triples into the target named graph.

    :param dataset: rdflib dataset-like object.
    :param mapping_path: Mapping file to execute.
    :param graph_uri: Target named-graph URI.
    :return: None
    """
    from pyhartig.mapping.MappingParser import MappingParser
    from pyhartig.utils.term_utils import term_to_rdflib

    parser = MappingParser(str(mapping_path))
    op = parser.parse()
    ctx = _get_graph(dataset, graph_uri)
    for mt in op.execute():
        s = term_to_rdflib(mt.get("subject"))
        p = term_to_rdflib(mt.get("predicate"))
        o = term_to_rdflib(mt.get("object"))
        if s is not None and p is not None and o is not None:
            ctx.add((s, p, o))


def _resolve_service_call_token(
        dataset,
        mapping_dir: Path,
        mapping_file: str,
        tok: str,
) -> Optional[Tuple[str, str]]:
    """Materialize one SERVICE-CALL token and return its token/graph URI pair.

    :param dataset: rdflib dataset-like object.
    :param mapping_dir: Base directory containing candidate mappings.
    :param mapping_file: Mapping filename requested by SERVICE-CALL.
    :param tok: Raw VALUES token to materialize.
    :return: Tuple of original token and graph URI string, or None.
    """
    repo_name = _normalize_token_to_name(tok)
    graph_uri = _token_to_graph_uri(tok)
    mapping_path = _find_mapping_for_repo(mapping_dir, repo_name, mapping_file)
    if mapping_path is None:
        return None

    _populate_graph_from_mapping(dataset, mapping_path, graph_uri)
    return tok, str(graph_uri)


def _process_service_call_match(
        dataset,
        query: str,
        mapping_dir: Path,
        match: re.Match[str],
) -> Optional[Tuple[str, str, List[Tuple[str, str]], str]]:
    """Process one SERVICE-CALL binding and prepare its rewritten VALUES clause.

    :param dataset: rdflib dataset-like object.
    :param query: Original SPARQL query string.
    :param mapping_dir: Base directory containing candidate mappings.
    :param match: Regex match describing one SERVICE-CALL binding.
    :return: Tuple containing output variable, VALUES text, token/graph pairs and input variable, or None.
    """
    var_in = match.group("in")
    mapping_file = match.group("mfile")
    var_out = match.group("out")
    tokens = _extract_tokens_from_values_clause(query, var_in)
    if not tokens:
        return None

    mapping_entries = []
    graph_uris = []
    for tok in tokens:
        resolved = _resolve_service_call_token(dataset, mapping_dir, mapping_file, tok)
        if resolved is None:
            continue
        repo_tok, graph_uri = resolved
        mapping_entries.append((repo_tok, graph_uri))
        graph_uris.append(graph_uri)

    if not graph_uris:
        return None

    uris_str = " ".join(f"<{uri}>" for uri in graph_uris)
    return var_out, uris_str, mapping_entries, var_in


def _prepare_service_call_context(
        dataset,
        query: str,
        mapping_dir: Path,
        matches: List[re.Match[str]],
) -> Tuple[str, List[Tuple[str, str]], Dict[str, List[Tuple[str, str]]], Dict[str, str]]:
    """Build the rewritten query context and graph mappings for all SERVICE-CALL matches.

    :param dataset: rdflib dataset-like object.
    :param query: Original SPARQL query string.
    :param mapping_dir: Base directory containing candidate mappings.
    :param matches: SERVICE-CALL regex matches.
    :return: Modified query, VALUES clauses, output-var graph pairs, and output-to-input variable map.
    """
    new_values_clauses: List[Tuple[str, str]] = []
    var_to_pairs: Dict[str, List[Tuple[str, str]]] = {}
    var_out_to_in: Dict[str, str] = {}
    modified_query = query

    for match in matches:
        processed = _process_service_call_match(dataset, query, mapping_dir, match)
        if processed is not None:
            var_out, uris_str, mapping_entries, var_in = processed
            new_values_clauses.append((var_out, uris_str))
            var_to_pairs[var_out] = mapping_entries
            var_out_to_in[var_out] = var_in
        modified_query = modified_query.replace(match.group(0), "")

    return modified_query, new_values_clauses, var_to_pairs, var_out_to_in


def _build_values_insert_text(new_values_clauses: List[Tuple[str, str]]) -> str:
    """Build the text block injected into the query for rewritten VALUES clauses.

    :param new_values_clauses: Rewritten VALUES clauses keyed by output variable.
    :return: Text block to inject in the SPARQL query.
    """
    insert_text = "\n"
    for var_out, uris_str in new_values_clauses:
        insert_text += f"  VALUES ?{var_out} {{ {uris_str} }}\n"
    return insert_text


def _inject_values_clauses(modified_query: str, new_values_clauses: List[Tuple[str, str]]) -> str:
    """Inject rewritten VALUES clauses into the query body.

    :param modified_query: Query string with SERVICE-CALL bindings removed.
    :param new_values_clauses: Rewritten VALUES clauses keyed by output variable.
    :return: Query string with injected VALUES clauses.
    """
    if not new_values_clauses:
        return modified_query

    insert_text = _build_values_insert_text(new_values_clauses)
    insert_pos = modified_query.lower().find("where")
    if insert_pos != -1:
        brace_pos = modified_query.find("{", insert_pos)
        if brace_pos != -1:
            insert_at = brace_pos + 1
            modified_query = modified_query[:insert_at] + insert_text + modified_query[insert_at:]
    else:
        modified_query = insert_text + modified_query

    return re.sub(r"\n{3,}", "\n\n", modified_query)


def _execute_materialized_query(dataset, query: str) -> List[object]:
    """Execute a query and always return a materialized list of rows.

    :param dataset: rdflib dataset-like object.
    :param query: SPARQL query string.
    :return: Materialized query result rows.
    """
    try:
        return list(dataset.query(query))
    except Exception:
        return []


def _extract_select_vars(query: str) -> List[str]:
    """Extract SELECT variable names in output order from a query.

    :param query: SPARQL query string.
    :return: Ordered list of SELECT variable names.
    """
    select_part = _extract_select_clause(query)
    if select_part is None:
        return []
    return re.findall(r"\?(\w+)", select_part)


def _extract_select_clause(query: str) -> Optional[str]:
    """Extract the raw SELECT clause located before WHERE.

    :param query: SPARQL query string.
    :return: SELECT clause content, or None when it cannot be isolated.
    """
    upper_query = query.upper()
    select_pos = upper_query.find("SELECT")
    if select_pos == -1:
        return None

    where_pos = upper_query.find("WHERE", select_pos + len("SELECT"))
    if where_pos == -1:
        return None

    return query[select_pos + len("SELECT"):where_pos]


def _extract_graph_bodies(query: str) -> Dict[str, List[str]]:
    """Collect `GRAPH ?var { ... }` bodies grouped by graph variable name.

    :param query: SPARQL query string.
    :return: Mapping of graph variables to their graph-body snippets.
    """
    body_pat = re.compile(r"GRAPH\s+\?(?P<var>\w+)\s*\{(?P<body>.*?)\}", re.IGNORECASE | re.DOTALL)
    bodies: Dict[str, List[str]] = {}
    for match in body_pat.finditer(query):
        bodies.setdefault(match.group("var"), []).append(match.group("body"))
    return bodies


def _extract_prefix_block(query: str) -> str:
    """Return the prefix declarations and pre-SELECT header of a query.

    :param query: SPARQL query string.
    :return: Prefix and header block preceding SELECT.
    """
    select_pos = query.lower().find("select")
    return query[:select_pos] if select_pos != -1 else ""


def _build_per_graph_query(prefixes: str, graph_uri: str, body: str, body_vars: List[str]) -> str:
    """Build a concrete per-graph fallback query for one GRAPH body.

    :param prefixes: Prefix/header block to preserve.
    :param graph_uri: Concrete named-graph URI.
    :param body: GRAPH body to replay.
    :param body_vars: Variables referenced in the body.
    :return: Concrete SPARQL query string for the graph.
    """
    per_select = " ".join("?" + v for v in body_vars)
    return f"{prefixes}SELECT {per_select} WHERE {{ GRAPH <{graph_uri}> {{ {body} }} }}"


def _get_row_value(row, var_name: str, body_vars: List[str]):
    """Read a value from a rdflib result row by name, then by positional fallback.

    :param row: rdflib query result row.
    :param var_name: Variable name to extract.
    :param body_vars: Variable order used by the per-graph query.
    :return: Matching row value, or None.
    """
    try:
        return row[var_name]
    except Exception:
        try:
            return row[body_vars.index(var_name)]
        except Exception:
            return None


def _build_aggregated_row(row, sel_vars: List[str], in_var: Optional[str], repo_tok: str, body_vars: List[str]):
    """Build one output row for the per-graph fallback aggregation path.

    :param row: rdflib query result row.
    :param sel_vars: Output SELECT variable order.
    :param in_var: Original SERVICE-CALL input variable name.
    :param repo_tok: Original repository token from VALUES.
    :param body_vars: Variables referenced in the per-graph body.
    :return: Aggregated output row tuple.
    """
    out_row = []
    for var_name in sel_vars:
        if var_name == in_var:
            out_row.append(_token_to_graph_uri(repo_tok))
        elif var_name in body_vars:
            out_row.append(_get_row_value(row, var_name, body_vars))
        else:
            out_row.append(None)
    return tuple(out_row)


def _aggregate_graph_body_results(dataset, per_query: str, sel_vars: List[str], in_var: Optional[str], repo_tok: str,
                                  body_vars: List[str]) -> List[tuple]:
    """Execute one concrete per-graph query and convert its rows to the outer shape.

    :param dataset: rdflib dataset-like object.
    :param per_query: Concrete per-graph SPARQL query.
    :param sel_vars: Output SELECT variable order.
    :param in_var: Original SERVICE-CALL input variable name.
    :param repo_tok: Original repository token from VALUES.
    :param body_vars: Variables referenced in the per-graph body.
    :return: Aggregated result rows for the concrete graph query.
    """
    try:
        per_res = dataset.query(per_query)
    except Exception:
        return []

    aggregated_rows = []
    for row in per_res:
        aggregated_rows.append(_build_aggregated_row(row, sel_vars, in_var, repo_tok, body_vars))
    return aggregated_rows


def _aggregate_service_call_results(
        dataset,
        query: str,
        var_to_pairs: Dict[str, List[Tuple[str, str]]],
        var_out_to_in: Dict[str, str],
) -> List[tuple]:
    """Aggregate fallback results by replaying GRAPH bodies on concrete named graphs.

    :param dataset: rdflib dataset-like object.
    :param query: Original SPARQL query string.
    :param var_to_pairs: Mapping of output variables to token/graph URI pairs.
    :param var_out_to_in: Mapping of output variables to original input variables.
    :return: Aggregated fallback rows.
    """
    aggregated: List[tuple] = []
    sel_vars = _extract_select_vars(query)
    bodies = _extract_graph_bodies(query)
    prefixes = _extract_prefix_block(query)

    for var_out, pairs in var_to_pairs.items():
        in_var = var_out_to_in.get(var_out)
        for repo_tok, graph_uri in pairs:
            for body in bodies.get(var_out, []):
                body_vars = re.findall(r"\?(\w+)", body)
                if not body_vars:
                    continue
                per_query = _build_per_graph_query(prefixes, graph_uri, body, body_vars)
                aggregated.extend(
                    _aggregate_graph_body_results(dataset, per_query, sel_vars, in_var, repo_tok, body_vars)
                )

    return aggregated


def execute_query_with_service_call(dataset, query: str, mapping_dir: Path) -> List[object]:
    """Execute a SPARQL query on `dataset` supporting SERVICE-CALL bindings.

    :param dataset: rdflib ConjunctiveGraph or Dataset
    :param query: SPARQL query string
    :param mapping_dir: base dir to resolve mapping files
    :return: Materialized query rows.
    """
    matches = _find_service_call_matches(query)
    if not matches:
        return _execute_materialized_query(dataset, query)

    modified_query, new_values_clauses, var_to_pairs, var_out_to_in = _prepare_service_call_context(
        dataset,
        query,
        Path(mapping_dir),
        matches,
    )
    modified_query = _inject_values_clauses(modified_query, new_values_clauses)

    direct_rows = _execute_materialized_query(dataset, modified_query)
    if direct_rows:
        return direct_rows

    return _aggregate_service_call_results(dataset, query, var_to_pairs, var_out_to_in)
