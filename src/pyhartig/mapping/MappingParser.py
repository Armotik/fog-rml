import logging
from rdflib import Graph, URIRef, Namespace, Node, BNode, Literal as RDFLiteral
from typing import List, Dict, Any
from pathlib import Path

from pyhartig.operators.Operator import Operator
from pyhartig.operators.ExtendOperator import ExtendOperator
from pyhartig.operators.UnionOperator import UnionOperator
from pyhartig.operators.EquiJoinOperator import EquiJoinOperator
from pyhartig.operators.ProjectOperator import ProjectOperator

from pyhartig.expressions.Expression import Expression
from pyhartig.expressions.Constant import Constant
from pyhartig.expressions.Reference import Reference
from pyhartig.expressions.FunctionCall import FunctionCall
from pyhartig.algebra.Terms import IRI as AlgebraIRI, Literal as AlgebraLiteral
from pyhartig.namespaces import RR_BASE, RML_BASE, QL_BASE, RDF_BASE, XSD_BASE
from pyhartig.operators.SourceFactory import SourceFactory
from pyhartig.functions.builtins import to_iri, to_literal, concat, to_bnode, to_literal_lang, percent_encode_component

RR = Namespace(RR_BASE)
RML = Namespace(RML_BASE)
QL = Namespace(QL_BASE)
RDF = Namespace(RDF_BASE)
XSD = Namespace(XSD_BASE)

logger = logging.getLogger(__name__)


class MappingParser:
    """
    Parses a valid RML mapping file (RDF/Turtle) and translates it into an algebraic operator pipeline.
    """

    def __init__(self, rml_file_path: str):
        """
        Initializes the MappingParser with the given RML file path.
        :param rml_file_path:  Path to the RML mapping file.
        """
        self.rml_file_path = rml_file_path
        self.mapping_dir = Path(rml_file_path).resolve().parent
        self.graph = Graph()
        self._q4_nojoin_parent = {}
        self._q5_tm_clones = {}
        self._q5_tm_runtime_nodes = {}
        # Attempt to extract @base from the mapping file so we can resolve
        # relative reference values to IRIs when needed.
        self.base_iri = None
        try:
            import re
            with open(rml_file_path, 'r', encoding='utf-8') as fh:
                txt = fh.read(1024)
            m = re.search(r"@base\s+<([^>]+)>", txt)
            if m:
                self.base_iri = m.group(1)
        except Exception:
            self.base_iri = None

    def parse(self) -> Operator:
        """
        Parses an RML mapping file and translates it into an algebraic plan
        :return: Operator representing the entire mapping.
        """
        # [INFO] Log the start of parsing
        logger.info(f"Parsing RML mapping file: {self.rml_file_path}")

        # Load the RML mapping file into an RDF graph
        # Fail fast if the mapping file does not exist to avoid silent empty results
        from pathlib import Path as _Path
        if not _Path(self.rml_file_path).exists():
            logger.error(f"RML mapping file not found: {self.rml_file_path}")
            raise FileNotFoundError(f"RML mapping file not found: {self.rml_file_path}")

        try:
            self.graph.parse(self.rml_file_path, format="turtle")
        except Exception as e:
            logger.error(f"Failed to parse RML file {self.rml_file_path}: {e}")
            # Try alternative RDF formats as a fallback
            for fmt in ("n3", "trig", "xml"):
                try:
                    logger.info(f"Attempting parse with format='{fmt}'")
                    self.graph.parse(self.rml_file_path, format=fmt)
                    logger.info(f"Parsed RML file with format '{fmt}'")
                    break
                except Exception:
                    continue
            else:
                # Try to sanitize unescaped backslashes inside quoted strings (Windows paths)
                try:
                    import re
                    with open(self.rml_file_path, 'r', encoding='utf-8') as f:
                        content = f.read()

                    def _escape_backslashes_in_str(m):
                        inner = m.group(1)
                        if "\\\\" in inner:
                            return '"' + inner + '"'
                        return '"' + inner.replace('\\', '\\\\') + '"'

                    sanitized = re.sub(r'"([^"\\]*(?:\\.[^"\\]*)*)"', _escape_backslashes_in_str, content)

                    # Try parsing the sanitized content directly
                    logger.info("Attempting to parse sanitized RML content (escaped backslashes in strings)")
                    self.graph.parse(data=sanitized, format='turtle')
                    logger.info("Parsed RML file from sanitized content")
                except Exception as e3:
                    # Provide a small sample of the file for diagnostics
                    try:
                        with open(self.rml_file_path, 'r', encoding='utf-8') as f:
                            sample = f.read(1000)
                        logger.debug(f"RML file sample (first 1000 chars): {repr(sample)}")
                    except Exception as e2:
                        logger.debug(f"Could not read file for diagnostic: {e2}")
                    # Re-raise the original parsing error so caller can handle it
                    raise

        # [DEBUG] Log the number of triples loaded
        logger.debug(f"RDF Graph loaded ({len(self.graph)} triples). Normalizing...")

        # Early validation: detect invalid mappings before normalization
        # (e.g., multiple rr:subjectMap per TriplesMap which should raise InvalidRulesError)
        triples_maps_raw = set(self.graph.subjects(RDF.type, RR.TriplesMap))
        triples_maps_raw.update(self.graph.subjects(RML.logicalSource, None))
        for tm_raw in triples_maps_raw:
            sm_count = len(list(self.graph.objects(tm_raw, RR.subjectMap)))
            if sm_count > 1:
                raise ValueError(f"InvalidRulesError: TriplesMap {tm_raw} contains multiple rr:subjectMap definitions")

        # Normalize the RML graph
        self._normalize_graph()
        logger.debug("RML Graph normalized.")

        # Initialize an empty list to hold the operators for each Triples Map
        S: List[Operator] = []

        # We find all resources typed as rr:TriplesMap or having a logicalSource
        triples_maps = set(self.graph.subjects(RDF.type, RR.TriplesMap))
        triples_maps.update(self.graph.subjects(RML.logicalSource, None))
        triples_maps.update(self.graph.subjects(RR.logicalSource, None))
        triples_maps.update(self.graph.subjects(RR.logicalTable, None))

        # [INFO] Log the number of Triples Maps found
        logger.info(f"Found {len(triples_maps)} TriplesMaps to process.")

        for tm in triples_maps:
            # [DEBUG] Log the current Triples Map being processed
            logger.debug(f"Processing TriplesMap: {tm}")

            # Line 4: Let LS be the logical source of TM
            ls_node = self.graph.value(tm, RML.logicalSource) or self.graph.value(tm, RR.logicalSource)

            # Compatibility: support rr:logicalTable by mapping it to an internal
            # logical source node backed by the first available D2RQ database node.
            if not ls_node:
                logical_table = self.graph.value(tm, RR.logicalTable)
                if logical_table:
                    d2rq_database_type = URIRef("http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#Database")
                    db_source = next(self.graph.subjects(RDF.type, d2rq_database_type), None)
                    table_name = self.graph.value(logical_table, RR.tableName)
                    sql_query = self.graph.value(logical_table, RR.sqlQuery)
                    sql_version = self.graph.value(logical_table, RR.sqlVersion)

                    if db_source and (table_name is not None or sql_query is not None):
                        ls_node = BNode()
                        self.graph.add((ls_node, RML.source, db_source))
                        if table_name is not None:
                            self.graph.add((ls_node, RR.tableName, table_name))
                        if sql_query is not None:
                            self.graph.add((ls_node, RML.query, sql_query))
                        if sql_version is not None:
                            self.graph.add((ls_node, RR.sqlVersion, sql_version))

            if not ls_node:
                logger.warning(f"Skipping TriplesMap {tm}: No logical source found.")
                continue

            # Algorithm 2 Call: P := EXTRACTQUERIES(TM)
            P = self._extract_queries(tm)

            try:
                E_src = SourceFactory.create_source_operator(
                    graph=self.graph,
                    logical_source_node=ls_node,
                    mapping_dir=self.mapping_dir,
                    attribute_mappings=P
                )
            except ValueError as e:
                logger.error(f"Failed to create source for {tm}: {e}")
                continue

            tm_branches = []

            # Line 6: Let SM be the subject map of TM
            # Validate there is at most one subjectMap per TriplesMap according to R2RML/RML rules
            sm_objs = list(self.graph.objects(tm, RR.subjectMap))
            if len(sm_objs) > 1:
                # This mapping is invalid (multiple subjectMap definitions); fail fast so tests expecting
                # InvalidRulesError are handled as errors rather than producing possibly inconsistent triples.
                raise ValueError(f"InvalidRulesError: TriplesMap {tm} contains multiple rr:subjectMap definitions")
            sm = sm_objs[0] if sm_objs else None

            # Line 7: phi_sbj := CREATEEXTEXPR(SM)
            if sm:
                phi_sbj = self._create_ext_expr(sm, default_term_type="IRI")
                # Line 8: E := Extend(E_src, "subject", phi_sbj)
                E_base = ExtendOperator(E_src, "subject", phi_sbj)
            else:
                # Treat missing subjectMap as an invalid mapping (fail fast)
                raise ValueError(f"InvalidRulesError: TriplesMap {tm} contains no rr:subjectMap definition")

            # Line 9: foreach predicate-object map POM in TM do
            poms = list(self.graph.objects(tm, RR.predicateObjectMap))

            # [DEBUG] Log number of PredicateObjectMaps found
            logger.debug(f"Found {len(poms)} PredicateObjectMaps in TriplesMap {tm}.")

            for pom in poms:
                E = E_base

                # Line 22: Let PM be the predicate map and OM be the object map of POM
                pm = self.graph.value(pom, RR.predicateMap)
                om = self.graph.value(pom, RR.objectMap)

                # Line 24: phi_pred := CREATEEXTEXPR(PM)
                phi_pred = self._create_ext_expr(pm, default_term_type="IRI")

                # Detect referencing object maps (joins)
                parent_tm = None
                try:
                    parent_tm = self.graph.value(om, RR.parentTriplesMap)
                except Exception:
                    parent_tm = None

                # Pre-extract join attributes so we can use them for resolution and
                # to augment attribute mappings before creating source operators.
                child_attrs, parent_attrs, child_queries, parent_queries = self._extract_join_attributes(om)
                nojoin_parent_fallback = False

                if (not parent_tm) and (om in self._q4_nojoin_parent):
                    parent_tm = self._q4_nojoin_parent.get(om)
                    nojoin_parent_fallback = True

                if parent_tm:
                    # Build parent source operator
                    parent_ls = self.graph.value(parent_tm, RML.logicalSource) or self.graph.value(parent_tm, RR.logicalSource)
                    if not parent_ls:
                        clone_rows = self._q5_tm_clones.get(parent_tm)
                        if clone_rows:
                            runtime_tm = self._q5_tm_runtime_nodes.get(parent_tm)
                            if runtime_tm is None:
                                runtime_tm = BNode()
                                for ls_c, sm_c, pom_c in clone_rows:
                                    self.graph.add((runtime_tm, RML.logicalSource, ls_c))
                                    self.graph.add((runtime_tm, RR.subjectMap, sm_c))
                                    self.graph.add((runtime_tm, RR.predicateObjectMap, pom_c))
                                self._q5_tm_runtime_nodes[parent_tm] = runtime_tm
                            parent_tm = runtime_tm
                            parent_ls = self.graph.value(parent_tm, RML.logicalSource) or self.graph.value(parent_tm, RR.logicalSource)

                    if not parent_ls:
                        # Try to log some diagnostics to help tests/debugging
                        try:
                            props = list(self.graph.predicate_objects(parent_tm))
                            logger.debug(f"Parent TriplesMap {parent_tm} predicates: {props}")
                        except Exception:
                            pass

                        # Try to locate the actual TriplesMap matching the join parent's reference
                        # by scanning existing TriplesMaps for a subjectMap that uses the
                        # same rml:reference or contains the same template variable.
                        found = False
                        if parent_attrs:
                            # Prefer candidate TriplesMaps whose logical source differs from
                            # the child's logical source (e.g., sport.csv vs student.csv).
                            import re
                            child_source_literal = None
                            try:
                                child_ls_node = ls_node
                                child_source_literal = str(self.graph.value(child_ls_node, RML.source) or self.graph.value(child_ls_node, RR.source) or "")
                            except Exception:
                                child_source_literal = None

                            preferred = []
                            fallback = []

                            for cand in triples_maps:
                                sm_cand = self.graph.value(cand, RR.subjectMap)
                                if not sm_cand:
                                    continue

                                cand_ls_node = self.graph.value(cand, RML.logicalSource) or self.graph.value(cand, RR.logicalSource)
                                cand_source = None
                                try:
                                    cand_source = str(self.graph.value(cand_ls_node, RML.source) or self.graph.value(cand_ls_node, RR.source) or "")
                                except Exception:
                                    cand_source = None

                                # check rml:reference
                                cand_ref = self.graph.value(sm_cand, RML.reference)
                                matched = False
                                if cand_ref:
                                    m = re.search(r"[A-Za-z_][A-Za-z0-9_\-]*", str(cand_ref))
                                    cand_name = m.group(0) if m else str(cand_ref)
                                    if any(cand_name == pa for pa in parent_attrs):
                                        matched = True

                                # check template variables inside rr:template
                                if not matched:
                                    cand_tmpl = self.graph.value(sm_cand, RR.template)
                                    if cand_tmpl:
                                        tmpl_str = str(cand_tmpl)
                                        for pa in parent_attrs:
                                            if pa in tmpl_str or f"{{{pa}}}" in tmpl_str:
                                                matched = True
                                                break

                                if not matched:
                                    continue

                                # Prefer candidates with a different source file than the child
                                if child_source_literal and cand_source and cand_source != child_source_literal:
                                    preferred.append((cand, cand_ls_node))
                                else:
                                    fallback.append((cand, cand_ls_node))

                            pick = None
                            if preferred:
                                pick = preferred[0]
                            elif fallback:
                                pick = fallback[0]

                            if pick:
                                parent_tm = pick[0]
                                parent_ls = self.graph.value(parent_tm, RML.logicalSource) or self.graph.value(parent_tm, RR.logicalSource)
                                found = True
                                logger.debug(f"Resolved parent TriplesMap by candidate match: {parent_tm}")
                            else:
                                found = False

                        if not found and (parent_ls is None):
                            logger.error(f"Parent TriplesMap {parent_tm} has no logicalSource; skipping join.")
                            continue

                    # Extract parent queries and ensure parent attribute mappings include the join parent attributes
                    P_parent = self._extract_queries(parent_tm)
                    rename_map = {}
                    try:
                        for name, q in zip(parent_attrs, parent_queries):
                            if not name:
                                continue
                            # Determine a sensible extraction query for the parent attribute.
                            # If the parent attribute provides an explicit query use it,
                            # otherwise assume a simple attribute/column name.
                            effective_q = q if q else name
                            # If this parent attribute name would collide with
                            # attributes produced by the child (P), synthesize
                            # a namespaced parent key and remember the mapping so
                            # we can update any subject expression references.
                            if name in P:
                                new_name = f"parent_{name}"
                                P_parent[new_name] = effective_q
                                rename_map[name] = new_name
                                # Avoid exposing the original parent attribute
                                # name on the parent source to prevent merge
                                # conflicts during tuple merging.
                                if name in P_parent:
                                    try:
                                        del P_parent[name]
                                    except Exception:
                                        pass
                            else:
                                # avoid overwriting existing parent mappings
                                if name not in P_parent:
                                    P_parent[name] = effective_q
                    except Exception:
                        pass

                    # Also rename any other parent attributes that collide with
                    # child attributes to avoid tuple-merge conflicts after join
                    # (e.g., both sides exposing ID.value with different values).
                    try:
                        for k in list(P_parent.keys()):
                            if k in P and not str(k).startswith("parent_"):
                                new_name = f"parent_{k}"
                                P_parent[new_name] = P_parent[k]
                                del P_parent[k]
                                rename_map[k] = new_name
                    except Exception:
                        pass

                    try:
                        E_parent_src = SourceFactory.create_source_operator(
                            graph=self.graph,
                            logical_source_node=parent_ls,
                            mapping_dir=self.mapping_dir,
                            attribute_mappings=P_parent
                        )
                    except ValueError as e:
                        logger.error(f"Failed to create parent source for {parent_tm}: {e}")
                        continue

                    # Prepare parent subject expression (do not extend parent source yet)
                    parent_sm = self.graph.value(parent_tm, RR.subjectMap)
                    if parent_sm:
                        phi_parent_sbj = self._create_ext_expr(parent_sm, default_term_type="IRI")
                        # If we renamed parent attributes, update any Reference
                        # expressions inside the subject expression to point to
                        # the namespaced parent attribute keys.
                        if rename_map and phi_parent_sbj is not None:
                            def _rename_refs(expr):
                                # Mutate Reference nodes in-place
                                from pyhartig.expressions.Reference import Reference
                                from pyhartig.expressions.FunctionCall import FunctionCall
                                if isinstance(expr, Reference):
                                    if expr.attribute_name in rename_map:
                                        expr.attribute_name = rename_map[expr.attribute_name]
                                elif isinstance(expr, FunctionCall):
                                    for arg in expr.arguments:
                                        _rename_refs(arg)
                                # Constants and others have no nested refs
                            try:
                                _rename_refs(phi_parent_sbj)
                            except Exception:
                                pass
                    else:
                        phi_parent_sbj = None

                    if len(child_attrs) != len(parent_attrs):
                        logger.error(f"Invalid join conditions in referencing object map {om}; skipping.")
                        continue

                    # Ensure the child attributes used in the join are available in
                    # a dedicated child-source mapping for this join branch. E_src was
                    # created before join conditions were expanded, so build a fresh
                    # source operator that includes join attributes.
                    P_child_join = dict(P)
                    if nojoin_parent_fallback and not child_attrs:
                        import re
                        om_ref = self.graph.value(om, RML.reference)
                        if om_ref:
                            m = re.search(r"[A-Za-z_][A-Za-z0-9_\-.]*", str(om_ref))
                            if m and m.group(0) in P_child_join:
                                del P_child_join[m.group(0)]
                        om_template = self.graph.value(om, RR.template)
                        if om_template:
                            for var in re.findall(r'(?<!\{)\{([^{}]+)\}(?!\})', str(om_template)):
                                if var in P_child_join:
                                    del P_child_join[var]
                    for name, q in zip(child_attrs, child_queries):
                        if not name:
                            continue
                        if name not in P_child_join:
                            if q:
                                P_child_join[name] = q
                            else:
                                P_child_join[name] = name

                    try:
                        E_child_src = SourceFactory.create_source_operator(
                            graph=self.graph,
                            logical_source_node=ls_node,
                            mapping_dir=self.mapping_dir,
                            attribute_mappings=P_child_join
                        )
                    except ValueError as e:
                        logger.error(f"Failed to create child source for join in {tm}: {e}")
                        continue

                    # Ensure parent attribute mappings include parent join attributes (already added to P_parent above)

                    # Create EquiJoin between the raw source operators (before subject extension)
                    try:
                        # If any parent attributes were renamed to avoid
                        # collisions, update the parent_attrs list to refer
                        # to the renamed keys so the EquiJoin probes the
                        # correct attributes on the right-hand tuples.
                        effective_parent_attrs = [rename_map.get(n, n) for n in parent_attrs]
                        join_op = EquiJoinOperator(E_child_src, E_parent_src, child_attrs, effective_parent_attrs)
                    except Exception as e:
                        logger.error(f"Failed to create EquiJoinOperator for {tm} <> {parent_tm}: {e}")
                        continue

                    # After joining, first extend to create the child's subject attribute
                    E_after_child = ExtendOperator(join_op, "subject", phi_sbj)

                    # Then extend to create the parent's subject under a distinct name to avoid attribute collision
                    if phi_parent_sbj is not None:
                        E_after_parent = ExtendOperator(E_after_child, "parent_subject", phi_parent_sbj)
                    else:
                        E_after_parent = E_after_child

                    # Finally, set predicate and set object to parent's subject
                    E = ExtendOperator(E_after_parent, "predicate", phi_pred)
                    E = ExtendOperator(E, "object", Reference("parent_subject"))

                else:
                    # Line 25: phi_obj := CREATEEXTEXPR(OM)
                    # Heuristic: if the objectMap contains a template that looks
                    # like an IRI (e.g., starts with http(s) or contains a scheme
                    # before template vars), prefer IRI as the default term type
                    om_template = self.graph.value(om, RR.template)
                    if om_template:
                        tstr = str(om_template)
                        prefix = tstr.split('{', 1)[0]
                        if prefix.startswith('http://') or prefix.startswith('https://') or (':' in prefix):
                            phi_obj = self._create_ext_expr(om, default_term_type="IRI")
                        else:
                            phi_obj = self._create_ext_expr(om, default_term_type="Literal")
                    else:
                        phi_obj = self._create_ext_expr(om, default_term_type="Literal")

                    # Line 26: E := Extend(E, "predicate", phi_pred)
                    E = ExtendOperator(E, "predicate", phi_pred)

                    # Line 27: E := Extend(E, "object", phi_obj)
                    E = ExtendOperator(E, "object", phi_obj)

                # Named-graph support: prefer POM-level graphMap, fallback to subject-level graphMap
                gm_node = self.graph.value(pom, RR.graphMap) or (sm and self.graph.value(sm, RR.graphMap))
                if gm_node:
                    try:
                        phi_graph = self._create_ext_expr(gm_node, default_term_type="IRI")
                        E = ExtendOperator(E, "graph", phi_graph)
                    except Exception as e:
                        logger.error(f"Failed to create graph extension for POM {pom} in TM {tm}: {e}")
                else:
                    E = ExtendOperator(E, "graph", Constant(AlgebraIRI(str(RR.defaultGraph))))

                E = ProjectOperator(E, {"subject", "predicate", "object", "graph"})

                tm_branches.append(E)

            # Line 28: S := S U {E}
            if tm_branches:
                S.extend(tm_branches)
            else:
                logger.debug(f"No branches generated for {tm} (maybe no POMs?).")

        # Line 29: return Union(S)
        if not S:
            logger.error("Parsing failed: No operators generated.")
            raise ValueError("No valid mappings generated from RML file.")

        logger.info(f"Pipeline construction complete. Total Union branches: {len(S)}")

        if len(S) == 1:
            return S[0]

        return UnionOperator(S)

    def _normalize_graph(self):
        """
        Normalizes the RML graph
        :return: None
        """
        logger.debug("Starting graph normalization (Applying rewriting rules)...")

        prefixes = f"""
                        PREFIX rr: <{RR_BASE}>
                        PREFIX rml: <{RML_BASE}>
                        PREFIX rdf: <{RDF_BASE}>
                        """

        # Query 1: Normalization step 1 (Expand shortcuts for class IRIs)
        q1 = prefixes + """
                DELETE { ?sm rr:class ?class }
                INSERT { 
                    ?tm rr:predicateObjectMap [
                        rr:predicateMap [ rr:termType rr:IRI ; rr:constant rdf:type ] ;
                        rr:objectMap    [ rr:termType rr:IRI ; rr:constant ?class ]
                    ]
                }
                WHERE { 
                    ?tm rr:subjectMap ?sm .
                    ?sm rr:class ?class .
                }
                """
        self.graph.update(q1)

        # Query 3: Normalization step 2 (Expand shortcuts for constant-valued term maps)
        q3 = prefixes + """
                DELETE { ?tm rr:subject ?sm_constant . ?pompm rr:predicate ?pm_constant .
                         ?termMap rr:graph ?gm_constant . ?pomom rr:object ?om_constant . }
                INSERT { ?tm rr:subjectMap [ rr:constant ?sm_constant ].
                         ?pompm rr:predicateMap [ rr:constant ?pm_constant ].
                         ?pomom rr:objectMap [ rr:constant ?om_constant ].
                         ?termMap rr:graphMap [ rr:constant ?gm_constant ]. }
                WHERE { { ?tm rr:subject ?sm_constant }
                        UNION { ?pompm rr:predicate ?pm_constant }
                        UNION { ?pomom rr:object ?om_constant }
                        UNION { ?termMap rr:graph ?gm_constant } }
                """
        self.graph.update(q3)

        # Query 2: Normalization step 3 (Duplicate multi-predicate-object maps into singletons)
        q2 = prefixes + """
                DELETE { ?tm rr:predicateObjectMap ?pom.
                         ?pom rr:predicateMap ?pm ;
                              rr:objectMap ?om ; 
                              rr:graphMap ?gm }
                INSERT { ?tm rr:predicateObjectMap [
                         rr:predicateMap ?pm ;
                         rr:objectMap ?om ;
                         rr:graphMap ?gm ] }
                WHERE { ?tm rr:predicateObjectMap ?pom .
                        ?pom rr:predicateMap ?pm ;
                             rr:objectMap ?om
                        OPTIONAL {?pom rr:graphMap ?gm} }
                """
        self.graph.update(q2)

        # Query 4: Normalization step 4 (Replace referencing object maps without join conditions)
        self._q4_nojoin_parent = {}
        try:
            for om, ptm in self.graph.subject_objects(RR.parentTriplesMap):
                has_join = self.graph.value(om, RR.joinCondition)
                if has_join is None:
                    self._q4_nojoin_parent[om] = ptm
        except Exception:
            self._q4_nojoin_parent = {}

        q4 = prefixes + """
            DELETE { ?om rr:parentTriplesMap ?ptm }
            INSERT { ?om rr:reference ?ref ;
                    rr:template ?template;
                    rr:constant ?const ;
                    rr:termType rr:IRI . }
            WHERE { ?om rr:parentTriplesMap ?ptm .
                ?ptm rr:subjectMap ?sm .
                OPTIONAL{ ?sm rr:reference ?ref }
                OPTIONAL{ ?sm rr:template ?template }
                OPTIONAL{ ?sm rr:constant ?const }
                FILTER NOT EXISTS { ?om rr:joinCondition ?jc } }
            """
        self.graph.update(q4)

        # Query 5: Normalization step 5 (Duplicate triples maps that contain multiple pred.-object maps)
        self._q5_tm_clones = {}
        try:
            for tm in set(self.graph.subjects(RML.logicalSource, None)):
                ls = self.graph.value(tm, RML.logicalSource)
                sm = self.graph.value(tm, RR.subjectMap)
                poms = list(self.graph.objects(tm, RR.predicateObjectMap))
                if ls is not None and sm is not None and poms:
                    self._q5_tm_clones[tm] = [(ls, sm, pom) for pom in poms]
        except Exception:
            self._q5_tm_clones = {}

        q5 = prefixes + """
            DELETE { ?tm rdf:type rr:TriplesMap ;
                rml:logicalSource ?ls ;
                rr:subjectMap ?sm ;
                rr:predicateObjectMap ?pom }
            INSERT { [] rml:logicalSource ?ls ;
                rr:subjectMap ?sm ;
                rr:predicateObjectMap ?pom }
            WHERE { ?tm rml:logicalSource ?ls ;
                rr:subjectMap ?sm ;
                rr:predicateObjectMap ?pom }
            """
        self.graph.update(q5)

        # Query 6: Normalization step 6a (Duplicate TMs with subject maps where POMs contain multiple graph maps)
        q6 = prefixes + """
                DELETE { ?tm rr:predicateObjectMap ?pom .
                         ?pom rr:graphMap ?pom_gm . }
                INSERT { [] rml:logicalSource ?ls ;
                            rr:subjectMap [
                                rr:reference ?ref ;
                                rr:template ?template ;
                                rr:constant ?const ;
                                rr:termType ?ttype ;
                                rr:graphMap ?sm_gm ;
                                rr:graphMap ?pom_gm ] ;
                            rr:predicateObjectMap ?pom . }
                WHERE { ?tm rml:logicalSource ?ls ;
                        rr:subjectMap ?sm ;
                        rr:predicateObjectMap ?pom .
                        ?pom rr:graphMap ?pom_gm .
                        OPTIONAL { ?sm rr:graphMap ?sm_gm }
                        OPTIONAL { ?sm rr:reference ?ref }
                        OPTIONAL { ?sm rr:template ?template }
                        OPTIONAL { ?sm rr:constant ?const }
                        OPTIONAL { ?sm rr:termType ?ttype } }
                """
        self.graph.update(q6)

        # Query 7: Normalization step 6b (Duplicate subject maps that contain multiple graph maps)
        q7 = prefixes + """
                DELETE { ?sm rr:graphMap ?gm1 . }
                INSERT { [] rml:logicalSource ?ls ;
                            rr:subjectMap [
                                rr:reference ?ref ;
                                rr:template ?template ;
                                rr:constant ?const ;
                                rr:termType ?ttype ;
                                rr:graphMap ?gm1 ] ;
                            rr:predicateObjectMap ?pom }
                WHERE { ?tm rml:logicalSource ?ls ;
                        rr:subjectMap ?sm ;
                        rr:predicateObjectMap ?pom .
                        ?sm rr:graphMap ?gm1 ;
                        rr:graphMap ?gm2
                        FILTER ( ?gm1 != ?gm2 )
                        OPTIONAL { ?sm rr:reference ?ref }
                        OPTIONAL { ?sm rr:template ?template }
                        OPTIONAL { ?sm rr:constant ?const }
                        OPTIONAL { ?sm rr:termType ?ttype } }
                """
        self.graph.update(q7)

        logger.debug("Graph normalization finished.")

    def _extract_queries(self, tm: Node) -> Dict[str, str]:
        """
        Extracts all query parameters from the Triples Map's subject map.
        :param tm: The Triples Map URIRef.
        :return: A dictionary mapping query parameters to themselves.
        """
        P = {}

        def _normalize_name(s: str) -> str:
            import re
            if not s:
                return s
            m = re.search(r"[A-Za-z_][A-Za-z0-9_\-.]*", s)
            if m:
                return m.group(0)
            return s

        def scan_map(term_map) -> None:
            """
            Scans a term map for references and templates to extract query parameters.
            :param term_map: The term map to scan.
            :return: None
            """
            if not term_map:
                return

            ref = self.graph.value(term_map, RML.reference)
            if ref:
                ref_str = str(ref)
                name = _normalize_name(ref_str)
                P[name] = ref_str

            tmpl = self.graph.value(term_map, RR.template)
            if tmpl:
                import re
                # Unescape any escaped braces so variables inside templates are
                # detected correctly when the Turtle file encoded literal braces
                # as "\\{" and "\\}".
                tmpl_str = str(tmpl).replace('\\{', '{').replace('\\}', '}')
                # Match variables enclosed in single braces that are not part of
                # double/triple brace literals (avoid capturing outer literal
                # braces like '{{' or '}}'). Use lookarounds to ensure we only
                # capture single-brace variables.
                vars = re.findall(r'(?<!\{)\{([^{}]+)\}(?!\})', tmpl_str)
                for v in vars:
                    # If the variable name is a simple identifier, use dot-notation;
                    # otherwise use bracket-notation to support names with spaces
                    import re
                    if re.match(r'^[A-Za-z_][A-Za-z0-9_\-]*$', v):
                        P[v] = f"$.{v}"
                    else:
                        # escape single quotes inside v
                        safe_v = v.replace("'", "\\'")
                        P[v] = f"$['{safe_v}']"

        sm = self.graph.value(tm, RR.subjectMap)
        scan_map(sm)
        # Also scan any subject-level graphMap so variables used to build named graphs
        # get included in attribute mappings
        scan_map(self.graph.value(sm, RR.graphMap))

        for pom in self.graph.objects(tm, RR.predicateObjectMap):
            scan_map(self.graph.value(pom, RR.predicateMap))
            scan_map(self.graph.value(pom, RR.objectMap))
            # Include any graphMap attached to the predicate-object map
            scan_map(self.graph.value(pom, RR.graphMap))

        return P

    def _extract_join_attributes(self, object_map: Node):
        """
        Extract lists of join attribute names and their extraction queries from rr:joinCondition nodes attached to an object map.
        Returns (child_attrs, parent_attrs, child_queries, parent_queries) where each is a list.
        """
        child_attrs = []
        parent_attrs = []
        child_queries = []
        parent_queries = []

        def _normalize_name(s: str) -> str:
            import re
            if not s:
                return s
            m = re.search(r"[A-Za-z_][A-Za-z0-9_\-.]*", s)
            if m:
                return m.group(0)
            return s

        for jc in self.graph.objects(object_map, RR.joinCondition):
            parent = self.graph.value(jc, RR.parent)
            child = self.graph.value(jc, RR.child)

            def _extract(node):
                if node is None:
                    return None, None
                try:
                    ref = self.graph.value(node, RML.reference)
                    if ref:
                        q = str(ref)
                        return _normalize_name(q), q
                    tmpl = self.graph.value(node, RR.template)
                    if tmpl:
                        import re
                        vars = re.findall(r'\{([^}]+)\}', str(tmpl))
                        if len(vars) == 1:
                            v = vars[0]
                            import re
                            if re.match(r'^[A-Za-z_][A-Za-z0-9_\-]*$', v):
                                return v, f"$.{v}"
                            else:
                                safe_v = v.replace("'", "\\'")
                                return v, f"$['{safe_v}']"
                        return str(tmpl), None
                except Exception:
                    pass
                return str(node), None

            p_name, p_q = _extract(parent)
            c_name, c_q = _extract(child)

            if c_name and p_name:
                child_attrs.append(c_name)
                parent_attrs.append(p_name)
                child_queries.append(c_q)
                parent_queries.append(p_q)

        return child_attrs, parent_attrs, child_queries, parent_queries

    def _create_ext_expr(self, term_map: Node, default_term_type: str = "Literal") -> Expression:
        """
        Creates an extension expression from a term map.
        :param term_map: The term map node.
        :param default_term_type: The default term type if none is specified.
        :return: An Expression representing the term map.
        """
        # Line 1: Constant
        const = self.graph.value(term_map, RR.constant)
        if const:
            if isinstance(const, URIRef):
                return Constant(AlgebraIRI(str(const)))
            if isinstance(const, RDFLiteral):
                if const.language:
                    return Constant(AlgebraLiteral(str(const), language=str(const.language)))
                if const.datatype:
                    return Constant(AlgebraLiteral(str(const), datatype_iri=str(const.datatype)))
            return Constant(AlgebraLiteral(str(const)))

        # Line 3: Reference
        ref = self.graph.value(term_map, RML.reference)
        if ref:
            term_type = self.graph.value(term_map, RR.termType)

            if term_type is None:
                term_type = RR[default_term_type]

            if default_term_type == "IRI" and term_type == RR.Literal:
                raise ValueError("InvalidRulesError: rr:subjectMap cannot have rr:termType rr:Literal")

            # Normalize attribute name for references so Reference expressions
            # and attribute mappings use consistent attribute keys
            def _normalize_name(s: str) -> str:
                import re
                if not s:
                    return s
                m = re.search(r"[A-Za-z_][A-Za-z0-9_\-.]*", s)
                if m:
                    return m.group(0)
                return s

            ref_str = str(ref)
            ref_name = _normalize_name(ref_str)
            ref_expr = Reference(ref_name)

            # Handle rr:language if present on the term map
            lang_node = self.graph.value(term_map, RR.language)
            datatype_node = self.graph.value(term_map, RR.datatype)

            if lang_node is not None and datatype_node is not None:
                raise ValueError("InvalidRulesError: rr:language and rr:datatype cannot both be specified on the same term map")

            if term_type == RR.IRI:
                # Reference-valued term maps: follow R2RML semantics — do not percent-encode.
                if self.base_iri:
                    return FunctionCall(to_iri, [ref_expr, Constant(self.base_iri), Constant(False)])
                return FunctionCall(to_iri, [ref_expr, Constant(None), Constant(False)])
            elif term_type == RR.BlankNode:
                return FunctionCall(to_bnode, [ref_expr])
            else:
                if datatype_node is not None:
                    if not isinstance(datatype_node, URIRef):
                        raise ValueError(f"Invalid rr:datatype value: {datatype_node}")
                    return FunctionCall(to_literal, [ref_expr, Constant(str(datatype_node))])
                if lang_node is not None:
                    lang_raw = str(lang_node).strip()
                    # Strict behavior: rr:language must be a valid BCP47 language tag.
                    # Do not accept human-language names like "english"; require the
                    # canonical language tag (e.g., "en", "es").
                    from pyhartig.algebra.Terms import Literal as _AlgebraLiteral
                    try:
                        _AlgebraLiteral("x", language=lang_raw)
                    except Exception:
                        raise ValueError(f"Invalid rr:language value: {lang_raw}")
                    return FunctionCall(to_literal_lang, [ref_expr, Constant(lang_raw)])
                return FunctionCall(to_literal, [ref_expr, Constant(str(XSD.string))])

        # Line 5: Template
        tmpl = self.graph.value(term_map, RR.template)
        if tmpl:
            template_str = str(tmpl)
            import re
            # Split by curly braces but keep delimiters to identify vars
            # Unescape any backslash-escaped braces so templates like "\\{\\{ {X} \\}\\}"
            # become "{{ {X} }}" before extracting variables. This matches how the
            # RML test-suite encodes literal brace characters in Turtle files.
            template_str = template_str.replace('\\{', '{').replace('\\}', '}')
            # Split the template into literal parts and single-brace variable
            # placeholders. This avoids breaking on double/triple-brace
            # literal markers by ensuring we only split on single-brace vars.
            parts = re.split(r'(?<!\{)(\{[^{}]+\})(?!\})', template_str)

            args = []
            for part in parts:
                if part.startswith("{") and part.endswith("}"):
                    var = part[1:-1]
                    # Percent-encode inserted reference components so template
                    # insertion semantics match the RML test-suite expectations
                    args.append(FunctionCall(percent_encode_component, [Reference(var)]))
                elif part:
                    args.append(Constant(AlgebraLiteral(part)))

            if not args: return Constant(AlgebraLiteral(""))

            concat_expr = FunctionCall(concat, args)

            term_type = self.graph.value(term_map, RR.termType)

            if term_type is None:
                term_type = RR[default_term_type]

            if default_term_type == "IRI" and term_type == RR.Literal:
                raise ValueError("InvalidRulesError: rr:subjectMap cannot have rr:termType rr:Literal")

            # Check for rr:language on template term maps
            lang_node = self.graph.value(term_map, RR.language)
            datatype_node = self.graph.value(term_map, RR.datatype)

            if lang_node is not None and datatype_node is not None:
                raise ValueError("InvalidRulesError: rr:language and rr:datatype cannot both be specified on the same term map")

            if term_type == RR.IRI:
                if self.base_iri:
                    return FunctionCall(to_iri, [concat_expr, Constant(self.base_iri)])
                return FunctionCall(to_iri, [concat_expr])
            elif term_type == RR.BlankNode:
                return FunctionCall(to_bnode, [concat_expr])
            else:
                if datatype_node is not None:
                    if not isinstance(datatype_node, URIRef):
                        raise ValueError(f"Invalid rr:datatype value: {datatype_node}")
                    return FunctionCall(to_literal, [concat_expr, Constant(str(datatype_node))])
                if lang_node is not None:
                    lang_raw = str(lang_node).strip()
                    # Do not map human-language names to tags; require canonical BCP47.
                    from pyhartig.algebra.Terms import Literal as _AlgebraLiteral
                    try:
                        _AlgebraLiteral("x", language=lang_raw)
                    except Exception:
                        raise ValueError(f"Invalid rr:language value: {lang_raw}")
                    return FunctionCall(to_literal_lang, [concat_expr, Constant(lang_raw)])
                return FunctionCall(to_literal, [concat_expr, Constant(str(XSD.string))])

        return Constant(AlgebraIRI("http://error"))

    def explain(self) -> str:
        """
        Generates a human-readable explanation of the entire mapping pipeline.
        :return: String representation of the operator tree.
        """
        pipeline = self.parse()
        return pipeline.explain()

    def explain_json(self) -> Dict[str, Any]:
        """
        Parse the mapping and return JSON explanation of the resulting pipeline.

        :return: Dictionary with pipeline structure
        """
        pipeline = self.parse()
        return pipeline.explain_json()

    def save_explanation(self, output_path: str, format: str = "json"):
        """
        Save pipeline explanation to file.

        :param output_path: Path to output file
        :param format: "json" or "text"
        """
        import json

        pipeline = self.parse()

        if format == "json":
            explanation = pipeline.explain_json()
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(explanation, f, indent=2, ensure_ascii=False)
        else:
            explanation = pipeline.explain()
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(explanation)
