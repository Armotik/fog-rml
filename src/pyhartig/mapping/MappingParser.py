import logging
from rdflib import Graph, URIRef, Namespace, Node
from typing import List, Dict, Any
from pathlib import Path

from pyhartig.operators.Operator import Operator
from pyhartig.operators.ExtendOperator import ExtendOperator
from pyhartig.operators.UnionOperator import UnionOperator
from pyhartig.operators.EquiJoinOperator import EquiJoinOperator

from pyhartig.expressions.Expression import Expression
from pyhartig.expressions.Constant import Constant
from pyhartig.expressions.Reference import Reference
from pyhartig.expressions.FunctionCall import FunctionCall
from pyhartig.algebra.Terms import IRI as AlgebraIRI, Literal as AlgebraLiteral
from pyhartig.namespaces import RR_BASE, RML_BASE, QL_BASE, RDF_BASE, XSD_BASE
from pyhartig.operators.SourceFactory import SourceFactory
from pyhartig.functions.builtins import to_iri, to_literal, concat, to_bnode

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

        # Normalize the RML graph
        self._normalize_graph()
        logger.debug("RML Graph normalized.")

        # Initialize an empty list to hold the operators for each Triples Map
        S: List[Operator] = []

        # We find all resources typed as rr:TriplesMap or having a logicalSource
        triples_maps = set(self.graph.subjects(RDF.type, RR.TriplesMap))
        triples_maps.update(self.graph.subjects(RML.logicalSource, None))
        triples_maps.update(self.graph.subjects(RR.logicalSource, None))

        # [INFO] Log the number of Triples Maps found
        logger.info(f"Found {len(triples_maps)} TriplesMaps to process.")

        for tm in triples_maps:
            # [DEBUG] Log the current Triples Map being processed
            logger.debug(f"Processing TriplesMap: {tm}")

            # Line 4: Let LS be the logical source of TM
            ls_node = self.graph.value(tm, RML.logicalSource) or self.graph.value(tm, RR.logicalSource)

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
            sm = self.graph.value(tm, RR.subjectMap)

            # Line 7: phi_sbj := CREATEEXTEXPR(SM)
            if sm:
                phi_sbj = self._create_ext_expr(sm, default_term_type="IRI")
                # Line 8: E := Extend(E_src, "subject", phi_sbj)
                E_base = ExtendOperator(E_src, "subject", phi_sbj)
            else:
                # Fallback if no subject map (should not happen in valid RML)
                logger.debug(f"No SubjectMap found for {tm}, skipping subject generation.")
                E_base = E_src

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

                if parent_tm:
                    # Build parent source operator
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
                            for cand in triples_maps:
                                sm_cand = self.graph.value(cand, RR.subjectMap)
                                if not sm_cand:
                                    continue
                                # check rml:reference
                                cand_ref = self.graph.value(sm_cand, RML.reference)
                                if cand_ref:
                                    # normalize cand_ref name and compare to parent_attrs
                                    import re
                                    m = re.search(r"[A-Za-z_][A-Za-z0-9_\-]*", str(cand_ref))
                                    cand_name = m.group(0) if m else str(cand_ref)
                                    if any(cand_name == pa for pa in parent_attrs):
                                        parent_tm = cand
                                        parent_ls = self.graph.value(parent_tm, RML.logicalSource) or self.graph.value(parent_tm, RR.logicalSource)
                                        found = True
                                        logger.debug(f"Resolved parent TriplesMap by reference: {parent_tm}")
                                        break
                                # check template variables inside rr:template
                                cand_tmpl = self.graph.value(sm_cand, RR.template)
                                if cand_tmpl:
                                    tmpl_str = str(cand_tmpl)
                                    for pa in parent_attrs:
                                        # match variable name inside template {var}
                                        if pa in tmpl_str or f"{{{pa}}}" in tmpl_str:
                                            parent_tm = cand
                                            parent_ls = self.graph.value(parent_tm, RML.logicalSource) or self.graph.value(parent_tm, RR.logicalSource)
                                            found = True
                                            logger.debug(f"Resolved parent TriplesMap by template match: {parent_tm}")
                                            break
                                if found:
                                    break

                        if not found and (parent_ls is None):
                            logger.error(f"Parent TriplesMap {parent_tm} has no logicalSource; skipping join.")
                            continue

                    # Extract parent queries and ensure parent attribute mappings include the join parent attributes
                    P_parent = self._extract_queries(parent_tm)
                    try:
                        for name, q in zip(parent_attrs, parent_queries):
                            if name and q and name not in P_parent:
                                P_parent[name] = q
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
                    else:
                        phi_parent_sbj = None

                    if not child_attrs or not parent_attrs:
                        logger.error(f"No valid join conditions found in referencing object map {om}; skipping.")
                        continue

                    # Ensure the child attributes used in the join are available in the
                    # current TriplesMap's attribute mappings (P). If not, add them using
                    # the extracted JSONPath queries so the SourceOperator will produce
                    # the required attributes for joining.
                    for name, q in zip(child_attrs, child_queries):
                        if name and q and name not in P:
                            P[name] = q

                    # Ensure parent attribute mappings include parent join attributes (already added to P_parent above)

                    # Create EquiJoin between the raw source operators (before subject extension)
                    try:
                        join_op = EquiJoinOperator(E_src, E_parent_src, child_attrs, parent_attrs)
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
            m = re.search(r"[A-Za-z_][A-Za-z0-9_\-]*", s)
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
                vars = re.findall(r'\{([^}]+)\}', str(tmpl))
                for v in vars:
                    P[v] = f"$.{v}"

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
            m = re.search(r"[A-Za-z_][A-Za-z0-9_\-]*", s)
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
                            return v, f"$.{v}"
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
            return Constant(AlgebraLiteral(str(const)))

        # Line 3: Reference
        ref = self.graph.value(term_map, RML.reference)
        if ref:
            term_type = self.graph.value(term_map, RR.termType)

            if term_type is None:
                term_type = RR[default_term_type]

            # Normalize attribute name for references so Reference expressions
            # and attribute mappings use consistent attribute keys
            def _normalize_name(s: str) -> str:
                import re
                if not s:
                    return s
                m = re.search(r"[A-Za-z_][A-Za-z0-9_\-]*", s)
                if m:
                    return m.group(0)
                return s

            ref_str = str(ref)
            ref_name = _normalize_name(ref_str)
            ref_expr = Reference(ref_name)

            if term_type == RR.IRI:
                return FunctionCall(to_iri, [ref_expr])
            elif term_type == RR.BlankNode:
                return FunctionCall(to_bnode, [ref_expr])
            else:
                return FunctionCall(to_literal, [ref_expr, Constant(str(XSD.string))])

        # Line 5: Template
        tmpl = self.graph.value(term_map, RR.template)
        if tmpl:
            template_str = str(tmpl)
            import re
            # Split by curly braces but keep delimiters to identify vars
            parts = re.split(r'(\{.*?\})', template_str)

            args = []
            for part in parts:
                if part.startswith("{") and part.endswith("}"):
                    var = part[1:-1]
                    args.append(Reference(var))
                elif part:
                    args.append(Constant(AlgebraLiteral(part)))

            if not args: return Constant(AlgebraLiteral(""))

            concat_expr = FunctionCall(concat, args)

            term_type = self.graph.value(term_map, RR.termType)

            if term_type is None:
                term_type = RR[default_term_type]

            if term_type == RR.IRI:
                return FunctionCall(to_iri, [concat_expr])
            elif term_type == RR.BlankNode:
                return FunctionCall(to_bnode, [concat_expr])
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
