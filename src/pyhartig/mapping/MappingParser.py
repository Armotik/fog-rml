import logging
from urllib.parse import urlsplit
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
from pyhartig.namespaces import (
    D2RQ_BASE,
    FNML_BASE,
    FNO_BASE,
    PYHARTIG_ERROR_URI,
    QL_BASE,
    RDF_BASE,
    RML_BASE,
    RR_BASE,
    XSD_BASE,
)
from pyhartig.operators.SourceFactory import SourceFactory
from pyhartig.functions.builtins import to_iri, to_literal, concat, to_bnode, to_literal_lang, percent_encode_component

RR = Namespace(RR_BASE)
RML = Namespace(RML_BASE)
QL = Namespace(QL_BASE)
RDF = Namespace(RDF_BASE)
XSD = Namespace(XSD_BASE)
FNML = Namespace(FNML_BASE)
FNO = Namespace(FNO_BASE)

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

    @staticmethod
    def _is_simple_identifier(value: str) -> bool:
        """
        Checks if the given string is a simple identifier (matches [A-Za-z_][A-Za-z0-9_-]*)
        :param value: The string to check
        :return: True if the string is a simple identifier, False otherwise
        """
        if not value:
            return False
        first_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_"
        rest_chars = first_chars + "0123456789-"
        if value[0] not in first_chars:
            return False
        return all(ch in rest_chars for ch in value[1:])

    @staticmethod
    def _flush_literal_buffer(segments: List[tuple[str, str]], literal_buffer: List[str]) -> None:
        """
        Flushes the literal buffer into the segments list as a single literal segment.
        :param segments: The segments to flush
        :param literal_buffer: The literal buffer
        :return: None
        """
        if not literal_buffer:
            return
        segments.append(("literal", "".join(literal_buffer)))
        literal_buffer.clear()

    @staticmethod
    def _consume_open_brace(
            template_str: str,
            index: int,
            segments: List[tuple[str, str]],
            literal_buffer: List[str],
    ) -> int:
        """
        Consumes an open brace from the template string, handling escaped braces and variable extraction.
        :param template_str: The template string
        :param index: The index in the template string
        :param segments: The segments list to append to
        :param literal_buffer: The literal buffer
        :return: The index in the template string
        """
        if index + 1 < len(template_str) and template_str[index + 1] == "{":
            literal_buffer.append("{{")
            return index + 2

        end = template_str.find("}", index + 1)
        if end == -1:
            literal_buffer.append("{")
            return index + 1

        inner = template_str[index + 1:end]
        if "{" in inner or (end + 1 < len(template_str) and template_str[end + 1] == "}"):
            literal_buffer.append("{")
            return index + 1

        MappingParser._flush_literal_buffer(segments, literal_buffer)
        segments.append(("var", inner))
        return end + 1

    @staticmethod
    def _consume_close_brace(template_str: str, index: int, literal_buffer: List[str]) -> int:
        """
        Consumes a close brace from the template string, handling escaped braces.
        :param template_str: The template string
        :param index: The index in the template string
        :param literal_buffer: The literal buffer
        :return: The index in the template string
        """
        if index + 1 < len(template_str) and template_str[index + 1] == "}":
            literal_buffer.append("}}")
            return index + 2
        literal_buffer.append("}")
        return index + 1

    @staticmethod
    def _iter_template_segments(template_str: str) -> List[tuple[str, str]]:
        """
        Iterates over the segments of a template string, yielding tuples of (kind, value) where kind is either "literal" or "var".
        :param template_str: The template string
        :return: The segments list
        """
        segments: List[tuple[str, str]] = []
        literal_buffer: List[str] = []
        i = 0

        while i < len(template_str):
            current = template_str[i]
            if current == "{":
                i = MappingParser._consume_open_brace(template_str, i, segments, literal_buffer)
                continue

            if current == "}":
                i = MappingParser._consume_close_brace(template_str, i, literal_buffer)
                continue

            literal_buffer.append(current)
            i += 1

        MappingParser._flush_literal_buffer(segments, literal_buffer)

        return segments

    @classmethod
    def _extract_single_brace_variables(cls, template_str: str) -> List[str]:
        """
        Extracts variable names from a template string that are enclosed in single braces (e.g., {var}).
        :param template_str: The template string
        :return: The list of variable names
        """
        return [value for kind, value in cls._iter_template_segments(template_str) if kind == "var"]

    def parse(self) -> Operator:
        """
        Parses an RML mapping file and translates it into an algebraic plan
        :return: Operator representing the entire mapping.
        """
        logger.info(f"Parsing RML mapping file: {self.rml_file_path}")
        self._load_mapping_graph()  # Load the mapping graph from the RML file
        triples_maps = self._collect_triples_maps()
        logger.info(f"Found {len(triples_maps)} TriplesMaps to process.")

        branches: List[Operator] = []
        for tm in triples_maps:
            branches.extend(self._process_triples_map(tm, triples_maps))

        if not branches:
            logger.error("Parsing failed: No operators generated.")
            raise ValueError("No valid mappings generated from RML file.")

        logger.info(f"Pipeline construction complete. Total Union branches: {len(branches)}")

        if len(branches) == 1:
            return branches[0]

        return UnionOperator(branches)

    def _load_mapping_graph(self) -> None:
        """
        Loads the RML mapping file into an RDF graph, with robust error handling and normalization.
        :return: None
        """
        from pathlib import Path as _Path
        if not _Path(self.rml_file_path).exists():
            logger.error(f"RML mapping file not found: {self.rml_file_path}")
            raise FileNotFoundError(f"RML mapping file not found: {self.rml_file_path}")

        self._parse_rml_file()
        logger.debug(f"RDF Graph loaded ({len(self.graph)} triples). Normalizing...")
        self._validate_raw_triples_maps()
        self._normalize_graph()
        logger.debug("RML Graph normalized.")

    def _parse_rml_file(self) -> None:
        """
        Attempts to parse the RML file using multiple strategies to handle various edge cases and malformed inputs.
        :return: None
        """
        try:
            self.graph.parse(self.rml_file_path, format="turtle")
            return
        except Exception as e:
            logger.error(f"Failed to parse RML file {self.rml_file_path}: {e}")

        if self._parse_with_fallback_formats():
            return
        self._parse_with_sanitized_content()

    def _parse_with_fallback_formats(self) -> bool:
        """
        Attempts to parse the RML file using fallback RDF formats (N3, TriG, RDF/XML) to handle cases where Turtle parsing fails.
        :return: None
        """
        for fmt in ("n3", "trig", "xml"):
            try:
                logger.info(f"Attempting parse with format='{fmt}'")
                self.graph.parse(self.rml_file_path, format=fmt)
                logger.info(f"Parsed RML file with format '{fmt}'")
                return True
            except Exception:
                continue
        return False

    def _parse_with_sanitized_content(self) -> None:
        """
        Attempts to parse the RML file after sanitizing its content by escaping backslashes in string literals. This can help handle cases where the Turtle parser fails due to unescaped backslashes in literals.
        :return: None
        """
        try:
            import re
            with open(self.rml_file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            def _escape_backslashes_in_str(m):
                """
                Escapes single backslashes inside a Turtle string literal match.
                :param m: Regex match for one quoted string literal.
                :return: Sanitized literal content ready for reparsing.
                """
                inner = m.group(1)
                if "\\\\" in inner:
                    return '"' + inner + '"'
                return '"' + inner.replace('\\', '\\\\') + '"'

            sanitized = re.sub(r'"([^"\\]*(?:\\.[^"\\]*)*)"', _escape_backslashes_in_str, content)
            logger.info("Attempting to parse sanitized RML content (escaped backslashes in strings)")
            self.graph.parse(data=sanitized, format='turtle')
            logger.info("Parsed RML file from sanitized content")
        except Exception:
            self._log_rml_file_sample()
            raise

    def _log_rml_file_sample(self) -> None:
        """
        Logs a sample of the RML file content for diagnostic purposes when parsing fails.
        :return: None
        """
        try:
            with open(self.rml_file_path, 'r', encoding='utf-8') as f:
                sample = f.read(1000)
            logger.debug(f"RML file sample (first 1000 chars): {repr(sample)}")
        except Exception as e:
            logger.debug(f"Could not read file for diagnostic: {e}")

    def _validate_raw_triples_maps(self) -> None:
        """
        Validates the raw TriplesMap definitions in the graph to ensure that no TriplesMap contains multiple rr:subjectMap definitions, which would be invalid according to RML specifications. This is done before normalization to catch issues early and provide clearer error messages.
        :return: None
        """
        triples_maps_raw = set(self.graph.subjects(RDF.type, RR.TriplesMap))
        triples_maps_raw.update(self.graph.subjects(RML.logicalSource, None))
        for tm_raw in triples_maps_raw:
            sm_count = len(list(self.graph.objects(tm_raw, RR.subjectMap)))
            if sm_count > 1:
                raise ValueError(f"InvalidRulesError: TriplesMap {tm_raw} contains multiple rr:subjectMap definitions")

    def _collect_triples_maps(self):
        """
        Collects all TriplesMap nodes from the graph, including those that may not be explicitly typed as rr:TriplesMap but can be inferred from the presence of logical source or subject map properties. This ensures that we process all valid TriplesMaps even if they are not perfectly defined.
        :return: None
        """
        triples_maps = set(self.graph.subjects(RDF.type, RR.TriplesMap))
        triples_maps.update(self.graph.subjects(RML.logicalSource, None))
        triples_maps.update(self.graph.subjects(RR.logicalSource, None))
        triples_maps.update(self.graph.subjects(RR.logicalTable, None))
        return triples_maps

    def _process_triples_map(self, tm: Node, triples_maps) -> List[Operator]:
        """
        Processes a single TriplesMap node and constructs the corresponding operator branches for it. This includes resolving the logical source, building the base subject operator, and processing each predicate-object map to create the full set of operators for this TriplesMap.
        :param tm: TriplesMap node
        :param triples_maps: TriplesMap nodes
        :return: List[Operator]
        """
        logger.debug(f"Processing TriplesMap: {tm}")
        ls_node = self._resolve_logical_source(tm)
        if not ls_node:
            logger.warning(f"Skipping TriplesMap {tm}: No logical source found.")
            return []

        source_mappings = self._extract_queries(tm)
        source_operator = self._create_source_operator(ls_node, source_mappings, f"Failed to create source for {tm}")
        if source_operator is None:
            return []

        sm, phi_sbj, base_operator = self._build_base_subject_operator(tm, source_operator)
        branches = self._process_predicate_object_maps(
            tm, sm, phi_sbj, base_operator, ls_node, source_mappings, triples_maps
        )
        if not branches:
            logger.debug(f"No branches generated for {tm} (maybe no POMs?).")
        return branches

    def _resolve_logical_source(self, tm: Node) -> Node | None:
        """
        Resolves the logical source for a given TriplesMap node. It first checks for an explicit logical source definition (RML or RR), and if not found, it attempts to infer a logical source from a logical table definition. If a logical table is found, it constructs a logical source node based on the database connection information and query/table name specified in the logical table.
        :param tm: TriplesMap node
        :return: TriplesMap node with logical source or None if it cannot be resolved
        """
        ls_node = self.graph.value(tm, RML.logicalSource) or self.graph.value(tm, RR.logicalSource)
        if ls_node:
            return ls_node

        logical_table = self.graph.value(tm, RR.logicalTable)
        if not logical_table:
            return None

        d2rq_database_type = URIRef(f"{D2RQ_BASE}Database")
        db_source = next(self.graph.subjects(RDF.type, d2rq_database_type), None)
        table_name = self.graph.value(logical_table, RR.tableName)
        sql_query = self.graph.value(logical_table, RR.sqlQuery)
        sql_version = self.graph.value(logical_table, RR.sqlVersion)
        if not db_source or (table_name is None and sql_query is None):
            return None

        ls_node = BNode()
        self.graph.add((ls_node, RML.source, db_source))
        if table_name is not None:
            self.graph.add((ls_node, RR.tableName, table_name))
        if sql_query is not None:
            self.graph.add((ls_node, RML.query, sql_query))
        if sql_version is not None:
            self.graph.add((ls_node, RR.sqlVersion, sql_version))
        return ls_node

    def _create_source_operator(self, logical_source_node: Node, attribute_mappings: Dict[str, str],
                                error_prefix: str) -> Operator | None:
        """
        Creates a source operator for the given logical source node and attribute mappings. This method uses the SourceFactory to create the appropriate source operator based on the type of logical source (e.g., CSV, JSON, SPARQL endpoint). It includes robust error handling to catch issues during source operator creation and logs detailed error messages to aid in debugging.
        :param logical_source_node: Logical source node
        :param attribute_mappings: Attribute mappings
        :param error_prefix: Error prefix
        :return: SourceOperator instance or None if creation fails
        """
        try:
            return SourceFactory.create_source_operator(
                graph=self.graph,
                logical_source_node=logical_source_node,
                mapping_dir=self.mapping_dir,
                attribute_mappings=attribute_mappings
            )
        except ValueError as e:
            logger.error(f"{error_prefix}: {e}")
            return None

    def _build_base_subject_operator(self, tm: Node, source_operator: Operator) -> tuple[Node, Expression, Operator]:
        """
        Builds the base subject operator for a given TriplesMap. It retrieves the subject map from the graph, creates the expression for the subject, and constructs an ExtendOperator to generate the subject term. This operator will serve as the base for further extensions when processing predicate-object maps.
        :param tm: TriplesMap node
        :param source_operator: SourceOperator instance
        :return: ExtendOperator instance
        """
        sm_objs = list(self.graph.objects(tm, RR.subjectMap))
        if len(sm_objs) > 1:
            raise ValueError(f"InvalidRulesError: TriplesMap {tm} contains multiple rr:subjectMap definitions")

        sm = sm_objs[0] if sm_objs else None
        if sm is None:
            raise ValueError(f"InvalidRulesError: TriplesMap {tm} contains no rr:subjectMap definition")

        phi_sbj = self._create_ext_expr(sm, default_term_type="IRI")
        base_operator = ExtendOperator(source_operator, "subject", phi_sbj)
        return sm, phi_sbj, base_operator

    def _process_predicate_object_maps(
            self,
            tm: Node,
            sm: Node,
            phi_sbj: Expression,
            base_operator: Operator,
            ls_node: Node,
            source_mappings: Dict[str, str],
            triples_maps,
    ) -> List[Operator]:
        """
        Processes all PredicateObjectMaps for a given TriplesMap and constructs the corresponding operator branches for each. It iterates over all PredicateObjectMaps, builds the predicate and object expressions, and creates the necessary operators to generate the RDF triples according to the mapping. This method also handles referencing object maps by resolving parent TriplesMaps and constructing join operations as needed.
        :param tm: TriplesMap node
        :param sm: TriplesMap node
        :param phi_sbj: PredicateObjectMap instance
        :param base_operator: PredicateObjectMap instance
        :param ls_node: PredicateObjectMap instance
        :param triples_maps: TriplesMap instance
        :return: List of RDF triples branches
        """
        poms = list(self.graph.objects(tm, RR.predicateObjectMap))
        logger.debug(f"Found {len(poms)} PredicateObjectMaps in TriplesMap {tm}.")

        branches = []
        for pom in poms:
            branch = self._build_predicate_object_branch(
                tm, pom, sm, phi_sbj, base_operator, ls_node, source_mappings, triples_maps
            )
            if branch is not None:
                branches.append(branch)
        return branches

    def _build_predicate_object_branch(
            self,
            tm: Node,
            pom: Node,
            sm: Node,
            phi_sbj: Expression,
            base_operator: Operator,
            ls_node: Node,
            source_mappings: Dict[str, str],
            triples_maps,
    ) -> Operator | None:
        """
        Builds the operator branch for a single PredicateObjectMap. It retrieves the predicate map and object map from the graph, creates the expressions for the predicate and object, and constructs the necessary operators to generate the RDF triples according to the mapping. If the object map is a referencing object map, it resolves the parent TriplesMap and constructs join operations as needed. This method includes robust error handling to catch issues during branch construction and logs detailed error messages to aid in debugging.
        :param tm: TriplesMap node
        :param sm: TriplesMap node
        :param phi_sbj: PredicateObjectMap instance
        :param base_operator: PredicateObjectMap instance
        :param ls_node: PredicateObjectMap instance
        :param triples_maps: TriplesMap instance
        :return: RDF triples branches
        """
        pm = self.graph.value(pom, RR.predicateMap)
        om = self.graph.value(pom, RR.objectMap)
        phi_pred = self._create_ext_expr(pm, default_term_type="IRI")

        try:
            parent_tm = self.graph.value(om, RR.parentTriplesMap)
        except Exception:
            parent_tm = None

        child_attrs, parent_attrs, child_queries, parent_queries = self._extract_join_attributes(om)
        nojoin_parent_fallback = False
        if (not parent_tm) and (om in self._q4_nojoin_parent):
            parent_tm = self._q4_nojoin_parent.get(om)
            nojoin_parent_fallback = True

        if parent_tm:
            branch = self._build_referencing_object_map_branch(
                tm,
                om,
                ls_node,
                phi_sbj,
                phi_pred,
                source_mappings,
                triples_maps,
                parent_tm,
                child_attrs,
                parent_attrs,
                child_queries,
                parent_queries,
                nojoin_parent_fallback,
            )
            if branch is None:
                return None
        else:
            branch = self._build_regular_object_map_branch(base_operator, om, phi_pred)

        return self._apply_graph_and_project(branch, pom, sm, tm)

    def _build_referencing_object_map_branch(
            self,
            tm: Node,
            om: Node,
            ls_node: Node,
            phi_sbj: Expression,
            phi_pred: Expression,
            source_mappings: Dict[str, str],
            triples_maps,
            parent_tm: Node,
            child_attrs: List[str],
            parent_attrs: List[str],
            child_queries: List[str],
            parent_queries: List[str],
            nojoin_parent_fallback: bool,
    ) -> Operator | None:
        """
        Builds the operator branch for a referencing object map. It resolves the parent TriplesMap and its logical source, prepares the source mappings for the parent and child, creates the necessary source operators, and constructs an EquiJoinOperator to join the child and parent sources based on the specified join conditions. The resulting branch is then extended with the appropriate expressions for the subject, predicate, and object to generate the RDF triples according to the mapping. This method includes robust error handling to catch issues during branch construction and logs detailed error messages to aid in debugging.
        :param tm: TriplesMap node
        :param om: TriplesMap node
        :param ls_node: TriplesMap node
        :param phi_sbj: PredicateObjectMap instance
        :param phi_pred: PredicateObjectMap instance
        :param source_mappings: TriplesMap instance
        :param triples_maps: TriplesMap instance
        :param parent_tm: TriplesMap instance
        :param child_attrs: TriplesMap instance
        :param parent_attrs: TriplesMap instance
        :param child_queries: TriplesMap instance
        :param parent_queries: TriplesMap instance
        :param nojoin_parent_fallback: bool
        :return: RDF triples branches
        """
        parent_tm, parent_ls = self._resolve_parent_join_context(parent_tm, ls_node, parent_attrs, triples_maps)
        if parent_ls is None:
            logger.error(f"Parent TriplesMap {parent_tm} has no logicalSource; skipping join.")
            return None

        parent_source_mappings, rename_map = self._prepare_parent_source_mappings(
            source_mappings, parent_tm, parent_attrs, parent_queries
        )
        parent_source = self._create_source_operator(
            parent_ls,
            parent_source_mappings,
            f"Failed to create parent source for {parent_tm}",
        )
        if parent_source is None:
            return None

        phi_parent_sbj = self._build_parent_subject_expr(parent_tm, rename_map)
        if len(child_attrs) != len(parent_attrs):
            logger.error(f"Invalid join conditions in referencing object map {om}; skipping.")
            return None

        child_join_mappings = self._prepare_child_join_mappings(
            source_mappings, om, child_attrs, child_queries, nojoin_parent_fallback
        )
        child_source = self._create_source_operator(
            ls_node,
            child_join_mappings,
            f"Failed to create child source for join in {tm}",
        )
        if child_source is None:
            return None

        try:
            effective_parent_attrs = [rename_map.get(name, name) for name in parent_attrs]
            join_op = EquiJoinOperator(child_source, parent_source, child_attrs, effective_parent_attrs)
        except Exception as e:
            logger.error(f"Failed to create EquiJoinOperator for {tm} <> {parent_tm}: {e}")
            return None

        branch = ExtendOperator(join_op, "subject", phi_sbj)
        if phi_parent_sbj is not None:
            branch = ExtendOperator(branch, "parent_subject", phi_parent_sbj)
        branch = ExtendOperator(branch, "predicate", phi_pred)
        return ExtendOperator(branch, "object", Reference("parent_subject"))

    def _resolve_parent_join_context(self, parent_tm: Node, ls_node: Node, parent_attrs: List[str], triples_maps):
        """
        Resolves the context for a parent TriplesMap in a referencing object map join. It first checks for an explicit logical source definition for the parent TriplesMap, and if not found, it looks for any clones of the parent TriplesMap that may have been created during normalization. If a clone is found, it uses the clone's logical source. If no explicit logical source is found, it attempts to resolve the parent TriplesMap by matching candidate TriplesMaps based on the join attributes and logical source characteristics. This method ensures that we can correctly identify the parent context for the join operation even in cases where the mapping is not perfectly defined.
        :param parent_tm: TriplesMap node
        :param ls_node: TriplesMap node
        :param parent_attrs: TriplesMap instance
        :param triples_maps: TriplesMap instance
        :return: TriplesMap instance
        """
        parent_ls = self.graph.value(parent_tm, RML.logicalSource) or self.graph.value(parent_tm, RR.logicalSource)
        if parent_ls:
            return parent_tm, parent_ls

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
            if parent_ls:
                return parent_tm, parent_ls

        try:
            props = list(self.graph.predicate_objects(parent_tm))
            logger.debug(f"Parent TriplesMap {parent_tm} predicates: {props}")
        except Exception:
            pass

        resolved = self._find_candidate_parent_triples_map(ls_node, parent_attrs, triples_maps)
        if resolved is None:
            return parent_tm, None
        logger.debug(f"Resolved parent TriplesMap by candidate match: {resolved}")
        parent_tm = resolved
        parent_ls = self.graph.value(parent_tm, RML.logicalSource) or self.graph.value(parent_tm, RR.logicalSource)
        return parent_tm, parent_ls

    def _find_candidate_parent_triples_map(self, ls_node: Node, parent_attrs: List[str], triples_maps):
        """
        Finds a candidate parent TriplesMap based on the logical source and join attributes. It iterates over all TriplesMaps and checks if they match the join attributes specified in the referencing object map. If multiple candidates match, it prefers those with a different logical source than the child, as this is more likely to be the intended parent in cases where the mapping is not perfectly defined. This method helps to resolve ambiguities in the mapping and ensures that we can still construct a valid operator pipeline even when the parent TriplesMap is not explicitly defined.
        :param ls_node: TriplesMap node
        :param parent_attrs: TriplesMap instance
        :param triples_maps: TriplesMap instance
        :return: TriplesMap instance
        """
        if not parent_attrs:
            return None

        child_source_literal = self._get_logical_source_literal(ls_node)

        preferred = []
        fallback = []
        for cand in triples_maps:
            if not self._candidate_matches_parent_attrs(cand, parent_attrs):
                continue

            cand_source = self._get_candidate_source_literal(cand)
            if self._prefer_candidate_source(child_source_literal, cand_source):
                preferred.append(cand)
            else:
                fallback.append(cand)

        if preferred:
            return preferred[0]
        if fallback:
            return fallback[0]
        return None

    def _get_logical_source_literal(self, ls_node: Node):
        """
        Retrieves a string representation of the logical source for a given logical source node. It checks for the presence of RML.source or RR.source properties and returns their string value if found. This method is used to compare logical sources when resolving candidate parent TriplesMaps in referencing object maps, allowing us to prefer candidates with different logical sources than the child.
        :param ls_node: TriplesMap node
        :return: Logical Source string representation of the logical source for a given logical source node
        """
        try:
            return str(self.graph.value(ls_node, RML.source) or self.graph.value(ls_node, RR.source) or "")
        except Exception:
            return None

    def _get_candidate_source_literal(self, candidate_tm: Node):
        """
        Retrieves the logical source literal associated with a candidate parent TriplesMap.
        :param candidate_tm: Candidate parent TriplesMap node.
        :return: String representation of the candidate logical source, or None.
        """
        candidate_ls_node = self.graph.value(candidate_tm, RML.logicalSource) or self.graph.value(candidate_tm,
                                                                                                  RR.logicalSource)
        return self._get_logical_source_literal(candidate_ls_node)

    def _candidate_matches_parent_attrs(self, candidate_tm: Node, parent_attrs: List[str]) -> bool:
        """
        Checks whether a candidate parent TriplesMap exposes one of the expected parent join attributes.
        :param candidate_tm: Candidate parent TriplesMap node.
        :param parent_attrs: Parent-side join attribute names.
        :return: True when the candidate can provide at least one requested parent attribute.
        """
        import re

        sm_cand = self.graph.value(candidate_tm, RR.subjectMap)
        if not sm_cand:
            return False

        cand_ref = self.graph.value(sm_cand, RML.reference)
        if cand_ref:
            match = re.search(r"[A-Za-z_][A-Za-z0-9_\-]*", str(cand_ref))
            cand_name = match.group(0) if match else str(cand_ref)
            if any(cand_name == parent_attr for parent_attr in parent_attrs):
                return True

        cand_tmpl = self.graph.value(sm_cand, RR.template)
        if not cand_tmpl:
            return False

        tmpl_str = str(cand_tmpl)
        return any(parent_attr in tmpl_str or f"{{{parent_attr}}}" in tmpl_str for parent_attr in parent_attrs)

    @staticmethod
    def _prefer_candidate_source(child_source_literal, candidate_source_literal) -> bool:
        """
        Prefers parent candidates whose logical source differs from the child logical source.
        :param child_source_literal: Child logical source literal.
        :param candidate_source_literal: Candidate parent logical source literal.
        :return: True when the candidate should be preferred.
        """
        return bool(
            child_source_literal and candidate_source_literal and candidate_source_literal != child_source_literal)

    def _prepare_parent_source_mappings(
            self,
            source_mappings: Dict[str, str],
            parent_tm: Node,
            parent_attrs: List[str],
            parent_queries: List[str],
    ):
        """
        Builds the attribute mapping dictionary used to instantiate the parent source in a join.
        :param source_mappings: Child-side source mappings.
        :param parent_tm: Parent TriplesMap node.
        :param parent_attrs: Parent-side join attribute names.
        :param parent_queries: Parent-side extraction queries.
        :return: Tuple of parent source mappings and attribute rename map.
        """
        parent_source_mappings = self._extract_queries(parent_tm)
        rename_map = {}
        self._inject_parent_join_mappings(
            parent_source_mappings,
            rename_map,
            source_mappings,
            parent_attrs,
            parent_queries,
        )
        self._rename_parent_mapping_collisions(parent_source_mappings, rename_map, source_mappings)
        return parent_source_mappings, rename_map

    @staticmethod
    def _inject_parent_join_mappings(
            parent_source_mappings: Dict[str, str],
            rename_map: Dict[str, str],
            source_mappings: Dict[str, str],
            parent_attrs: List[str],
            parent_queries: List[str],
    ) -> None:
        """
        Injects parent join attributes into the parent source mappings.
        :param parent_source_mappings: Parent-side source mappings to update.
        :param rename_map: Mapping of original parent names to renamed attributes.
        :param source_mappings: Child-side source mappings.
        :param parent_attrs: Parent-side join attribute names.
        :param parent_queries: Parent-side extraction queries.
        :return: None
        """
        try:
            for name, query in zip(parent_attrs, parent_queries):
                MappingParser._inject_single_parent_join_mapping(
                    parent_source_mappings,
                    rename_map,
                    source_mappings,
                    name,
                    query,
                )
        except Exception:
            pass

    @staticmethod
    def _inject_single_parent_join_mapping(
            parent_source_mappings: Dict[str, str],
            rename_map: Dict[str, str],
            source_mappings: Dict[str, str],
            name: str,
            query: str,
    ) -> None:
        """
        Injects one parent join attribute into the parent source mappings, renaming it if needed.
        :param parent_source_mappings: Parent-side source mappings to update.
        :param rename_map: Mapping of original parent names to renamed attributes.
        :param source_mappings: Child-side source mappings.
        :param name: Parent-side join attribute name.
        :param query: Parent-side extraction query.
        :return: None
        """
        if not name:
            return

        effective_query = query if query else name
        if name in source_mappings:
            new_name = f"parent_{name}"
            parent_source_mappings[new_name] = effective_query
            rename_map[name] = new_name
            if name in parent_source_mappings:
                try:
                    del parent_source_mappings[name]
                except Exception:
                    pass
            return

        if name not in parent_source_mappings:
            parent_source_mappings[name] = effective_query

    @staticmethod
    def _rename_parent_mapping_collisions(
            parent_source_mappings: Dict[str, str],
            rename_map: Dict[str, str],
            source_mappings: Dict[str, str],
    ) -> None:
        """
        Renames parent mappings that would collide with child-side attribute names.
        :param parent_source_mappings: Parent-side source mappings to update.
        :param rename_map: Mapping of original parent names to renamed attributes.
        :param source_mappings: Child-side source mappings.
        :return: None
        """
        try:
            for key in list(parent_source_mappings.keys()):
                if key in source_mappings and not str(key).startswith("parent_"):
                    new_name = f"parent_{key}"
                    parent_source_mappings[new_name] = parent_source_mappings[key]
                    del parent_source_mappings[key]
                    rename_map[key] = new_name
        except Exception:
            pass

    def _build_parent_subject_expr(self, parent_tm: Node, rename_map: Dict[str, str]):
        """
        Builds the parent subject expression used as the object of a referencing object map.
        :param parent_tm: Parent TriplesMap node.
        :param rename_map: Mapping of original parent names to renamed attributes.
        :return: Parent subject expression, or None if no subject map exists.
        """
        parent_sm = self.graph.value(parent_tm, RR.subjectMap)
        if not parent_sm:
            return None

        phi_parent_sbj = self._create_ext_expr(parent_sm, default_term_type="IRI")
        if rename_map and phi_parent_sbj is not None:
            try:
                self._rename_reference_attributes(phi_parent_sbj, rename_map)
            except Exception:
                pass
        return phi_parent_sbj

    def _rename_reference_attributes(self, expr: Expression, rename_map: Dict[str, str]) -> None:
        """
        Recursively rewrites reference attribute names inside an expression tree.
        :param expr: Expression tree to update.
        :param rename_map: Mapping of original attribute names to renamed attributes.
        :return: None
        """
        from pyhartig.expressions.Reference import Reference as ReferenceExpr
        from pyhartig.expressions.FunctionCall import FunctionCall as FunctionCallExpr

        if isinstance(expr, ReferenceExpr):
            if expr.attribute_name in rename_map:
                expr.attribute_name = rename_map[expr.attribute_name]
            return

        if isinstance(expr, FunctionCallExpr):
            for arg in expr.arguments:
                self._rename_reference_attributes(arg, rename_map)

    def _prepare_child_join_mappings(
            self,
            source_mappings: Dict[str, str],
            om: Node,
            child_attrs: List[str],
            child_queries: List[str],
            nojoin_parent_fallback: bool,
    ) -> Dict[str, str]:
        """
        Builds the child-side attribute mappings needed to evaluate a join.
        :param source_mappings: Child-side source mappings.
        :param om: Referencing object map node.
        :param child_attrs: Child-side join attribute names.
        :param child_queries: Child-side extraction queries.
        :param nojoin_parent_fallback: Whether normalization converted a no-join parent mapping.
        :return: Child-side source mappings for the join source.
        """
        child_join_mappings = dict(source_mappings)
        self._strip_nojoin_child_mappings(child_join_mappings, om, child_attrs, nojoin_parent_fallback)
        self._add_missing_child_join_mappings(child_join_mappings, child_attrs, child_queries)
        return child_join_mappings

    def _strip_nojoin_child_mappings(
            self,
            child_join_mappings: Dict[str, str],
            om: Node,
            child_attrs: List[str],
            nojoin_parent_fallback: bool,
    ) -> None:
        """
        Removes child mappings that were synthesized from a no-join parent fallback object map.
        :param child_join_mappings: Child-side source mappings to update.
        :param om: Referencing object map node.
        :param child_attrs: Child-side join attribute names.
        :param nojoin_parent_fallback: Whether normalization converted a no-join parent mapping.
        :return: None
        """
        import re

        if not nojoin_parent_fallback or child_attrs:
            return

        om_ref = self.graph.value(om, RML.reference)
        if om_ref:
            match = re.search(r"[A-Za-z_][A-Za-z0-9_\-.]*", str(om_ref))
            if match:
                child_join_mappings.pop(match.group(0), None)

        om_template = self.graph.value(om, RR.template)
        if not om_template:
            return

        for var in self._extract_single_brace_variables(str(om_template)):
            child_join_mappings.pop(var, None)

    @staticmethod
    def _add_missing_child_join_mappings(
            child_join_mappings: Dict[str, str],
            child_attrs: List[str],
            child_queries: List[str],
    ) -> None:
        """
        Ensures that all child join attributes are present in the child-side source mappings.
        :param child_join_mappings: Child-side source mappings to update.
        :param child_attrs: Child-side join attribute names.
        :param child_queries: Child-side extraction queries.
        :return: None
        """
        for name, query in zip(child_attrs, child_queries):
            if not name or name in child_join_mappings:
                continue
            child_join_mappings[name] = query if query else name

    def _build_regular_object_map_branch(self, base_operator: Operator, om: Node, phi_pred: Expression) -> Operator:
        """
        Builds the operator branch for a non-referencing object map.
        :param base_operator: Base operator that already produces the subject.
        :param om: Object map node.
        :param phi_pred: Predicate expression.
        :return: Branch producing subject, predicate and object attributes.
        """
        om_template = self.graph.value(om, RR.template)
        if om_template:
            tstr = str(om_template)
            prefix = tstr.split('{', 1)[0]
            default_term_type = "IRI" if prefix and urlsplit(prefix).scheme else "Literal"
        else:
            default_term_type = "Literal"

        phi_obj = self._create_ext_expr(om, default_term_type=default_term_type)
        branch = ExtendOperator(base_operator, "predicate", phi_pred)
        return ExtendOperator(branch, "object", phi_obj)

    def _apply_graph_and_project(self, branch: Operator, pom: Node, sm: Node, tm: Node) -> Operator:
        """
        Adds the graph term to a branch and projects the final quad attributes.
        :param branch: Branch to extend.
        :param pom: Predicate-object map node.
        :param sm: Subject map node.
        :param tm: TriplesMap node.
        :return: Final projected branch.
        """
        gm_node = self.graph.value(pom, RR.graphMap) or (sm and self.graph.value(sm, RR.graphMap))
        if gm_node:
            try:
                phi_graph = self._create_ext_expr(gm_node, default_term_type="IRI")
                branch = ExtendOperator(branch, "graph", phi_graph)
            except Exception as e:
                logger.error(f"Failed to create graph extension for POM {pom} in TM {tm}: {e}")
        else:
            branch = ExtendOperator(branch, "graph", Constant(AlgebraIRI(str(RR.defaultGraph))))

        # Try to apply algebraic equivalence: Project^{P U {a}}(Extend_phi^a(r)) = Extend_phi^a(Project^P(r))
        # We conservatively push a Project below an Extend when:
        # - the operator is a single Extend (parent is not another Extend)
        # - the extended attribute is in the final projection
        # - the extension expression only references source attributes (not produced by deeper Extends)
        final_proj = {"subject", "predicate", "object", "graph"}

        def _collect_expr_refs(expr) -> set:
            refs = set()
            from pyhartig.expressions.Reference import Reference as RefExpr
            from pyhartig.expressions.FunctionCall import FunctionCall as FuncExpr
            from pyhartig.expressions.Constant import Constant as ConstExpr

            if isinstance(expr, RefExpr):
                refs.add(expr.attribute_name)
                return refs
            if isinstance(expr, FuncExpr):
                for a in expr.arguments:
                    refs.update(_collect_expr_refs(a))
                return refs
            if isinstance(expr, ConstExpr):
                return refs
            # conservative fallback: no refs discovered
            try:
                # Some expression types may expose subexpressions via .arguments
                args = getattr(expr, "arguments", None)
                if args:
                    for a in args:
                        refs.update(_collect_expr_refs(a))
            except Exception:
                pass
            return refs

        def _parent_has_extends(op: Operator) -> set:
            # collect new_attribute names of any ExtendOperator in the subtree rooted at op
            names = set()
            from pyhartig.operators.ExtendOperator import ExtendOperator as _Ext
            try:
                if isinstance(op, _Ext):
                    names.add(op.new_attribute)
                    names.update(_parent_has_extends(op.parent_operator))
                else:
                    # try to inspect known binary/leaf operators for parents
                    parent = getattr(op, "operator", None) or getattr(op, "parent_operator", None)
                    if parent is not None:
                        names.update(_parent_has_extends(parent))
            except Exception:
                pass
            return names

        try:
            from pyhartig.operators.ExtendOperator import ExtendOperator as _ExtOp
            if isinstance(branch, _ExtOp):
                ext = branch
                # only push if parent is not itself an Extend (conservative)
                if ext.new_attribute in final_proj and not isinstance(ext.parent_operator, _ExtOp):
                    refs = _collect_expr_refs(ext.expression)
                    if refs:
                        parent_ext_names = _parent_has_extends(ext.parent_operator)
                        # ensure expression doesn't depend on attributes produced by deeper Extends
                        if refs.isdisjoint(parent_ext_names):
                            try:
                                branch = ExtendOperator(ProjectOperator(ext.parent_operator, refs), ext.new_attribute,
                                                        ext.expression)
                            except Exception:
                                # if projection fails (e.g., empty/ref mismatch), fall back to original
                                branch = ext
        except Exception:
            # any error in attempting the rewrite should not break parsing
            pass

        return ProjectOperator(branch, final_proj)

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
        queries = {}

        sm = self.graph.value(tm, RR.subjectMap)
        self._scan_term_map_queries(sm, queries)
        # Also scan any subject-level graphMap so variables used to build named graphs
        # get included in attribute mappings
        self._scan_term_map_queries(self.graph.value(sm, RR.graphMap), queries)

        for pom in self.graph.objects(tm, RR.predicateObjectMap):
            self._scan_term_map_queries(self.graph.value(pom, RR.predicateMap), queries)
            self._scan_term_map_queries(self.graph.value(pom, RR.objectMap), queries)
            # Include any graphMap attached to the predicate-object map
            self._scan_term_map_queries(self.graph.value(pom, RR.graphMap), queries)

        return queries

    @staticmethod
    def _normalize_query_name(value: str) -> str:
        """
        Normalizes a reference or join operand to its attribute identifier.
        :param value: Raw reference string.
        :return: Extracted attribute identifier when possible.
        """
        import re

        if not value:
            return value
        match = re.search(r"[A-Za-z_][A-Za-z0-9_\-.]*", value)
        if match:
            return match.group(0)
        return value

    def _scan_term_map_queries(self, term_map: Node, queries: Dict[str, str]) -> None:
        """
        Collects reference and template-derived extraction queries from a term map.
        :param term_map: Term map node to inspect.
        :param queries: Output dictionary of attribute mappings.
        :return: None
        """
        if not term_map:
            return

        ref = self.graph.value(term_map, RML.reference)
        if ref:
            ref_str = str(ref)
            queries[self._normalize_query_name(ref_str)] = ref_str

        tmpl = self.graph.value(term_map, RR.template)
        if tmpl:
            self._register_template_query_variables(str(tmpl), queries)

    def _register_template_query_variables(self, template_value: str, queries: Dict[str, str]) -> None:
        """
        Registers template variables as extraction queries in source mappings.
        :param template_value: Raw rr:template string.
        :param queries: Output dictionary of attribute mappings.
        :return: None
        """
        tmpl_str = template_value.replace('\\{', '{').replace('\\}', '}')
        template_vars = self._extract_single_brace_variables(tmpl_str)
        for value in template_vars:
            # If the template variable is a dotted path (e.g. "info.key"),
            # keep it as a bracketed JSONPath lookup so it resolves against
            # the iterator context (nested access). If it's a simple
            # identifier, use the short `$.name` JSONPath. For other complex
            # names (containing spaces or special characters) use a
            # bracketed lookup with proper escaping.
            safe_v = value.replace("'", "\\'")
            if "." in value:
                queries[value] = f"$['{safe_v}']"
            elif self._is_simple_identifier(value):
                queries[value] = f"$.{value}"
            else:
                queries[value] = f"$['{safe_v}']"

    def _extract_join_attributes(self, object_map: Node):
        """
        Extract lists of join attribute names and their extraction queries from rr:joinCondition nodes attached to an object map.
        Returns (child_attrs, parent_attrs, child_queries, parent_queries) where each is a list.
        """
        child_attrs = []
        parent_attrs = []
        child_queries = []
        parent_queries = []

        for jc in self.graph.objects(object_map, RR.joinCondition):
            parent = self.graph.value(jc, RR.parent)
            child = self.graph.value(jc, RR.child)
            p_name, p_q = self._extract_join_operand(parent)
            c_name, c_q = self._extract_join_operand(child)
            self._append_join_attribute_pair(
                child_attrs,
                parent_attrs,
                child_queries,
                parent_queries,
                c_name,
                p_name,
                c_q,
                p_q,
            )

        return child_attrs, parent_attrs, child_queries, parent_queries

    def _extract_join_operand(self, node):
        """
        Extracts the attribute name and query associated with one join operand node.
        :param node: rr:child or rr:parent operand node.
        :return: Tuple of normalized attribute name and extraction query.
        """
        if node is None:
            return None, None
        try:
            ref = self.graph.value(node, RML.reference)
            if ref:
                query = str(ref)
                return self._normalize_query_name(query), query

            tmpl = self.graph.value(node, RR.template)
            if tmpl:
                template_vars = self._extract_single_brace_variables(str(tmpl))
                if len(template_vars) == 1:
                    value = template_vars[0]
                    if self._is_simple_identifier(value):
                        return value, f"$.{value}"
                    safe_v = value.replace("'", "\\'")
                    return value, f"$['{safe_v}']"
                return str(tmpl), None
        except Exception:
            pass
        return str(node), None

    @staticmethod
    def _append_join_attribute_pair(
            child_attrs: List[str],
            parent_attrs: List[str],
            child_queries: List[str],
            parent_queries: List[str],
            child_name,
            parent_name,
            child_query,
            parent_query,
    ) -> None:
        """
        Appends one child/parent join attribute pair and their extraction queries.
        :param child_attrs: Child-side attribute names.
        :param parent_attrs: Parent-side attribute names.
        :param child_queries: Child-side extraction queries.
        :param parent_queries: Parent-side extraction queries.
        :param child_name: Child-side attribute name.
        :param parent_name: Parent-side attribute name.
        :param child_query: Child-side extraction query.
        :param parent_query: Parent-side extraction query.
        :return: None
        """
        if not child_name or not parent_name:
            return
        child_attrs.append(child_name)
        parent_attrs.append(parent_name)
        child_queries.append(child_query)
        parent_queries.append(parent_query)

    def _create_ext_expr(self, term_map: Node, default_term_type: str = "Literal") -> Expression:
        """
        Creates an extension expression from a term map.
        :param term_map: The term map node.
        :param default_term_type: The default term type if none is specified.
        :return: An Expression representing the term map.
        """
        fnml_expr = self._create_fnml_ext_expr(term_map)
        if fnml_expr is not None:
            return fnml_expr

        const_expr = self._create_constant_ext_expr(term_map)
        if const_expr is not None:
            return const_expr

        ref_expr = self._create_reference_ext_expr(term_map, default_term_type)
        if ref_expr is not None:
            return ref_expr

        template_expr = self._create_template_ext_expr(term_map, default_term_type)
        if template_expr is not None:
            return template_expr

        return Constant(AlgebraIRI(PYHARTIG_ERROR_URI))

    def _create_fnml_ext_expr(self, term_map: Node):
        """
        Builds an expression for an FnML function-valued term map.
        :param term_map: Term map node.
        :return: FunctionCall expression or None when the term map is not FnML-based.
        """
        try:
            fn_node = self.graph.value(term_map, FNML.functionValue)
        except Exception:
            fn_node = None
        if fn_node is None:
            return None

        func_uri = self._resolve_fnml_function_uri(fn_node)
        params = self._collect_fnml_params(fn_node)
        if func_uri:
            return FunctionCall(func_uri, params)
        return None

    def _resolve_fnml_function_uri(self, fn_node: Node):
        """
        Resolves the function URI executed by an FnML function value node.
        :param fn_node: FnML function value node.
        :return: Function URI string or None.
        """
        func_uri = None
        try:
            fno_exec_direct = self.graph.value(fn_node, FNO.executes)
            if fno_exec_direct is not None:
                func_uri = str(fno_exec_direct)
        except Exception:
            pass

        for pom in self.graph.objects(fn_node, RR.predicateObjectMap):
            pm = self.graph.value(pom, RR.predicateMap)
            om = self.graph.value(pom, RR.objectMap)
            if pm is None or om is None:
                continue
            pm_const = self.graph.value(pm, RR.constant)
            if pm_const == FNO.executes or (isinstance(pm_const, URIRef) and str(pm_const).endswith('#executes')):
                f_const = self.graph.value(om, RR.constant)
                if f_const is not None:
                    func_uri = str(f_const)
        return func_uri

    def _collect_fnml_params(self, fn_node: Node):
        """
        Collects and orders FnML function parameters from a function value node.
        :param fn_node: FnML function value node.
        :return: Ordered list of parameter expressions.
        """
        params = []
        for pom in self.graph.objects(fn_node, RR.predicateObjectMap):
            param = self._extract_fnml_param(pom)
            if param is not None:
                params.append(param)
        return [expr for _, expr in sorted(params, key=self._fnml_param_sort_key)]

    def _extract_fnml_param(self, pom: Node):
        """
        Extracts one FnML parameter expression from a predicate-object map.
        :param pom: Predicate-object map attached to the function value node.
        :return: Tuple of parameter predicate and expression, or None.
        """
        pm = self.graph.value(pom, RR.predicateMap)
        om = self.graph.value(pom, RR.objectMap)
        if pm is None or om is None:
            return None

        pm_const = self.graph.value(pm, RR.constant)
        if pm_const == FNO.executes or (isinstance(pm_const, URIRef) and str(pm_const).endswith('#executes')):
            return None

        try:
            arg_expr = self._create_ext_expr(om, default_term_type="Literal")
        except Exception:
            arg_expr = Constant(AlgebraLiteral(""))
        return pm_const, arg_expr

    @staticmethod
    def _fnml_param_sort_key(item):
        """
        Computes a stable sort key for ordered FnML parameters.
        :param item: Tuple containing the parameter predicate and expression.
        :return: Integer sort key extracted from the predicate suffix.
        """
        key, _ = item
        if key is None:
            return 0
        key_str = str(key)
        suffix_digits: List[str] = []
        for char in reversed(key_str):
            if not char.isdigit():
                break
            suffix_digits.append(char)
        if not suffix_digits:
            return 0
        return int("".join(reversed(suffix_digits)))

    def _create_constant_ext_expr(self, term_map: Node):
        """
        Builds an expression for a constant-valued term map.
        :param term_map: Term map node.
        :return: Constant expression or None when no rr:constant is defined.
        """
        const = self.graph.value(term_map, RR.constant)
        if not const:
            return None
        if isinstance(const, URIRef):
            return Constant(AlgebraIRI(str(const)))
        if isinstance(const, RDFLiteral):
            if const.language:
                return Constant(AlgebraLiteral(str(const), language=str(const.language)))
            if const.datatype:
                return Constant(AlgebraLiteral(str(const), datatype_iri=str(const.datatype)))
        return Constant(AlgebraLiteral(str(const)))

    def _create_reference_ext_expr(self, term_map: Node, default_term_type: str):
        """
        Builds an expression for a reference-valued term map.
        :param term_map: Term map node.
        :param default_term_type: Fallback term type when rr:termType is absent.
        :return: Expression for the reference value, or None.
        """
        ref = self.graph.value(term_map, RML.reference)
        if not ref:
            return None

        term_type, lang_node, datatype_node = self._resolve_term_map_type_info(term_map, default_term_type)
        ref_expr = Reference(self._normalize_query_name(str(ref)))
        if term_type == RR.IRI:
            return self._build_reference_iri_expr(ref_expr)
        if term_type == RR.BlankNode:
            return FunctionCall(to_bnode, [ref_expr])
        return self._build_literal_expr(ref_expr, datatype_node, lang_node)

    def _create_template_ext_expr(self, term_map: Node, default_term_type: str):
        """
        Builds an expression for a template-valued term map.
        :param term_map: Term map node.
        :param default_term_type: Fallback term type when rr:termType is absent.
        :return: Expression for the template value, or None.
        """
        tmpl = self.graph.value(term_map, RR.template)
        if not tmpl:
            return None

        concat_expr = self._build_template_concat_expr(str(tmpl))
        term_type, lang_node, datatype_node = self._resolve_term_map_type_info(term_map, default_term_type)
        if term_type == RR.IRI:
            return self._build_template_iri_expr(concat_expr)
        if term_type == RR.BlankNode:
            return FunctionCall(to_bnode, [concat_expr])
        return self._build_literal_expr(concat_expr, datatype_node, lang_node)

    def _resolve_term_map_type_info(self, term_map: Node, default_term_type: str):
        """
        Resolves the effective term type, language and datatype associated with a term map.
        :param term_map: Term map node.
        :param default_term_type: Fallback term type when rr:termType is absent.
        :return: Tuple of effective term type, language node and datatype node.
        """
        term_type = self.graph.value(term_map, RR.termType)
        if term_type is None:
            term_type = RR[default_term_type]

        if default_term_type == "IRI" and term_type == RR.Literal:
            raise ValueError("InvalidRulesError: rr:subjectMap cannot have rr:termType rr:Literal")

        lang_node = self.graph.value(term_map, RR.language)
        datatype_node = self.graph.value(term_map, RR.datatype)
        if lang_node is not None and datatype_node is not None:
            raise ValueError(
                "InvalidRulesError: rr:language and rr:datatype cannot both be specified on the same term map")
        return term_type, lang_node, datatype_node

    def _build_reference_iri_expr(self, ref_expr: Reference):
        """
        Wraps a reference expression in the builtin IRI constructor.
        :param ref_expr: Reference expression to convert to an IRI.
        :return: FunctionCall building the final IRI.
        """
        if self.base_iri:
            return FunctionCall(to_iri, [ref_expr, Constant(self.base_iri), Constant(False)])
        return FunctionCall(to_iri, [ref_expr, Constant(None), Constant(False)])

    def _build_template_concat_expr(self, template_str: str):
        """
        Converts an rr:template string into a concatenation expression.
        :param template_str: Raw template string.
        :return: Concatenation expression for the template expansion.
        """
        template_str = template_str.replace('\\{', '{').replace('\\}', '}')
        parts = self._iter_template_segments(template_str)
        args = []
        for part_type, part_value in parts:
            if part_type == "var":
                args.append(FunctionCall(percent_encode_component, [Reference(part_value)]))
            elif part_value:
                args.append(Constant(AlgebraLiteral(part_value)))
        if not args:
            return Constant(AlgebraLiteral(""))
        return FunctionCall(concat, args)

    def _build_template_iri_expr(self, concat_expr: Expression):
        """
        Wraps a template concatenation expression in the builtin IRI constructor.
        :param concat_expr: Concatenation expression produced from an rr:template.
        :return: FunctionCall building the final IRI.
        """
        if self.base_iri:
            return FunctionCall(to_iri, [concat_expr, Constant(self.base_iri)])
        return FunctionCall(to_iri, [concat_expr])

    def _build_literal_expr(self, value_expr: Expression, datatype_node, lang_node):
        """
        Wraps an expression in the appropriate literal constructor according to datatype or language.
        :param value_expr: Expression producing the lexical form.
        :param datatype_node: Optional datatype node.
        :param lang_node: Optional language tag node.
        :return: FunctionCall building the final literal.
        """
        if datatype_node is not None:
            if not isinstance(datatype_node, URIRef):
                raise ValueError(f"Invalid rr:datatype value: {datatype_node}")
            return FunctionCall(to_literal, [value_expr, Constant(str(datatype_node))])
        if lang_node is not None:
            lang_raw = self._validated_language_tag(lang_node)
            return FunctionCall(to_literal_lang, [value_expr, Constant(lang_raw)])
        return FunctionCall(to_literal, [value_expr, Constant(str(XSD.string))])

    @staticmethod
    def _validated_language_tag(lang_node) -> str:
        """
        Validates a language tag by attempting to construct an RDF literal with it.
        :param lang_node: Language tag node.
        :return: Normalized language tag string.
        """
        lang_raw = str(lang_node).strip()
        from pyhartig.algebra.Terms import Literal as _AlgebraLiteral
        try:
            _AlgebraLiteral("x", language=lang_raw)
        except Exception:
            raise ValueError(f"Invalid rr:language value: {lang_raw}")
        return lang_raw

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
        :return: None
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
