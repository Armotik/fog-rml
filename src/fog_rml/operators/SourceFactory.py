import logging
import urllib.parse
from pathlib import Path
from rdflib import Graph, Namespace, URIRef, Node

from fog_rml.operators.Operator import Operator
from fog_rml.operators.sources.JsonSourceOperator import JsonSourceOperator
from fog_rml.operators.sources.CsvSourceOperator import CsvSourceOperator
from fog_rml.operators.sources.XmlSourceOperator import XmlSourceOperator
from fog_rml.operators.sources.SparqlSourceOperator import SparqlSourceOperator
from fog_rml.operators.sources.MysqlSourceOperator import MysqlSourceOperator
from fog_rml.operators.sources.PostgresqlSourceOperator import PostgresqlSourceOperator
from fog_rml.operators.sources.SqlserverSourceOperator import SqlserverSourceOperator
from fog_rml.namespaces import D2RQ_BASE, QL_BASE, RML_BASE, RR_BASE, SD_BASE

logger = logging.getLogger(__name__)

RML = Namespace(RML_BASE)
QL = Namespace(QL_BASE)
RR = Namespace(RR_BASE)
SD = Namespace(SD_BASE)
D2RQ = Namespace(D2RQ_BASE)


class SourceFactory:
    """
    Factory class to instantiate the appropriate SourceOperator based on the
    rml:referenceFormulation defined in the Logical Source.
    """

    @staticmethod
    def create_source_operator(graph: Graph, logical_source_node: Node, mapping_dir: Path,
                               attribute_mappings: dict) -> Operator:
        """
        Analyzes the Logical Source node and returns the correct SourceOperator.
        :param graph: RDFLib Graph containing the mapping.
        :param logical_source_node: Node representing the Logical Source.
        :param mapping_dir: Directory path of the mapping file for resolving relative paths.
        :param attribute_mappings: Dictionary of attribute mappings for the source.
        :return: An instance of the appropriate SourceOperator.
        :raises ValueError: If the reference formulation is unsupported.
        """
        source_file, iterator, ref_formulation = SourceFactory._extract_source_metadata(graph, logical_source_node)
        _, src_path, is_uri_like_source = SourceFactory._resolve_source_path(source_file, mapping_dir)
        SourceFactory._log_source_resolution(mapping_dir, source_file, src_path)
        src_path = SourceFactory._resolve_missing_source_path(
            source_file,
            mapping_dir,
            src_path,
            is_uri_like_source,
        )

        sparql_source = SourceFactory._create_sparql_source_if_applicable(
            graph,
            logical_source_node,
            source_file,
            iterator,
            attribute_mappings,
            mapping_dir,
        )
        if sparql_source is not None:
            return sparql_source

        database_source = SourceFactory._create_database_source_if_applicable(
            graph,
            logical_source_node,
            source_file,
            iterator,
            attribute_mappings,
            mapping_dir,
        )
        if database_source is not None:
            return database_source

        factory = SourceFactory._get_reference_formulation_factory(ref_formulation)
        return factory(src_path, iterator, attribute_mappings)

    @staticmethod
    def _extract_source_metadata(graph: Graph, logical_source_node: Node):
        """
        Extracts the logical-source metadata needed to create a source operator.
        :param graph: RDFLib Graph containing the mapping.
        :param logical_source_node: Node representing the Logical Source.
        :return: Tuple of source node, iterator node, and normalized reference formulation.
        """
        source_file = graph.value(logical_source_node, RML.source) or graph.value(logical_source_node, RML.reference)
        iterator = graph.value(logical_source_node, RML.iterator)
        ref_formulation = graph.value(logical_source_node, RML.referenceFormulation)
        return source_file, iterator, SourceFactory._normalize_ref_formulation(ref_formulation)

    @staticmethod
    def _normalize_ref_formulation(ref_formulation):
        """
        Normalizes a reference formulation literal to a URIRef when needed.
        :param ref_formulation: Raw reference formulation node.
        :return: Normalized reference formulation node.
        """
        try:
            from rdflib import URIRef as _URIRef
            if ref_formulation is not None and not isinstance(ref_formulation, _URIRef):
                return _URIRef(str(ref_formulation))
        except Exception:
            pass
        return ref_formulation

    @staticmethod
    def _resolve_source_path(source_file, mapping_dir: Path):
        """
        Resolves a source literal to a candidate local path and URI-like status.
        :param source_file: Source node extracted from the mapping.
        :param mapping_dir: Directory path of the mapping file.
        :return: Tuple of source literal string, candidate path, and URI-like flag.
        """
        source_str = str(source_file) if source_file is not None else ""
        parsed_source = urllib.parse.urlparse(source_str)
        is_uri_like_source = bool(parsed_source.scheme in ("http", "https", "jdbc", "mysql", "postgres"))

        src_path = Path(source_str) if source_str else mapping_dir
        if not src_path.is_absolute() and source_str:
            src_path = mapping_dir / src_path
        return source_str, src_path, is_uri_like_source

    @staticmethod
    def _log_source_resolution(mapping_dir: Path, source_file, src_path: Path) -> None:
        """
        Logs the resolved source path for debugging purposes.
        :param mapping_dir: Directory path of the mapping file.
        :param source_file: Source node extracted from the mapping.
        :param src_path: Candidate resolved path.
        :return: None
        """
        try:
            logger.debug(
                "SourceFactory: mapping_dir=%s, source_literal=%s, resolved_path=%s",
                mapping_dir,
                source_file,
                src_path,
            )
        except Exception:
            pass

    @staticmethod
    def _resolve_missing_source_path(source_file, mapping_dir: Path, src_path: Path, is_uri_like_source: bool) -> Path:
        """
        Applies fallback path resolution when the initial source path does not exist.
        :param source_file: Source node extracted from the mapping.
        :param mapping_dir: Directory path of the mapping file.
        :param src_path: Candidate resolved path.
        :param is_uri_like_source: Whether the source should be treated as a URI-like source.
        :return: Best-effort resolved path.
        """
        if is_uri_like_source or src_path.exists():
            return src_path

        cwd_candidate = Path(str(source_file))
        if cwd_candidate.exists():
            logger.debug("Found source file in CWD fallback: %s", cwd_candidate)
            return cwd_candidate

        try:
            return SourceFactory._search_source_path(mapping_dir, src_path.name) or src_path
        except Exception:
            return src_path

    @staticmethod
    def _search_source_path(mapping_dir: Path, filename: str) -> Path | None:
        """
        Searches the mapping directory and its parents for a matching source filename.
        :param mapping_dir: Directory path of the mapping file.
        :param filename: Filename to search for.
        :return: Located path, or None.
        """
        search_dirs = [mapping_dir] + list(mapping_dir.parents)
        for search_dir in search_dirs:
            for path in search_dir.rglob(filename):
                logger.debug("Located source file by search in %s: %s", search_dir, path)
                return path
        return None

    @staticmethod
    def _create_sparql_source_if_applicable(
            graph: Graph,
            logical_source_node: Node,
            source_file,
            iterator,
            attribute_mappings: dict,
            mapping_dir: Path,
    ) -> Operator | None:
        """
        Creates a SPARQL source operator when the logical source is backed by an endpoint.
        :param graph: RDFLib Graph containing the mapping.
        :param logical_source_node: Node representing the Logical Source.
        :param source_file: Source node extracted from the mapping.
        :param iterator: Iterator node extracted from the mapping.
        :param attribute_mappings: Dictionary of attribute mappings for the source.
        :param mapping_dir: Directory path of the mapping file.
        :return: SparqlSourceOperator instance, or None.
        """
        if graph is None:
            return None

        endpoint = graph.value(source_file, SD.endpoint)
        if endpoint is None:
            endpoint = SourceFactory._lookup_endpoint_by_uri(graph, source_file)

        query_literal = graph.value(logical_source_node, RML.query)
        if not endpoint or not query_literal:
            return None

        logger.debug("SPARQL logical source detected: endpoint=%s", endpoint)
        query = str(iterator) if iterator else "$.results.bindings[*]"
        return SparqlSourceOperator(
            endpoint=str(endpoint),
            sparql_query=str(query_literal),
            iterator_query=query,
            attribute_mappings=attribute_mappings,
            mapping_dir=mapping_dir,
            source_node=str(source_file) if source_file is not None else None,
        )

    @staticmethod
    def _lookup_endpoint_by_uri(graph: Graph, source_file):
        """
        Resolves an endpoint when the source node needs to be coerced to a URIRef.
        :param graph: RDFLib Graph containing the mapping.
        :param source_file: Source node extracted from the mapping.
        :return: Endpoint node, or None.
        """
        try:
            from rdflib import URIRef
            return graph.value(URIRef(str(source_file)), SD.endpoint)
        except Exception:
            return None

    @staticmethod
    def _create_database_source_if_applicable(
            graph: Graph,
            logical_source_node: Node,
            source_file,
            iterator,
            attribute_mappings: dict,
            mapping_dir: Path,
    ) -> Operator | None:
        """
        Creates a relational database source operator when the logical source describes one.
        :param graph: RDFLib Graph containing the mapping.
        :param logical_source_node: Node representing the Logical Source.
        :param source_file: Source node extracted from the mapping.
        :param iterator: Iterator node extracted from the mapping.
        :param attribute_mappings: Dictionary of attribute mappings for the source.
        :param mapping_dir: Directory path of the mapping file.
        :return: Database source operator, or None.
        """
        db_metadata = SourceFactory._extract_database_metadata(graph, logical_source_node, source_file)
        if db_metadata is None:
            return None

        source_class = SourceFactory._detect_database_source_class(
            db_metadata["jdbc_driver"],
            db_metadata["dsn"],
        )
        if source_class is None:
            return None

        return source_class(
            dsn=db_metadata["dsn"],
            iterator_query=str(iterator) if iterator else "$",
            attribute_mappings=attribute_mappings,
            query=db_metadata["query"],
            table_name=db_metadata["table_name"],
            username=db_metadata["username"],
            password=db_metadata["password"],
            mapping_dir=mapping_dir,
        )

    @staticmethod
    def _extract_database_metadata(graph: Graph, logical_source_node: Node, source_file):
        """
        Extracts JDBC-style metadata for relational logical sources.
        :param graph: RDFLib Graph containing the mapping.
        :param logical_source_node: Node representing the Logical Source.
        :param source_file: Source node extracted from the mapping.
        :return: Dictionary of database metadata.
        """
        db_node = source_file
        jdbc_driver = graph.value(db_node, D2RQ.jdbcDriver)
        jdbc_dsn = graph.value(db_node, D2RQ.jdbcDSN)
        username = graph.value(db_node, D2RQ.username)
        password = graph.value(db_node, D2RQ.password)

        return {
            "jdbc_driver": jdbc_driver,
            "dsn": str(jdbc_dsn) if jdbc_dsn is not None else "",
            "username": str(username) if username is not None else None,
            "password": str(password) if password is not None else None,
            "query": str(graph.value(logical_source_node, RML.query)) if graph.value(logical_source_node, RML.query) is not None else None,
            "table_name": str(graph.value(logical_source_node, RR.tableName)) if graph.value(logical_source_node, RR.tableName) is not None else None,
        }

    @staticmethod
    def _detect_database_source_class(jdbc_driver, dsn_str: str):
        """
        Detects the database source operator class matching a JDBC driver or DSN.
        :param jdbc_driver: JDBC driver node.
        :param dsn_str: JDBC DSN string.
        :return: Database source operator class, or None.
        """
        driver_str = str(jdbc_driver).lower() if jdbc_driver is not None else ""
        if (
            "mysql" in driver_str
            or dsn_str.startswith("mysql://")
            or dsn_str.startswith("jdbc:mysql://")
            or (jdbc_driver is not None and "com.mysql" in str(jdbc_driver))
        ):
            return MysqlSourceOperator
        if (
            "postgresql" in driver_str
            or dsn_str.startswith("postgresql://")
            or dsn_str.startswith("jdbc:postgresql://")
            or (jdbc_driver is not None and "org.postgresql" in str(jdbc_driver))
        ):
            return PostgresqlSourceOperator
        if (
            "sqlserver" in driver_str
            or "microsoft" in driver_str
            or dsn_str.startswith("sqlserver://")
            or dsn_str.startswith("jdbc:sqlserver://")
            or (jdbc_driver is not None and "com.microsoft.sqlserver" in str(jdbc_driver))
        ):
            return SqlserverSourceOperator
        return None

    @staticmethod
    def _get_reference_formulation_factory(ref_formulation):
        """
        Returns the source factory method matching a reference formulation.
        :param ref_formulation: Normalized reference formulation node.
        :return: Factory function creating the source operator.
        :raises ValueError: If the reference formulation is unsupported.
        """
        registry = {
            QL.JSONPath: SourceFactory._create_json_source,
            QL.CSV: SourceFactory._create_csv_source,
            QL.XPath: SourceFactory._create_xml_source,
        }
        if ref_formulation is None:
            return SourceFactory._create_json_source

        factory = registry.get(ref_formulation)
        if factory is None:
            raise ValueError(f"Unsupported reference formulation: {ref_formulation}")
        return factory

    @staticmethod
    def _create_json_source(path: Path, iterator: Node, mappings: dict) -> JsonSourceOperator:
        """
        Creates a JsonSourceOperator for the given JSON file path and iterator.
        :param path: Path to the JSON source file.
        :param iterator: RDFLib Node representing the JSONPath iterator.
        :param mappings: Attribute mappings for the source.
        :return: An instance of JsonSourceOperator.
        """
        query = str(iterator) if iterator else "$"

        try:
            logger.debug(f"Loading JSON source file: {path}")
            return JsonSourceOperator.from_json_file(
                source_path=path,
                iterator_query=query,
                attribute_mappings=mappings,
            )
        except FileNotFoundError:
            logger.error(f"Source file not found at: {path}")
            raise
        except Exception as e:
            logger.error(f"Error loading JSON source {path}: {e}")
            raise

    @staticmethod
    def _create_csv_source(path: Path, iterator: Node, mappings: dict) -> CsvSourceOperator:
        """
        Creates a CsvSourceOperator for the given CSV file path and iterator.
        :param path: Path to the CSV source file.
        :param iterator: RDFLib Node representing the iterator.
        :param mappings: Attribute mappings for the source.
        :return: An instance of CsvSourceOperator.
        """
        query = str(iterator) if iterator else "$"

        try:
            logger.debug(f"Loading CSV source file: {path}")
            if not path.is_file():
                raise FileNotFoundError(path)
        except FileNotFoundError:
            logger.error(f"Source file not found at: {path}")
            raise
        except Exception as e:
            logger.error(f"Error loading CSV source {path}: {e}")
            raise

        return CsvSourceOperator(source_data=path, iterator_query=query, attribute_mappings=mappings)

    @staticmethod
    def _create_xml_source(path: Path, iterator: Node, mappings: dict) -> XmlSourceOperator:
        """
        Creates an XmlSourceOperator for the given XML file path and iterator.
        :param path: Path to the XML source file.
        :param iterator: RDFLib Node representing the XPath iterator.
        :param mappings: Attribute mappings for the source.
        :return: An instance of XmlSourceOperator.
        """
        query = str(iterator) if iterator else "."

        try:
            logger.debug(f"Loading XML source file: {path}")
            import xml.etree.ElementTree as ET
            tree = ET.parse(path)
            raw_data = tree.getroot()
        except FileNotFoundError:
            logger.error(f"Source file not found at: {path}")
            raise
        except Exception as e:
            logger.error(f"Error loading XML source {path}: {e}")
            raise

        return XmlSourceOperator(source_data=raw_data, iterator_query=query, attribute_mappings=mappings)

