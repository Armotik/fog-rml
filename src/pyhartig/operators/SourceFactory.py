import logging
import urllib.parse
from pathlib import Path
from rdflib import Graph, Namespace, URIRef, Node

from pyhartig.operators.Operator import Operator
from pyhartig.operators.sources.JsonSourceOperator import JsonSourceOperator
from pyhartig.operators.sources.CsvSourceOperator import CsvSourceOperator
from pyhartig.operators.sources.XmlSourceOperator import XmlSourceOperator
from pyhartig.operators.sources.SparqlSourceOperator import SparqlSourceOperator
from pyhartig.operators.sources.MysqlSourceOperator import MysqlSourceOperator
from pyhartig.operators.sources.PostgresqlSourceOperator import PostgresqlSourceOperator
from pyhartig.operators.sources.SqlserverSourceOperator import SqlserverSourceOperator
from pyhartig.namespaces import D2RQ_BASE, QL_BASE, RML_BASE, RR_BASE, SD_BASE

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
        :param graph: RDFLib Graph containing the mapping
        :param logical_source_node: Node representing the Logical Source
        :param mapping_dir: Directory path of the mapping file for resolving relative paths
        :param attribute_mappings: Dictionary of attribute mappings for the source
        :return: An instance of the appropriate SourceOperator
        :raises ValueError: If the reference formulation is unsupported
        """

        # 1. Extract Metadata
        # Prefer rml:source but allow rml:reference (common in some RML variants/tests)
        source_file = graph.value(logical_source_node, RML.source) or graph.value(logical_source_node, RML.reference)
        iterator = graph.value(logical_source_node, RML.iterator)
        ref_formulation = graph.value(logical_source_node, RML.referenceFormulation)
        # Normalize reference formulation: mapping files sometimes use string literals
        # for the referenceFormulation (e.g. "http://semweb.mmlab.be/ns/ql#JSONPath").
        # Convert those to URIRef so they match the registry keys below.
        try:
            from rdflib import URIRef as _URIRef
            if ref_formulation is not None and not isinstance(ref_formulation, _URIRef):
                ref_formulation = _URIRef(str(ref_formulation))
        except Exception:
            pass

        # 2. Resolve File Path
        source_str = str(source_file) if source_file is not None else ""
        parsed_source = urllib.parse.urlparse(source_str)
        is_uri_like_source = bool(parsed_source.scheme in ("http", "https", "jdbc", "mysql", "postgres"))

        src_path = Path(source_str) if source_str else mapping_dir
        if not src_path.is_absolute() and source_str:
            src_path = mapping_dir / src_path

        # Log resolution info to aid debugging of incorrect parent/child source resolution
        try:
            logger.debug(f"SourceFactory: mapping_dir={mapping_dir}, source_literal={source_file}, resolved_path={src_path}")
        except Exception:
            pass

        # If the resolved path does not exist, try fallbacks to improve
        # robustness when mappings reference different CWDs or were rewritten.
        if (not is_uri_like_source) and (not src_path.exists()):
            # 1) Try interpreting the literal relative to current working directory
            alt = Path(str(source_file))
            if alt.exists():
                logger.debug(f"Found source file in CWD fallback: {alt}")
                src_path = alt
            else:
                # 2) Try searching under the mapping directory and its parents for a matching filename
                try:
                    found = None
                    # search mapping_dir and upward parents
                    search_dirs = [mapping_dir] + list(mapping_dir.parents)
                    for sd in search_dirs:
                        for p in sd.rglob(src_path.name):
                            logger.debug(f"Located source file by search in {sd}: {p}")
                            found = p
                            break
                        if found:
                            break
                    if found:
                        src_path = found
                except Exception:
                    pass

        # Special-case: SPARQL service logical source (rml:query + sd:endpoint)
        sparql_detected = False
        try:
            if graph is not None:
                svc = graph.value(source_file, SD.endpoint)
                if svc is None:
                    try:
                        from rdflib import URIRef
                        svc = graph.value(URIRef(str(source_file)), SD.endpoint)
                    except Exception:
                        svc = None

                query_literal = graph.value(logical_source_node, RML.query)
                if svc and query_literal:
                    sparql_detected = True
                    logger.debug(f"SPARQL logical source detected: endpoint={svc}")
                    query = str(iterator) if iterator else "$.results.bindings[*]"
                    return SparqlSourceOperator(
                        endpoint=str(svc),
                        sparql_query=str(query_literal),
                        iterator_query=query,
                        attribute_mappings=attribute_mappings,
                        mapping_dir=mapping_dir,
                        source_node=str(source_file) if source_file is not None else None,
                    )
        except Exception:
            if sparql_detected:
                raise

        # Special-case: relational database logical source
        try:
            db_node = source_file
            jdbc_driver = graph.value(db_node, D2RQ.jdbcDriver)
            jdbc_dsn = graph.value(db_node, D2RQ.jdbcDSN)
            username = graph.value(db_node, D2RQ.username)
            password = graph.value(db_node, D2RQ.password)

            query_literal = graph.value(logical_source_node, RML.query)
            table_name = graph.value(logical_source_node, RR.tableName)

            driver_str = str(jdbc_driver).lower() if jdbc_driver is not None else ""
            dsn_str = str(jdbc_dsn) if jdbc_dsn is not None else ""
            is_mysql = (
                "mysql" in driver_str
                or dsn_str.startswith("mysql://")
                or dsn_str.startswith("jdbc:mysql://")
                or (jdbc_driver is not None and "com.mysql" in str(jdbc_driver))
            )
            is_postgresql = (
                "postgresql" in driver_str
                or dsn_str.startswith("postgresql://")
                or dsn_str.startswith("jdbc:postgresql://")
                or (jdbc_driver is not None and "org.postgresql" in str(jdbc_driver))
            )
            is_sqlserver = (
                "sqlserver" in driver_str
                or "microsoft" in driver_str
                or dsn_str.startswith("sqlserver://")
                or dsn_str.startswith("jdbc:sqlserver://")
                or (jdbc_driver is not None and "com.microsoft.sqlserver" in str(jdbc_driver))
            )

            if is_mysql:
                return MysqlSourceOperator(
                    dsn=dsn_str,
                    iterator_query=str(iterator) if iterator else "$",
                    attribute_mappings=attribute_mappings,
                    query=str(query_literal) if query_literal is not None else None,
                    table_name=str(table_name) if table_name is not None else None,
                    username=str(username) if username is not None else None,
                    password=str(password) if password is not None else None,
                    mapping_dir=mapping_dir,
                )
            if is_postgresql:
                return PostgresqlSourceOperator(
                    dsn=dsn_str,
                    iterator_query=str(iterator) if iterator else "$",
                    attribute_mappings=attribute_mappings,
                    query=str(query_literal) if query_literal is not None else None,
                    table_name=str(table_name) if table_name is not None else None,
                    username=str(username) if username is not None else None,
                    password=str(password) if password is not None else None,
                    mapping_dir=mapping_dir,
                )
            if is_sqlserver:
                return SqlserverSourceOperator(
                    dsn=dsn_str,
                    iterator_query=str(iterator) if iterator else "$",
                    attribute_mappings=attribute_mappings,
                    query=str(query_literal) if query_literal is not None else None,
                    table_name=str(table_name) if table_name is not None else None,
                    username=str(username) if username is not None else None,
                    password=str(password) if password is not None else None,
                    mapping_dir=mapping_dir,
                )
        except Exception:
            raise

        # 3. Dispatch based on Reference Formulation using registry
        registry = {
            QL.JSONPath: SourceFactory._create_json_source,
            QL.CSV: SourceFactory._create_csv_source,
            QL.XPath: SourceFactory._create_xml_source,
        }

        # Default to JSON when not specified
        factory = None
        if ref_formulation is None:
            factory = SourceFactory._create_json_source
        else:
            factory = registry.get(ref_formulation)

        if factory is None:
            raise ValueError(f"Unsupported reference formulation: {ref_formulation}")

        return factory(src_path, iterator, attribute_mappings)

    @staticmethod
    def _create_json_source(path: Path, iterator: Node, mappings: dict) -> JsonSourceOperator:
        """
        Creates a JsonSourceOperator for the given JSON file path and iterator.
        :param path: Path to the JSON source file
        :param iterator: RDFLib Node representing the JSONPath iterator
        :param mappings: Attribute mappings for the source
        :return: An instance of JsonSourceOperator
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
        # For CSV, iterator is ignored (we iterate rows). Extraction queries are column names
        query = str(iterator) if iterator else "$"

        try:
            logger.debug(f"Loading CSV source file: {path}")
            import csv
            with open(path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                raw_data = list(reader)
        except FileNotFoundError:
            logger.error(f"Source file not found at: {path}")
            raise
        except Exception as e:
            logger.error(f"Error loading CSV source {path}: {e}")
            raise

        return CsvSourceOperator(source_data=raw_data, iterator_query=query, attribute_mappings=mappings)

    @staticmethod
    def _create_xml_source(path: Path, iterator: Node, mappings: dict) -> XmlSourceOperator:
        # For XML, iterator is an XPath expression; extraction queries are relative XPaths
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
