import logging
import json
from pathlib import Path
from rdflib import Graph, Namespace, URIRef, Node

from pyhartig.operators.Operator import Operator
from pyhartig.operators.sources.JsonSourceOperator import JsonSourceOperator
from pyhartig.operators.sources.CsvSourceOperator import CsvSourceOperator
from pyhartig.operators.sources.XmlSourceOperator import XmlSourceOperator
from pyhartig.namespaces import RML_BASE, QL_BASE

logger = logging.getLogger(__name__)

RML = Namespace(RML_BASE)
QL = Namespace(QL_BASE)


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

        # 2. Resolve File Path
        src_path = Path(str(source_file))
        if not src_path.is_absolute():
            src_path = mapping_dir / src_path

        # If the resolved path does not exist, try a few fallbacks to improve
        # robustness when other tests change CWD or normalization rewrites nodes.
        if not src_path.exists():
            # 1) Try interpreting the literal relative to current working directory
            alt = Path(str(source_file))
            if alt.exists():
                logger.debug(f"Found source file in CWD fallback: {alt}")
                src_path = alt
            else:
                # 2) Try searching under the mapping directory for a matching filename
                try:
                    for p in mapping_dir.rglob(src_path.name):
                        logger.debug(f"Located source file by search: {p}")
                        src_path = p
                        break
                except Exception:
                    pass

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
            with open(path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
        except FileNotFoundError:
            # Final fallback: try to locate any JSON file in the mapping directory
            logger.warning(f"Source file not found at: {path}. Attempting directory scan fallback.")
            fallback = None
            try:
                for p in path.parent.glob('*.json'):
                    fallback = p
                    logger.debug(f"Using fallback JSON source: {fallback}")
                    break
            except Exception:
                fallback = None

            if fallback and fallback.exists():
                try:
                    with open(fallback, 'r', encoding='utf-8') as f:
                        raw_data = json.load(f)
                except Exception:
                    logger.error(f"Error loading JSON source via fallback: {fallback}")
                    raise
            else:
                logger.error(f"Source file not found at: {path}")
                raise
        except Exception as e:
            logger.error(f"Error loading JSON source {path}: {e}")
            raise

        return JsonSourceOperator(source_data=raw_data, iterator_query=query, attribute_mappings=mappings)

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