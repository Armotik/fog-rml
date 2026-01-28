import logging
import json
from pathlib import Path
from rdflib import Graph, Namespace, URIRef, Node

from pyhartig.operators.Operator import Operator
from pyhartig.operators.sources.JsonSourceOperator import JsonSourceOperator
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
        source_file = graph.value(logical_source_node, RML.source)
        iterator = graph.value(logical_source_node, RML.iterator)
        ref_formulation = graph.value(logical_source_node, RML.referenceFormulation)

        # 2. Resolve File Path
        src_path = Path(str(source_file))
        if not src_path.is_absolute():
            src_path = mapping_dir / src_path

        # 3. Dispatch based on Reference Formulation
        # Default to JSON if not specified or if explicitly JSONPath
        if ref_formulation == QL.JSONPath or ref_formulation is None:
            return SourceFactory._create_json_source(src_path, iterator, attribute_mappings)

        # Future extension:
        # if ref_formulation == QL.CSV:
        #     return SourceFactory._create_csv_source(...)

        raise ValueError(f"Unsupported reference formulation: {ref_formulation}")

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
            logger.warning(f"Source file not found at: {path}. Using empty dataset.")
            raw_data = {}
        except Exception as e:
            logger.error(f"Error loading JSON source {path}: {e}")
            raw_data = {}

        return JsonSourceOperator(source_data=raw_data, iterator_query=query, attribute_mappings=mappings)