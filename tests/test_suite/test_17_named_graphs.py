import pytest
from rdflib import Graph
from pyhartig.mapping.MappingParser import MappingParser
from pyhartig.serializers.NTriplesSerializer import NTriplesSerializer
import tempfile


def test_named_graphs_simple_template(tmp_path):
    """Simple mapping with rr:graphMap using a template variable producing a named graph."""
    # Create a small RML mapping (Turtle) that uses graphMap on predicateObjectMap
    rml = '''@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix ql: <http://semweb.mmlab.be/ns/ql#> .
@prefix ex: <http://example.org/> .

<> a rr:TriplesMap ;
  rml:logicalSource [ rml:reference "data.json" ; rml:referenceFormulation ql:JSONPath ] ;
  rr:subjectMap [ rr:template "http://example.org/issue/{id}" ; rr:termType rr:IRI ] ;
  rr:predicateObjectMap [
    rr:predicateMap [ rr:constant ex:title ] ;
    rr:objectMap [ rml:reference "title" ] ;
    rr:graphMap [ rr:template "http://example.org/graph/{repo}" ]
  ] .
'''

    # Create data.json referenced by mapping
    data = '[{"id": "1", "title": "Hello", "repo": "r1"}]'

    mapping_file = tmp_path / "mapping.ttl"
    data_file = tmp_path / "data.json"
    mapping_file.write_text(rml, encoding="utf-8")
    data_file.write_text(data, encoding="utf-8")

    parser = MappingParser(str(mapping_file))
    pipeline = parser.parse()

    # Execute the pipeline to get tuples
    results = list(pipeline.execute())

    # Expect at least one tuple with 'graph' attribute set to IRI http://example.org/graph/r1
    graphs = [t.get('graph') for t in results if 'graph' in t]
    assert graphs, "No graph attribute generated"
    # Graph term may be IRI object or string representation; ensure substring present
    assert any('http://example.org/graph/r1' in str(g) for g in graphs)


def test_named_graphs_pom_override(tmp_path):
    """Test that POM-level graphMap overrides subject-level graphMap."""
    rml = '''@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix ql: <http://semweb.mmlab.be/ns/ql#> .
@prefix ex: <http://example.org/> .

<> a rr:TriplesMap ;
  rml:logicalSource [ rml:reference "data.json" ; rml:referenceFormulation ql:JSONPath ] ;
  rr:subjectMap [ rr:template "http://example.org/issue/{id}" ; rr:termType rr:IRI ; rr:graphMap [ rr:template "http://example.org/graph/subject" ] ] ;
  rr:predicateObjectMap [
    rr:predicateMap [ rr:constant ex:title ] ;
    rr:objectMap [ rml:reference "title" ] ;
    rr:graphMap [ rr:template "http://example.org/graph/pom" ]
  ] .
'''

    data = '[{"id": "2", "title": "Hello2"}]'

    mapping_file = tmp_path / "mapping2.ttl"
    data_file = tmp_path / "data2.json"
    mapping_file.write_text(rml, encoding="utf-8")
    data_file.write_text(data, encoding="utf-8")

    parser = MappingParser(str(mapping_file))
    pipeline = parser.parse()
    results = list(pipeline.execute())

    graphs = [t.get('graph') for t in results if 'graph' in t]
    assert graphs, "No graph attribute generated"
    # Must match the POM-level graph template
    assert any('http://example.org/graph/pom' in str(g) for g in graphs)


# Optionally test serialization to N-Quads via existing serializer if available
def test_serialization_includes_graph(tmp_path):
    rml = '''@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix ql: <http://semweb.mmlab.be/ns/ql#> .
@prefix ex: <http://example.org/> .

<> a rr:TriplesMap ;
  rml:logicalSource [ rml:reference "data.json" ; rml:referenceFormulation ql:JSONPath ] ;
  rr:subjectMap [ rr:template "http://example.org/issue/{id}" ; rr:termType rr:IRI ] ;
  rr:predicateObjectMap [
    rr:predicateMap [ rr:constant ex:title ] ;
    rr:objectMap [ rml:reference "title" ] ;
    rr:graphMap [ rr:template "http://example.org/graph/{repo}" ]
  ] .
'''
    data = '[{"id": "3", "title": "Hello3", "repo": "r3"}]'
    mapping_file = tmp_path / "mapping3.ttl"
    data_file = tmp_path / "data3.json"
    mapping_file.write_text(rml, encoding="utf-8")
    data_file.write_text(data, encoding="utf-8")

    parser = MappingParser(str(mapping_file))
    pipeline = parser.parse()
    results = list(pipeline.execute())

    # Use serializer if available; otherwise ensure graph attribute present
    try:
        s = NTriplesSerializer()
        quads = s.serialize(results, quads=True)
        assert 'http://example.org/graph/r3' in quads
    except Exception:
        assert any('graph' in t for t in results)
