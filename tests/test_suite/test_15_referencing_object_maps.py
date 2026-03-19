import json
import tempfile
from pathlib import Path

from pyhartig.mapping.MappingParser import MappingParser
from pyhartig.namespaces import QL_BASE, RDF_BASE, RML_BASE, RR_BASE


def test_referencing_object_map_join():
    # Create temporary directory for mapping and source files
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)

        # Parent JSON: users
        parent = [
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "u2", "name": "Bob"}
        ]
        parent_path = td_path / "parent.json"
        with open(parent_path, "w", encoding="utf-8") as f:
            json.dump(parent, f)

        # Child JSON: posts referencing users by author_id
        child = [
            {"post_id": "p1", "author": "u1", "title": "Hello"},
            {"post_id": "p2", "author": "u2", "title": "World"},
            {"post_id": "p3", "author": "u3", "title": "Orphan"}
        ]
        child_path = td_path / "child.json"
        with open(child_path, "w", encoding="utf-8") as f:
            json.dump(child, f)

        # Build an RML mapping that creates triples: subject from child, predicate ex:author, object from parent subject via join
        mapping_ttl = f"""
@prefix rr: <{RR_BASE}> .
@prefix rml: <{RML_BASE}> .
@prefix ql: <{QL_BASE}> .
@prefix ex: <http://example.org/> .
@prefix rdf: <{RDF_BASE}> .

# Child TriplesMap (posts) with inline parent TriplesMap
<#child> a rr:TriplesMap;
    rml:logicalSource [ rml:source "{child_path.name}" ; rml:referenceFormulation ql:JSONPath ; rml:iterator "$" ] ;
    rr:subjectMap [ rr:template "http://example.org/post/{{post_id}}" ] ;
    rr:predicateObjectMap [
         rr:predicateMap [ rr:constant ex:author ] ;
         rr:objectMap [
             rr:parentTriplesMap [ a rr:TriplesMap ;
                 rml:logicalSource [ rml:source "{parent_path.name}" ; rml:referenceFormulation ql:JSONPath ; rml:iterator "$" ] ;
                rr:subjectMap [ rr:template "http://example.org/user/{{user_id}}" ] ;
                 rr:predicateObjectMap [ rr:predicateMap [ rr:constant ex:uid ] ; rr:objectMap [ rml:reference "$.user_id" ] ]
             ] ;
             rr:joinCondition [ rr:child [ rml:reference "$.author" ] ; rr:parent [ rml:reference "$.user_id" ] ]
         ]
    ] .
"""

        mapping_path = td_path / "mapping.ttl"
        with open(mapping_path, "w", encoding="utf-8") as f:
            f.write(mapping_ttl)

        # Run the mapping parser
        parser = MappingParser(str(mapping_path))
        pipeline = parser.parse()

        # Verify pipeline explanation contains an EquiJoin for the referencing object map
        explain = pipeline.explain_json()

        def _find_equijoin(node):
            if isinstance(node, dict):
                if node.get("type") == "EquiJoin":
                    return node
                # search common child keys
                for k in ("left", "right", "children", "branches", "operators"):
                    if k in node:
                        found = _find_equijoin(node[k])
                        if found:
                            return found
                # generic descent
                for v in node.values():
                    found = _find_equijoin(v)
                    if found:
                        return found
            elif isinstance(node, list):
                for it in node:
                    found = _find_equijoin(it)
                    if found:
                        return found
            return None

        eq = _find_equijoin(explain)
        assert eq is not None, "Expected an EquiJoin operator in the generated pipeline"

        # Check that the join conditions include author = user_id
        join_conds = eq.get("parameters", {}).get("join_conditions", [])
        assert any(c.get("left") == "author" and c.get("right") == "user_id" for c in join_conds), f"Unexpected join conditions: {join_conds}"

        # Execute pipeline and collect triples-like rows: ensure joined rows exist for u1 and u2 but not u3
        rows = list(pipeline.execute())
        subjects = set(r.get("subject") for r in rows)
        predicates = set(r.get("predicate") for r in rows)
        objects = set(r.get("object") for r in rows)

        # Normalize object values to strings for comparison (IRI -> IRI.value, Literal -> lexical form)
        from pyhartig.algebra.Terms import IRI as _IRI, Literal as _Literal
        def _norm(o):
            if isinstance(o, _IRI):
                return o.value
            if isinstance(o, _Literal):
                return o.lexical_form
            return o

        objects_norm = set(_norm(o) for o in objects)

        # Objects should include parent subjects for u1 and u2 (join semantics)
        assert "http://example.org/user/u1" in objects_norm
        assert "http://example.org/user/u2" in objects_norm
        # u3 has no parent -> should not produce a joined triple for that post
        assert "http://example.org/user/u3" not in objects_norm


def test_referencing_object_map_join_parent_tm_reference():
    # Parent TriplesMap is referenced by IRI instead of inline
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)

        parent = [
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "u2", "name": "Bob"}
        ]
        parent_path = td_path / "parent.json"
        with open(parent_path, "w", encoding="utf-8") as f:
            json.dump(parent, f)

        child = [
            {"post_id": "p1", "author": "u1", "title": "Hello"},
            {"post_id": "p2", "author": "u2", "title": "World"},
            {"post_id": "p3", "author": "u3", "title": "Orphan"}
        ]
        child_path = td_path / "child.json"
        with open(child_path, "w", encoding="utf-8") as f:
            json.dump(child, f)

        mapping_ttl = f"""
@prefix rr: <{RR_BASE}> .
@prefix rml: <{RML_BASE}> .
@prefix ql: <{QL_BASE}> .
@prefix ex: <http://example.org/> .
@prefix rdf: <{RDF_BASE}> .

<#parent> a rr:TriplesMap;
    rml:logicalSource [ rml:source "{parent_path.name}" ; rml:referenceFormulation ql:JSONPath ; rml:iterator "$" ] ;
    rr:subjectMap [ rr:template "http://example.org/user/{{user_id}}" ] .

<#child> a rr:TriplesMap;
    rml:logicalSource [ rml:source "{child_path.name}" ; rml:referenceFormulation ql:JSONPath ; rml:iterator "$" ] ;
    rr:subjectMap [ rr:template "http://example.org/post/{{post_id}}" ] ;
    rr:predicateObjectMap [
         rr:predicateMap [ rr:constant ex:author ] ;
         rr:objectMap [
             rr:parentTriplesMap <#parent> ;
             rr:joinCondition [ rr:child [ rml:reference "$.author" ] ; rr:parent [ rml:reference "$.user_id" ] ]
         ]
    ] .
"""

        mapping_path = td_path / "mapping2.ttl"
        with open(mapping_path, "w", encoding="utf-8") as f:
            f.write(mapping_ttl)

        parser = MappingParser(str(mapping_path))
        pipeline = parser.parse()

        explain = pipeline.explain_json()

        # ensure EquiJoin exists
        def _find_equijoin(node):
            if isinstance(node, dict):
                if node.get("type") == "EquiJoin":
                    return node
                for k in ("left", "right", "children", "branches", "operators"):
                    if k in node:
                        found = _find_equijoin(node[k])
                        if found:
                            return found
                for v in node.values():
                    found = _find_equijoin(v)
                    if found:
                        return found
            elif isinstance(node, list):
                for it in node:
                    found = _find_equijoin(it)
                    if found:
                        return found
            return None

        eq = _find_equijoin(explain)
        assert eq is not None, "Expected an EquiJoin operator when parent TriplesMap is referenced"

        join_conds = eq.get("parameters", {}).get("join_conditions", [])
        assert any(c.get("left") == "author" and c.get("right") == "user_id" for c in join_conds)

        rows = list(pipeline.execute())
        from pyhartig.algebra.Terms import IRI as _IRI, Literal as _Literal
        def _norm(o):
            if isinstance(o, _IRI):
                return o.value
            if isinstance(o, _Literal):
                return o.lexical_form
            return o

        objects_norm = set(_norm(r.get("object")) for r in rows)
        assert "http://example.org/user/u1" in objects_norm
        assert "http://example.org/user/u2" in objects_norm
        assert "http://example.org/user/u3" not in objects_norm
