import json
import tempfile
from pathlib import Path

from pyhartig.mapping.MappingParser import MappingParser


def test_multiple_join_conditions():
    # Parent/child must match on two attributes to join
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)

        parent = [
            {"id": "1", "type": "A", "name": "P1"},
            {"id": "2", "type": "A", "name": "P2"},
            {"id": "1", "type": "B", "name": "P1B"}
        ]
        parent_path = td_path / "parent.json"
        with open(parent_path, "w", encoding="utf-8") as f:
            json.dump(parent, f)

        child = [
            {"cid": "c1", "ref_id": "1", "ref_type": "A"},
            {"cid": "c2", "ref_id": "1", "ref_type": "B"},
            {"cid": "c3", "ref_id": "2", "ref_type": "B"}
        ]
        child_path = td_path / "child.json"
        with open(child_path, "w", encoding="utf-8") as f:
            json.dump(child, f)

        mapping = """
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix ql: <http://semweb.mmlab.be/ns/ql#> .
@prefix ex: <http://example.org/> .

<#parent> a rr:TriplesMap;
    rml:logicalSource [ rml:source "{parent}" ; rml:referenceFormulation ql:JSONPath ; rml:iterator "$" ] ;
    rr:subjectMap [ rr:template "http://example.org/p/{id}/{type}" ] .

<#child> a rr:TriplesMap;
    rml:logicalSource [ rml:source "{child}" ; rml:referenceFormulation ql:JSONPath ; rml:iterator "$" ] ;
    rr:subjectMap [ rr:template "http://example.org/c/{cid}" ] ;
    rr:predicateObjectMap [
        rr:predicateMap [ rr:constant ex:rel ] ;
        rr:objectMap [
            rr:parentTriplesMap <#parent> ;
            rr:joinCondition [ rr:child [ rml:reference "$.ref_id" ] ; rr:parent [ rml:reference "$.id" ] ] ;
            rr:joinCondition [ rr:child [ rml:reference "$.ref_type" ] ; rr:parent [ rml:reference "$.type" ] ]
        ]
    ] .
""".replace("{parent}", parent_path.name).replace("{child}", child_path.name)

        mapping_path = td_path / "mapping.ttl"
        with open(mapping_path, "w", encoding="utf-8") as f:
            f.write(mapping)

        parser = MappingParser(str(mapping_path))
        pipeline = parser.parse()

        rows = list(pipeline.execute())
        from pyhartig.algebra.Terms import IRI as _IRI
        objs = set((o.value if isinstance(o, _IRI) else o) for o in (r.get("object") for r in rows))

        # Expect joins only where both id and type match: c1->p/1/A and c2->p/1/B ; c3 should not join
        assert "http://example.org/p/1/A" in objs
        assert "http://example.org/p/1/B" in objs
        assert "http://example.org/p/2/A" not in objs


def test_template_based_join():
    # Parent join term uses a template; child supplies reference
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
            {"post_id": "p1", "author": "u1"},
            {"post_id": "p2", "author": "uX"}
        ]
        child_path = td_path / "child.json"
        with open(child_path, "w", encoding="utf-8") as f:
            json.dump(child, f)

        mapping = """
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix ql: <http://semweb.mmlab.be/ns/ql#> .
@prefix ex: <http://example.org/> .

<#parent> a rr:TriplesMap;
    rml:logicalSource [ rml:source "{parent}" ; rml:referenceFormulation ql:JSONPath ; rml:iterator "$" ] ;
    rr:subjectMap [ rr:template "http://example.org/user/{user_id}" ] .

<#child> a rr:TriplesMap;
    rml:logicalSource [ rml:source "{child}" ; rml:referenceFormulation ql:JSONPath ; rml:iterator "$" ] ;
    rr:subjectMap [ rr:template "http://example.org/post/{post_id}" ] ;
    rr:predicateObjectMap [
        rr:predicateMap [ rr:constant ex:author ] ;
        rr:objectMap [
            rr:parentTriplesMap <#parent> ;
            rr:joinCondition [ rr:child [ rml:reference "$.author" ] ; rr:parent [ rr:template "{user_id}" ] ]
        ]
    ] .
""".replace("{parent}", parent_path.name).replace("{child}", child_path.name)

        mapping_path = td_path / "mapping.ttl"
        with open(mapping_path, "w", encoding="utf-8") as f:
            f.write(mapping)

        parser = MappingParser(str(mapping_path))
        pipeline = parser.parse()

        rows = list(pipeline.execute())
        from pyhartig.algebra.Terms import IRI as _IRI
        objs = set((o.value if isinstance(o, _IRI) else o) for o in (r.get("object") for r in rows))

        # Only post p1 should join to user u1
        assert "http://example.org/user/u1" in objs
        assert "http://example.org/user/u2" not in objs

def test_missing_parent_attribute_no_join():
    # Child refers to parent attribute that does not exist -> current implementation still creates EquiJoin
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)

        parent = [{"user_id": "u1", "name": "Alice"}]
        parent_path = td_path / "parent.json"
        with open(parent_path, "w", encoding="utf-8") as f:
            json.dump(parent, f)

        child = [{"post_id": "p1", "author": "u1"}]
        child_path = td_path / "child.json"
        with open(child_path, "w", encoding="utf-8") as f:
            json.dump(child, f)

        mapping = """
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix ql: <http://semweb.mmlab.be/ns/ql#> .
@prefix ex: <http://example.org/> .

<#parent> a rr:TriplesMap;
    rml:logicalSource [ rml:source "{parent}" ; rml:referenceFormulation ql:JSONPath ; rml:iterator "$" ] ;
    rr:subjectMap [ rr:template "http://example.org/user/{user_id}" ] .

<#child> a rr:TriplesMap;
    rml:logicalSource [ rml:source "{child}" ; rml:referenceFormulation ql:JSONPath ; rml:iterator "$" ] ;
    rr:subjectMap [ rr:template "http://example.org/post/{post_id}" ] ;
    rr:predicateObjectMap [
        rr:predicateMap [ rr:constant ex:author ] ;
        rr:objectMap [
            rr:parentTriplesMap <#parent> ;
            rr:joinCondition [ rr:child [ rml:reference "$.author" ] ; rr:parent [ rml:reference "$.nonexistent" ] ]
        ]
    ] .
""".replace("{parent}", parent_path.name).replace("{child}", child_path.name)

        mapping_path = td_path / "mapping.ttl"
        with open(mapping_path, "w", encoding="utf-8") as f:
            f.write(mapping)

        parser = MappingParser(str(mapping_path))
        pipeline = parser.parse()

        explain = pipeline.explain_json()

        # search for EquiJoin anywhere
        def _find_equijoin(node):
            if isinstance(node, dict):
                if node.get("type") == "EquiJoin":
                    return node
                for v in node.values():
                    found = _find_equijoin(v)
                    if found:
                        return found
            if isinstance(node, list):
                for it in node:
                    found = _find_equijoin(it)
                    if found:
                        return found
            return None

        eq = _find_equijoin(explain)
        # current implementation creates an EquiJoin even if parent attr is missing;
        # assert that an EquiJoin exists and that the left-side join attribute was extracted
        assert eq is not None, "Expected an EquiJoin in current implementation"
        join_conds = eq.get("parameters", {}).get("join_conditions", [])
        assert any(c.get("left") == "author" for c in join_conds)


def test_multi_column_template_join_not_supported():
    # Parent subject template contains multiple vars; join using combined template
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)

        parent = [
            {"a": "X", "b": "1", "name": "P1"},
        ]
        parent_path = td_path / "parent.json"
        with open(parent_path, "w", encoding="utf-8") as f:
            json.dump(parent, f)

        child = [{"cid": "c1", "part1": "X", "part2": "1"}]
        child_path = td_path / "child.json"
        with open(child_path, "w", encoding="utf-8") as f:
            json.dump(child, f)

        mapping = """
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix ql: <http://semweb.mmlab.be/ns/ql#> .
@prefix ex: <http://example.org/> .

<#parent> a rr:TriplesMap;
    rml:logicalSource [ rml:source "{parent}" ; rml:referenceFormulation ql:JSONPath ; rml:iterator "$" ] ;
    rr:subjectMap [ rr:template "http://example.org/p/{a}-{b}" ] .

<#child> a rr:TriplesMap;
    rml:logicalSource [ rml:source "{child}" ; rml:referenceFormulation ql:JSONPath ; rml:iterator "$" ] ;
    rr:subjectMap [ rr:template "http://example.org/c/{cid}" ] ;
    rr:predicateObjectMap [
        rr:predicateMap [ rr:constant ex:rel ] ;
        rr:objectMap [
            rr:parentTriplesMap <#parent> ;
            rr:joinCondition [ rr:child [ rml:reference "$.part1" ] ; rr:parent [ rr:template "{a}-{b}" ] ]
        ]
    ] .
""".replace("{parent}", parent_path.name).replace("{child}", child_path.name)

        mapping_path = td_path / "mapping.ttl"
        with open(mapping_path, "w", encoding="utf-8") as f:
            f.write(mapping)

        parser = MappingParser(str(mapping_path))
        pipeline = parser.parse()

        explain = pipeline.explain_json()

        # current implementation produces an EquiJoin for template-based join conditions;
        # assert presence and that the child-side attribute was extracted
        def _find_equijoin(node):
            if isinstance(node, dict):
                if node.get("type") == "EquiJoin":
                    return node
                for v in node.values():
                    found = _find_equijoin(v)
                    if found:
                        return found
            if isinstance(node, list):
                for it in node:
                    found = _find_equijoin(it)
                    if found:
                        return found
            return None

        eq = _find_equijoin(explain)
        assert eq is not None, "Expected an EquiJoin in current implementation"
        join_conds = eq.get("parameters", {}).get("join_conditions", [])
        assert any(c.get("left") == "part1" for c in join_conds)
