import rdflib
from rdflib import Graph, URIRef, BNode, Literal

from pyhartig.mapping.MappingParser import MappingParser
from pyhartig.functions.registry import FunctionRegistry
from pyhartig.expressions.FunctionCall import FunctionCall
from pyhartig.algebra.Tuple import MappingTuple, EPSILON
from rdflib import Namespace

# Namespaces used in tests
RR = Namespace("http://www.w3.org/ns/r2rml#")
FNML = Namespace("http://semweb.mmlab.be/ns/fnml#")
FNO = Namespace("https://w3id.org/function/ontology#")


def _make_fnml_function(graph: Graph, fn_node, func_uri, params):
    # Add direct fno:executes triple
    graph.add((fn_node, FNO.executes, URIRef(func_uri)))
    # attach predicateObjectMap entries for parameters
    for i, (pm_pred, om_value) in enumerate(params, start=1):
        pom = BNode()
        graph.add((fn_node, RR.predicateObjectMap, pom))
        # predicate map
        pm = BNode()
        graph.add((pom, RR.predicateMap, pm))
        graph.add((pm, RR.constant, URIRef(pm_pred)))
        # object map
        om = BNode()
        graph.add((pom, RR.objectMap, om))
        # Use RML.reference for reference-valued object maps
        RML = Namespace("http://semweb.mmlab.be/ns/rml#")
        if isinstance(om_value, tuple) and om_value[0] == 'ref':
            graph.add((om, RML.reference, Literal(om_value[1])))
        else:
            graph.add((om, RR.constant, Literal(om_value)))


def test_fno_direct_executes_blank_node(tmp_path):
    # prepare dummy mapping file so MappingParser can initialize
    dummy = tmp_path / "dummy19a.ttl"
    dummy.write_text("# dummy")
    mp = MappingParser(str(dummy))
    g = mp.graph

    fn_node = BNode()
    _make_fnml_function(g, fn_node, "http://example.com/fn/equal", [("http://example.com/arg1", ("ref", "a")), ("http://example.com/arg2", "b")])

    # construct a term map that uses fnml:functionValue
    term = BNode()
    g.add((term, FNML.functionValue, fn_node))

    expr = mp._create_ext_expr(term)
    assert isinstance(expr, FunctionCall)

    # register the function
    def equal(a, b):
        if hasattr(a, 'lexical_form'):
            av = a.lexical_form
        elif hasattr(a, 'value'):
            av = a.value
        else:
            av = a
        if hasattr(b, 'lexical_form'):
            bv = b.lexical_form
        elif hasattr(b, 'value'):
            bv = b.value
        else:
            bv = b
        return av == bv

    FunctionRegistry.register("http://example.com/fn/equal", equal)

    # evaluate: provide a MappingTuple where reference 'a' resolves to 'b'
    val = expr.evaluate(MappingTuple({"a": "b"}))
    assert val is not EPSILON
    assert val is True


def test_fno_direct_executes_uriref(tmp_path):
    dummy = tmp_path / "dummy19b.ttl"
    dummy.write_text("# dummy")
    mp = MappingParser(str(dummy))
    g = mp.graph

    fn_node = URIRef("http://example.com/functionDescription/1")
    _make_fnml_function(g, fn_node, "http://example.com/fn/concat", [("http://example.com/arg1", ("ref", "x")), ("http://example.com/arg2", ("ref", "y"))])

    term = BNode()
    g.add((term, FNML.functionValue, fn_node))

    expr = mp._create_ext_expr(term)
    assert isinstance(expr, FunctionCall)

    # register concat
    def concat(a, b):
        if hasattr(a, 'lexical_form'):
            av = a.lexical_form
        elif hasattr(a, 'value'):
            av = a.value
        else:
            av = a
        if hasattr(b, 'lexical_form'):
            bv = b.lexical_form
        elif hasattr(b, 'value'):
            bv = b.value
        else:
            bv = b
        return str(av) + str(bv)

    FunctionRegistry.register("http://example.com/fn/concat", concat)

    val = expr.evaluate(MappingTuple({"x": "hello", "y": " world"}))
    assert val == "hello world"
