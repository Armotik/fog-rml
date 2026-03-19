import pytest

from pyhartig.functions import FunctionRegistry
from pyhartig.expressions.FunctionCall import FunctionCall
from pyhartig.expressions.Constant import Constant
from pyhartig.algebra.Tuple import MappingTuple


def test_idlab_functions_registered():
    """Registry contains sample idlab functions"""
    funcs = [
        'http://example.com/idlab/function/trueCondition',
        'http://example.com/idlab/function/equal',
        'http://example.com/idlab/function/notEqual',
        'http://example.com/idlab/function/getMIMEType',
    ]
    for uri in funcs:
        f = FunctionRegistry.get(uri)
        assert f is not None and callable(f), f"Function {uri} not registered"


def test_functioncall_resolves_and_executes_equal_and_mimetype():
    # equal -> True
    fc = FunctionCall('http://example.com/idlab/function/equal', [Constant('abc'), Constant('abc')])
    res = fc.evaluate(MappingTuple({}))
    assert res is True

    # notEqual -> False
    fc2 = FunctionCall('http://example.com/idlab/function/notEqual', [Constant('x'), Constant('x')])
    res2 = fc2.evaluate(MappingTuple({}))
    assert res2 is False

    # getMIMEType
    fc3 = FunctionCall('http://example.com/idlab/function/getMIMEType', [Constant('sample.ttl')])
    res3 = fc3.evaluate(MappingTuple({}))
    assert res3 == 'text/turtle'


def test_mappingparser_parses_fnml_and_creates_functioncall(tmp_path):
    """Create a minimal fnml:functionValue structure in the parser graph and ensure
    MappingParser._create_ext_expr returns a FunctionCall that resolves and executes."""
    from rdflib import BNode, Literal, URIRef, Namespace
    from pyhartig.mapping.MappingParser import MappingParser
    from pyhartig.expressions.FunctionCall import FunctionCall

    # prepare dummy mapping file so MappingParser __init__ can read it
    dummy = tmp_path / "dummy.ttl"
    dummy.write_text("# dummy")

    p = MappingParser(str(dummy))

    RR = Namespace("http://www.w3.org/ns/r2rml#")
    FNML = Namespace("http://semweb.mmlab.be/ns/fnml#")
    FNO = Namespace("https://w3id.org/function/ontology#")

    term_map = BNode()
    fn_node = BNode()
    p.graph.add((term_map, FNML.functionValue, fn_node))

    # pom that defines the function IRI using fno:executes
    pom1 = BNode(); pm1 = BNode(); om1 = BNode()
    p.graph.add((fn_node, RR.predicateObjectMap, pom1))
    p.graph.add((pom1, RR.predicateMap, pm1))
    p.graph.add((pom1, RR.objectMap, om1))
    p.graph.add((pm1, RR.constant, FNO.executes))
    p.graph.add((om1, RR.constant, Literal('http://example.com/idlab/function/equal')))

    # pom that provides a single constant parameter
    pom2 = BNode(); pm2 = BNode(); om2 = BNode()
    p.graph.add((fn_node, RR.predicateObjectMap, pom2))
    p.graph.add((pom2, RR.predicateMap, pm2))
    p.graph.add((pom2, RR.objectMap, om2))
    p.graph.add((pm2, RR.constant, URIRef('http://example.com/param1')))
    p.graph.add((om2, RR.constant, Literal('abc')))
    # second parameter for equality check
    pom3 = BNode(); pm3 = BNode(); om3 = BNode()
    p.graph.add((fn_node, RR.predicateObjectMap, pom3))
    p.graph.add((pom3, RR.predicateMap, pm3))
    p.graph.add((pom3, RR.objectMap, om3))
    p.graph.add((pm3, RR.constant, URIRef('http://example.com/param2')))
    p.graph.add((om3, RR.constant, Literal('abc')))

    expr = p._create_ext_expr(term_map, default_term_type="Literal")
    assert isinstance(expr, FunctionCall)
    # function stored as string IRI
    assert expr.function == 'http://example.com/idlab/function/equal'
    # executing the expression should call our registered equal() and return True
    res = expr.evaluate(MappingTuple({}))
    assert res is True


def test_nested_fnml_and_reference_args(tmp_path):
    from rdflib import BNode, Literal, URIRef, Namespace
    from pyhartig.mapping.MappingParser import MappingParser
    from pyhartig.algebra.Tuple import EPSILON

    dummy = tmp_path / "dummy2.ttl"
    dummy.write_text("# dummy")
    p = MappingParser(str(dummy))

    RR = Namespace("http://www.w3.org/ns/r2rml#")
    FNML = Namespace("http://semweb.mmlab.be/ns/fnml#")
    RML = Namespace("http://semweb.mmlab.be/ns/rml#")

    term_map = BNode(); fn_node = BNode()
    p.graph.add((term_map, FNML.functionValue, fn_node))

    # Build inner functionValue node for concat
    inner_fn = BNode()
    # inner param1 constant 'ab'
    ip1 = BNode(); ipm1 = BNode(); iom1 = BNode()
    p.graph.add((inner_fn, RR.predicateObjectMap, ip1))
    p.graph.add((ip1, RR.predicateMap, ipm1))
    p.graph.add((ip1, RR.objectMap, iom1))
    p.graph.add((ipm1, RR.constant, URIRef('http://pyhartig.org/functions#param1')))
    p.graph.add((iom1, RR.constant, Literal('ab')))
    # inner param2: reference 'uid'
    ip2 = BNode(); ipm2 = BNode(); iom2 = BNode()
    p.graph.add((inner_fn, RR.predicateObjectMap, ip2))
    p.graph.add((ip2, RR.predicateMap, ipm2))
    p.graph.add((ip2, RR.objectMap, iom2))
    p.graph.add((ipm2, RR.constant, URIRef('http://pyhartig.org/functions#param2')))
    p.graph.add((iom2, RML.reference, Literal('uid')))
    # inner executes -> concat
    iexec = BNode(); iepm = BNode(); ieom = BNode()
    p.graph.add((inner_fn, RR.predicateObjectMap, iexec))
    p.graph.add((iexec, RR.predicateMap, iepm))
    p.graph.add((iexec, RR.objectMap, ieom))
    p.graph.add((iepm, RR.constant, URIRef('https://w3id.org/function/ontology#executes')))
    p.graph.add((ieom, RR.constant, Literal('http://pyhartig.org/functions#concat')))

    # top-level: create an object map that contains the inner functionValue
    om_inner = BNode()
    p.graph.add((om_inner, FNML.functionValue, inner_fn))

    # top-level function node
    # top executes = idlab equal
    top_exec = BNode(); top_pm = BNode(); top_om = BNode()
    p.graph.add((fn_node, RR.predicateObjectMap, top_exec))
    p.graph.add((top_exec, RR.predicateMap, top_pm))
    p.graph.add((top_exec, RR.objectMap, top_om))
    p.graph.add((top_pm, RR.constant, URIRef('https://w3id.org/function/ontology#executes')))
    p.graph.add((top_om, RR.constant, Literal('http://example.com/idlab/function/equal')))

    # top-level param1 is the inner functionValue (as objectMap)
    pom_inner = BNode(); pm_inner = BNode()
    p.graph.add((fn_node, RR.predicateObjectMap, pom_inner))
    p.graph.add((pom_inner, RR.predicateMap, pm_inner))
    p.graph.add((pom_inner, RR.objectMap, om_inner))
    p.graph.add((pm_inner, RR.constant, URIRef('http://example.com/arg1')))

    # top-level second parameter constant 'abuid'
    pom_const = BNode(); pm_const = BNode(); om_const = BNode()
    p.graph.add((fn_node, RR.predicateObjectMap, pom_const))
    p.graph.add((pom_const, RR.predicateMap, pm_const))
    p.graph.add((pom_const, RR.objectMap, om_const))
    p.graph.add((pm_const, RR.constant, URIRef('http://example.com/paramB')))
    p.graph.add((om_const, RR.constant, Literal('abuid')))

    expr = p._create_ext_expr(term_map, default_term_type="Literal")
    assert expr is not None
    res = expr.evaluate(MappingTuple({'uid': 'uid'}))
    assert res is True


def test_failure_modes(tmp_path):
    from rdflib import BNode, Literal, URIRef, Namespace
    from pyhartig.mapping.MappingParser import MappingParser
    from pyhartig.algebra.Tuple import EPSILON

    dummy = tmp_path / "dummy3.ttl"
    dummy.write_text("# dummy")
    p = MappingParser(str(dummy))
    RR = Namespace("http://www.w3.org/ns/r2rml#")
    FNML = Namespace("http://semweb.mmlab.be/ns/fnml#")

    # Missing registration -> EPSILON
    term_map = BNode(); fn_node = BNode()
    p.graph.add((term_map, FNML.functionValue, fn_node))
    pom = BNode(); pm = BNode(); om = BNode()
    p.graph.add((fn_node, RR.predicateObjectMap, pom))
    p.graph.add((pom, RR.predicateMap, pm))
    p.graph.add((pom, RR.objectMap, om))
    p.graph.add((pm, RR.constant, URIRef('https://w3id.org/function/ontology#executes')))
    p.graph.add((om, RR.constant, Literal('http://example.com/not/registered')))
    # add two params
    pp1 = BNode(); ppm1 = BNode(); pom1 = BNode()
    p.graph.add((fn_node, RR.predicateObjectMap, pp1))
    p.graph.add((pp1, RR.predicateMap, ppm1))
    p.graph.add((pp1, RR.objectMap, pom1))
    p.graph.add((ppm1, RR.constant, URIRef('p1')))
    p.graph.add((pom1, RR.constant, Literal('x')))
    pp2 = BNode(); ppm2 = BNode(); pom2 = BNode()
    p.graph.add((fn_node, RR.predicateObjectMap, pp2))
    p.graph.add((pp2, RR.predicateMap, ppm2))
    p.graph.add((pp2, RR.objectMap, pom2))
    p.graph.add((ppm2, RR.constant, URIRef('p2')))
    p.graph.add((pom2, RR.constant, Literal('x')))

    expr = p._create_ext_expr(term_map)
    res = expr.evaluate(MappingTuple({}))
    assert res == EPSILON

    # Arity mismatch -> EPSILON
    term_map2 = BNode(); fn2 = BNode()
    p.graph.add((term_map2, FNML.functionValue, fn2))
    # execute equal but only one parameter
    e1 = BNode(); epm = BNode(); eom = BNode()
    p.graph.add((fn2, RR.predicateObjectMap, e1))
    p.graph.add((e1, RR.predicateMap, epm))
    p.graph.add((e1, RR.objectMap, eom))
    p.graph.add((epm, RR.constant, URIRef('https://w3id.org/function/ontology#executes')))
    p.graph.add((eom, RR.constant, Literal('http://example.com/idlab/function/equal')))
    # only one param
    pa = BNode(); pam = BNode(); poa = BNode()
    p.graph.add((fn2, RR.predicateObjectMap, pa))
    p.graph.add((pa, RR.predicateMap, pam))
    p.graph.add((pa, RR.objectMap, poa))
    p.graph.add((pam, RR.constant, URIRef('p')))
    p.graph.add((poa, RR.constant, Literal('onlyone')))

    expr2 = p._create_ext_expr(term_map2)
    res2 = expr2.evaluate(MappingTuple({}))
    assert res2 == EPSILON

    # Inner arg EPSILON propagation
    term_map3 = BNode(); fn3 = BNode()
    p.graph.add((term_map3, FNML.functionValue, fn3))
    # inner concat with reference missing
    inner = BNode(); p.graph.add((fn3, RR.predicateObjectMap, inner))
    # set executes inner
    iem = BNode(); ieom = BNode(); iepm = BNode()
    p.graph.add((inner, RR.predicateMap, iepm))
    p.graph.add((inner, RR.objectMap, ieom))
    p.graph.add((iepm, RR.constant, URIRef('https://w3id.org/function/ontology#executes')))
    p.graph.add((ieom, RR.constant, Literal('http://pyhartig.org/functions#concat')))
    # param is a reference that doesn't exist
    ip = BNode(); ipm = BNode(); iop = BNode()
    p.graph.add((inner, RR.predicateObjectMap, ip))
    p.graph.add((ip, RR.predicateMap, ipm))
    p.graph.add((ip, RR.objectMap, iop))
    p.graph.add((ipm, RR.constant, URIRef('p1')))
    from rdflib import Namespace as RDFNamespace
    RML = RDFNamespace('http://semweb.mmlab.be/ns/rml#')
    p.graph.add((iop, RML.reference, Literal('this_does_not_exist')))

    # top-level equal comparing inner to 'X'
    top = BNode(); tpm = BNode(); tom = BNode()
    p.graph.add((fn3, RR.predicateObjectMap, top))
    p.graph.add((top, RR.predicateMap, tpm))
    p.graph.add((top, RR.objectMap, tom))
    p.graph.add((tpm, RR.constant, URIRef('https://w3id.org/function/ontology#executes')))
    p.graph.add((tom, RR.constant, Literal('http://example.com/idlab/function/equal')))
    # add second param constant
    t1 = BNode(); tpm1 = BNode(); tom1 = BNode()
    p.graph.add((fn3, RR.predicateObjectMap, t1))
    p.graph.add((t1, RR.predicateMap, tpm1))
    p.graph.add((t1, RR.objectMap, tom1))
    p.graph.add((tpm1, RR.constant, URIRef('p2')))
    p.graph.add((tom1, RR.constant, Literal('X')))

    expr3 = p._create_ext_expr(term_map3)
    res3 = expr3.evaluate(MappingTuple({}))
    assert res3 == EPSILON


def test_template_percent_encoding_single_var(tmp_path):
    from rdflib import BNode, Literal, Namespace
    from pyhartig.mapping.MappingParser import MappingParser
    from pyhartig.algebra.Tuple import MappingTuple
    from pyhartig.algebra.Terms import IRI

    dummy = tmp_path / "dummy_t1.ttl"
    dummy.write_text("# dummy")
    p = MappingParser(str(dummy))

    RR = Namespace("http://www.w3.org/ns/r2rml#")

    term_map = BNode()
    p.graph.add((term_map, RR.template, Literal('http://ex.org/{p}')))

    expr = p._create_ext_expr(term_map, default_term_type="IRI")
    res = expr.evaluate(MappingTuple({'p': 'a/b:c'}))
    assert isinstance(res, IRI)
    assert res.value == 'http://ex.org/a%2Fb%3Ac'


def test_template_percent_encoding_multiple_vars(tmp_path):
    from rdflib import BNode, Literal, Namespace
    from pyhartig.mapping.MappingParser import MappingParser
    from pyhartig.algebra.Tuple import MappingTuple
    from pyhartig.algebra.Terms import IRI

    dummy = tmp_path / "dummy_t2.ttl"
    dummy.write_text("# dummy")
    p = MappingParser(str(dummy))

    RR = Namespace("http://www.w3.org/ns/r2rml#")

    term_map = BNode()
    p.graph.add((term_map, RR.template, Literal('http://ex.org/{p}/{q}')))

    expr = p._create_ext_expr(term_map, default_term_type="IRI")
    res = expr.evaluate(MappingTuple({'p': 'a:b', 'q': 'c/d'}))
    assert isinstance(res, IRI)
    assert res.value == 'http://ex.org/a%3Ab/c%2Fd'


def test_template_missing_reference_returns_epsilon(tmp_path):
    from rdflib import BNode, Literal, Namespace
    from pyhartig.mapping.MappingParser import MappingParser
    from pyhartig.algebra.Tuple import MappingTuple, EPSILON

    dummy = tmp_path / "dummy_t3.ttl"
    dummy.write_text("# dummy")
    p = MappingParser(str(dummy))

    RR = Namespace("http://www.w3.org/ns/r2rml#")

    term_map = BNode()
    p.graph.add((term_map, RR.template, Literal('http://ex.org/{missing}')))

    expr = p._create_ext_expr(term_map, default_term_type="IRI")
    res = expr.evaluate(MappingTuple({}))
    assert res == EPSILON
