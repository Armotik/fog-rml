import time
from pyhartig.operators.Operator import Operator
from pyhartig.operators.ProjectOperator import ProjectOperator
from pyhartig.operators.ExtendOperator import ExtendOperator
from pyhartig.algebra.Tuple import MappingTuple
from pyhartig.expressions.Reference import Reference
from pyhartig.expressions.FunctionCall import FunctionCall
from pyhartig.expressions.Constant import Constant
from pyhartig.algebra.Terms import Literal as AlgebraLiteral


class DummySource(Operator):
    def __init__(self, n_rows: int):
        self.n = n_rows

    def execute(self):
        for i in range(self.n):
            # simple tuple with numeric id and a string
            yield MappingTuple({'x': i, 'y': f"v{i}"})

    def explain(self, indent: int = 0, prefix: str = ""):
        return "DummySource"

    def explain_json(self):
        return {"type": "DummySource"}


def test_project_extend_benchmark():
    N = 20000
    base = DummySource(N)

    # phi depends only on 'x'
    phi = FunctionCall(lambda v: f"obj-{v}", [Reference('x')])

    P = {'x'}

    # Left: Project after Extend => Project^{P U {a}}(Extend_phi^a(r))
    left_ext = ExtendOperator(base, 'a', phi)
    left = ProjectOperator(left_ext, P.union({'a'}))

    # Right: Project before Extend => Extend_phi^a(Project^P(r))
    right_proj = ProjectOperator(base, P)
    right = ExtendOperator(right_proj, 'a', phi)

    # Materialize and time left
    t0 = time.perf_counter()
    left_rows = list(left.execute())
    t1 = time.perf_counter()
    right_rows = list(right.execute())
    t2 = time.perf_counter()

    left_time = t1 - t0
    right_time = t2 - t1

    # Ensure results are identical
    assert left_rows == right_rows

    print(f"Rows: {N} | Left (Extend then Project): {left_time:.4f}s | Right (Project then Extend): {right_time:.4f}s")
