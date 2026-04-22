from typing import Any, List, Callable
from fog_rml.expressions.Expression import Expression
from fog_rml.algebra.Tuple import MappingTuple, EPSILON
from fog_rml.functions.registry import FunctionRegistry

class FunctionCall(Expression):
    """
    Represents the application of an extension function f to subexpressions. (f(phi1, ..., phin))
    """

    def __init__(self, function: Callable, arguments: List[Expression]):
        """
        Initializes a FunctionCall expression.
        :param function: Python callable or function IRI (str/URIRef). If a string/IRI is provided,
        it will be resolved via `FunctionRegistry.get` at evaluation time.
        :param arguments: List of sub-expressions (Expression) that will provide the arguments
        """
        self.function = function
        self.arguments = arguments

    def evaluate(self, tuple_data: MappingTuple) -> Any:
        """
        Evaluates the function call against the provided tuple data.
        :param tuple_data: The tuple data to evaluate against.
        :return: The result of applying the function to the evaluated arguments, or EPSILON if any argument is EPSILON or an error occurs.
        """
        evaluated_args = []
        # Evaluate arguments; do not short-circuit on EPSILON so functions
        # can implement their own fallback logic when some args are missing.
        for arg in self.arguments:
            val = arg.evaluate(tuple_data)
            evaluated_args.append(val)

        # Apply the function safely to ensure total functionality
        try:
            func = self.function
            # resolve registry-based function identifiers lazily
            if isinstance(func, str):
                func = FunctionRegistry.get(func)
                if func is None:
                    return EPSILON
            result = func(*evaluated_args)
            # Ensure the function itself didn't return None or an error state
            # that should be represented as EPSILON in this algebra.
            return result if result is not None else EPSILON
        except (TypeError, ValueError, ArithmeticError):
            # Capture potential domain errors (e.g., division by zero,
            # incompatible types) and return the algebraic error constant.
            return EPSILON
        except Exception:
            # Capture potential domain errors (e.g., division by zero,
            # incompatible types) and return the algebraic error constant.
            return EPSILON

    def __repr__(self):
        """
        Returns a string representation of the FunctionCall expression.
        :return: A string representing the FunctionCall expression.
        """
        args_repr = ", ".join(repr(a) for a in self.arguments)
        func_name = getattr(self.function, "__name__", str(self.function))
        return f"{func_name}({args_repr})"

