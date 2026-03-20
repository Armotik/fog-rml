from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING, Dict, Iterable
from pyhartig.algebra.Tuple import MappingTuple
class StreamRows:
    """A lazy iterable wrapper around an iterator/generator that supports
    iteration and on-demand materialization for operations like `len()` and
    indexing used by existing tests. It caches items as they are consumed.
    """

    def __init__(self, iterator: Iterable[MappingTuple]):
        # Accept any iterable (generator or list)
        self._iter = iter(iterator)
        self._cache = []
        self._exhausted = False

    def __iter__(self):
        # Yield from cache first
        for item in self._cache:
            yield item
        if not self._exhausted:
            for item in self._iter:
                self._cache.append(item)
                yield item
            self._exhausted = True

    def _materialize_all(self):
        if not self._exhausted:
            for item in self._iter:
                self._cache.append(item)
            self._exhausted = True

    def __len__(self):
        self._materialize_all()
        return len(self._cache)

    def __getitem__(self, idx):
        # support indexing and slicing by materializing as needed
        if isinstance(idx, slice):
            self._materialize_all()
            return self._cache[idx]
        # single index
        if idx < 0:
            self._materialize_all()
            return self._cache[idx]
        # materialize until we have idx+1 items
        while len(self._cache) <= idx and not self._exhausted:
            try:
                nxt = next(self._iter)
                self._cache.append(nxt)
            except StopIteration:
                self._exhausted = True
                break
        return self._cache[idx]


if TYPE_CHECKING:
    from pyhartig.operators.ExtendOperator import ExtendOperator


class Operator(ABC):
    """
    Abstract base class for all operators in the system.
    """

    @abstractmethod
    def execute(self) -> Iterable[MappingTuple]:
        """
        Execute the operator and return an iterable of MappingTuple results.
        Implementations should use generators (yield) to stream results.
        :return: Iterable[MappingTuple]
        """
        pass

    @abstractmethod
    def explain(self, indent: int = 0, prefix: str = "") -> str:
        """
        Generate a human-readable explanation of the operator tree.

        :param indent: Current indentation level
        :param prefix: Prefix for tree structure (e.g., "├─", "└─")
        :return: String representation of the operator tree
        """
        pass

    @abstractmethod
    def explain_json(self) -> Dict[str, Any]:
        """
        Generate a JSON-serializable explanation of the operator tree.

        :return: Dictionary representing the operator tree structure
        """
        pass

    def extend(self, var_name: str, expression: Any) -> 'ExtendOperator':
        """
        Fluent interface helper to chain ExtendOperators.

        :usage:
            op.extend("new_col", Constant("val")).extend(...)

        :param var_name: Name of the variable to extend
        :param expression: Expression to compute the new value
        :return: ExtendOperator instance
        """
        from pyhartig.operators.ExtendOperator import ExtendOperator

        return ExtendOperator(
            parent_operator=self,
            new_attribute=var_name,
            expression=expression
        )
