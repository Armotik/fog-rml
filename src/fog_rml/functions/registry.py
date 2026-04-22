from typing import Callable, Dict, Optional


class FunctionRegistry:
    """A simple registry for extension functions (FnO-style).

    Usage:
        from fog_rml.functions.registry import FunctionRegistry
        FunctionRegistry.register("http://ex.org/fno#toUpper", my_func)
        f = FunctionRegistry.get("http://ex.org/fno#toUpper")
    """

    _registry: Dict[str, Callable] = {}

    @classmethod
    def register(cls, uri: str, func: Callable) -> None:
        if not isinstance(uri, str):
            uri = str(uri)
        cls._registry[uri] = func

    @classmethod
    def get(cls, uri: str) -> Optional[Callable]:
        if not isinstance(uri, str):
            uri = str(uri)
        return cls._registry.get(uri)

    @classmethod
    def unregister(cls, uri: str) -> None:
        if not isinstance(uri, str):
            uri = str(uri)
        cls._registry.pop(uri, None)

    @classmethod
    def list_registered(cls) -> Dict[str, Callable]:
        return dict(cls._registry)

