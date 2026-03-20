from __future__ import annotations

import pytest

from pyhartig.functions.registry import FunctionRegistry


@pytest.mark.coverage_suite
def test_function_registry_registers_and_lists_functions():
    FunctionRegistry.register("http://example.org/f", lambda: "ok")
    assert FunctionRegistry.get("http://example.org/f") is not None
    assert "http://example.org/f" in FunctionRegistry.list_registered()
    FunctionRegistry.unregister("http://example.org/f")


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_function_registry_unregister_is_idempotent():
    FunctionRegistry.unregister("http://example.org/missing")
    assert FunctionRegistry.get("http://example.org/missing") is None
