from __future__ import annotations

import importlib

import pytest

from pyhartig.functions.registry import FunctionRegistry


@pytest.mark.coverage_suite
def test_functions_package_imports_and_exposes_registry():
    module = importlib.import_module("pyhartig.functions")
    assert module.FunctionRegistry is FunctionRegistry


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_functions_package_reloads_after_builtin_registration():
    module = importlib.import_module("pyhartig.functions")
    assert importlib.reload(module).__name__ == "pyhartig.functions"
