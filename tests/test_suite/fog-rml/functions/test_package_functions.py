from __future__ import annotations

import importlib

import pytest

from fog_rml.functions.registry import FunctionRegistry


@pytest.mark.coverage_suite
def test_functions_package_imports_and_exposes_registry():
    module = importlib.import_module("fog_rml.functions")
    assert module.FunctionRegistry is FunctionRegistry


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_functions_package_reloads_after_builtin_registration():
    module = importlib.import_module("fog_rml.functions")
    assert importlib.reload(module).__name__ == "fog_rml.functions"

