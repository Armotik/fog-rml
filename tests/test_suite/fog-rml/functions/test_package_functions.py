from __future__ import annotations

import importlib
import sys
import builtins as py_builtins

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


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_functions_package_ignores_optional_plugin_import_failures(monkeypatch):
    module = importlib.import_module("fog_rml.functions")
    original_import = py_builtins.__import__
    blocked_fromlist = {"builtins", "idlab_plugins", "fog_plugins"}

    def _blocked_optional_import(name, globals=None, locals=None, fromlist=(), level=0):
        if globals and globals.get("__package__") == "fog_rml.functions" and level == 1:
            if blocked_fromlist.intersection(fromlist or ()):
                raise RuntimeError("optional plugin unavailable")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(py_builtins, "__import__", _blocked_optional_import)
    reloaded = importlib.reload(module)
    assert reloaded.__all__ == ["FunctionRegistry"]

    monkeypatch.undo()
    importlib.reload(sys.modules["fog_rml.functions"])
