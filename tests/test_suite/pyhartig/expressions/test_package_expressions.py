from __future__ import annotations

import importlib

import pytest


@pytest.mark.coverage_suite
def test_expressions_package_imports():
    module = importlib.import_module("pyhartig.expressions")
    assert module.__name__ == "pyhartig.expressions"


@pytest.mark.edge_case
def test_expressions_package_reloads():
    module = importlib.import_module("pyhartig.expressions")
    assert importlib.reload(module) is module
