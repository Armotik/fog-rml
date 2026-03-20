from __future__ import annotations

import importlib

import pytest


@pytest.mark.coverage_suite
def test_operators_package_imports():
    module = importlib.import_module("pyhartig.operators")
    assert module.__name__ == "pyhartig.operators"


@pytest.mark.edge_case
def test_operators_package_reloads():
    module = importlib.import_module("pyhartig.operators")
    assert importlib.reload(module) is module
