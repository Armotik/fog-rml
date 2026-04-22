from __future__ import annotations

import importlib

import pytest


@pytest.mark.coverage_suite
def test_expressions_package_imports():
    module = importlib.import_module("fog_rml.expressions")
    assert module.__name__ == "fog_rml.expressions"


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_expressions_package_reloads():
    module = importlib.import_module("fog_rml.expressions")
    assert importlib.reload(module) is module
