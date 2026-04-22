from __future__ import annotations

import importlib

import pytest


@pytest.mark.coverage_suite
def test_operators_package_imports():
    module = importlib.import_module("fog_rml.operators")
    assert module.__name__ == "fog_rml.operators"


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_operators_package_reloads():
    module = importlib.import_module("fog_rml.operators")
    assert importlib.reload(module) is module
