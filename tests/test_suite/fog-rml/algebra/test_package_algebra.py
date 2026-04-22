from __future__ import annotations

import importlib

import pytest


@pytest.mark.coverage_suite
def test_algebra_package_imports():
    module = importlib.import_module("fog_rml.algebra")
    assert module.__name__ == "fog_rml.algebra"


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_algebra_package_reloads():
    module = importlib.import_module("fog_rml.algebra")
    assert importlib.reload(module) is module

