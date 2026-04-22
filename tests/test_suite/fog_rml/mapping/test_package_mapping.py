from __future__ import annotations

import importlib

import pytest


@pytest.mark.coverage_suite
def test_mapping_package_imports():
    module = importlib.import_module("fog_rml.mapping")
    assert module.__name__ == "fog_rml.mapping"


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_mapping_package_reloads():
    module = importlib.import_module("fog_rml.mapping")
    assert importlib.reload(module) is module
