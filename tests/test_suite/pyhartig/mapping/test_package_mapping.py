from __future__ import annotations

import importlib

import pytest


@pytest.mark.coverage_suite
def test_mapping_package_imports():
    module = importlib.import_module("pyhartig.mapping")
    assert module.__name__ == "pyhartig.mapping"


@pytest.mark.edge_case
def test_mapping_package_reloads():
    module = importlib.import_module("pyhartig.mapping")
    assert importlib.reload(module) is module
