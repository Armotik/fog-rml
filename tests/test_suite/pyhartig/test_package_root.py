from __future__ import annotations

import importlib

import pytest


@pytest.mark.coverage_suite
def test_pyhartig_package_imports():
    module = importlib.import_module("pyhartig")
    assert module.__name__ == "pyhartig"


@pytest.mark.edge_case
def test_pyhartig_package_reloads_cleanly():
    module = importlib.import_module("pyhartig")
    reloaded = importlib.reload(module)
    assert reloaded is module
