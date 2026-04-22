from __future__ import annotations

import importlib

import pytest


@pytest.mark.coverage_suite
def test_fog_rml_package_imports():
    module = importlib.import_module("fog_rml")
    assert module.__name__ == "fog_rml"


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_fog_rml_package_reloads_cleanly():
    module = importlib.import_module("fog_rml")
    reloaded = importlib.reload(module)
    assert reloaded is module

