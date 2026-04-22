from __future__ import annotations

import importlib

import pytest


@pytest.mark.coverage_suite
def test_serializers_package_imports():
    module = importlib.import_module("fog_rml.serializers")
    assert module.__name__ == "fog_rml.serializers"
    assert hasattr(module, "NTriplesSerializer")
    assert hasattr(module, "NQuadsSerializer")
    assert hasattr(module, "TurtleSerializer")


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_serializers_package_reloads():
    module = importlib.import_module("fog_rml.serializers")
    assert importlib.reload(module) is module

