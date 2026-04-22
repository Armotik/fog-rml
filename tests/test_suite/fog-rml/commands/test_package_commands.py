from __future__ import annotations

import importlib

import pytest


@pytest.mark.coverage_suite
def test_commands_package_imports():
    module = importlib.import_module("fog_rml.commands")
    assert module.__name__ == "fog_rml.commands"


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_commands_package_reloads():
    module = importlib.import_module("fog_rml.commands")
    assert importlib.reload(module) is module

