from __future__ import annotations

import pytest

from fog_rml.functions.idlab_plugins import equal, get_mime_type, not_equal, true_condition
from fog_rml.functions.registry import FunctionRegistry
from fog_rml.namespaces import IDLAB_FUNCTIONS_BASE


@pytest.mark.coverage_suite
def test_idlab_plugins_behave_and_register():
    assert true_condition("", "value") is True
    assert equal("a", "a") is True
    assert get_mime_type("data.ttl") == "text/turtle"
    assert FunctionRegistry.get(f"{IDLAB_FUNCTIONS_BASE}trueCondition") is true_condition


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_idlab_plugins_handle_negative_cases():
    assert not_equal("a", "b") is True
    assert get_mime_type("archive.bin") == "application/octet-stream"
