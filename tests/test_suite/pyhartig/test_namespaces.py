from __future__ import annotations

import pytest

from pyhartig.algebra.Terms import IRI
from pyhartig.namespaces import EXAMPLE_ORG_BASE, FOAF_NAME, XSD_DOUBLE, get_xsd_from_python_type


@pytest.mark.coverage_suite
def test_namespace_constants_are_exported_as_expected():
    assert EXAMPLE_ORG_BASE.endswith("/")
    assert isinstance(FOAF_NAME, IRI)
    assert get_xsd_from_python_type(1.25) == XSD_DOUBLE


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_get_xsd_from_python_type_defaults_to_string():
    assert get_xsd_from_python_type(object()).value.endswith("#string")
