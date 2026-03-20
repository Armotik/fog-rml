from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from pyhartig.operators.sources.XmlSourceOperator import XmlSourceOperator


@pytest.mark.coverage_suite
def test_xml_source_operator_extracts_attributes_and_elements(stream_to_list):
    root = ET.fromstring("<root><row code='1'><name>Alice</name></row></root>")
    source = XmlSourceOperator(root, ".//row", {"code": "@code", "name": "name"})
    rows = stream_to_list(source.execute())
    assert rows[0]["code"] == "1"
    assert rows[0]["name"] == "Alice"
    assert source.explain_json()["parameters"]["source_type"] == "XML"


@pytest.mark.edge_case
def test_xml_source_operator_returns_empty_iterators_for_missing_root():
    source = XmlSourceOperator(None, ".//row", {"id": "id"})
    assert source._apply_iterator(None, ".//row") == []


@pytest.mark.coverage_suite
def test_xml_source_operator_loads_from_file_and_handles_jsonpath_style_queries(tmp_path: Path):
    path = tmp_path / "items.xml"
    path.write_text("<root><row><Name>Alice</Name></row></root>", encoding="utf-8")
    source = XmlSourceOperator(path, "/root/row", {"name": "$.Name"})
    assert source._apply_iterator(None, "/root/row")
    assert source._apply_extraction(source._apply_iterator(None, "/root/row")[0], "$.Name") == ["Alice"]


@pytest.mark.edge_case
def test_xml_source_operator_fallbacks_on_invalid_contexts():
    root = ET.fromstring("<root><row><name>Alice</name></row></root>")
    source = XmlSourceOperator(root, ".//missing", {"name": "name"})
    assert source._apply_extraction(object(), "name") == []
