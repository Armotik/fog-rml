import json
from pathlib import Path

from pyhartig.operators.sources.CsvSourceOperator import CsvSourceOperator
from pyhartig.operators.sources.XmlSourceOperator import XmlSourceOperator


def test_csv_source_operator(tmp_path: Path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

    mappings = {"id": "id", "name": "name"}

    op = CsvSourceOperator(str(csv_file), iterator_query="$", attribute_mappings=mappings)
    rows = list(op.execute())

    assert len(rows) == 2
    assert rows[0]["id"] == "1"
    assert rows[0]["name"] == "Alice"
    assert rows[1]["id"] == "2"
    assert rows[1]["name"] == "Bob"

    info = op.explain_json()
    assert info["parameters"]["attribute_mappings"] == mappings
    assert info["parameters"]["iterator"] == "$"


def test_xml_source_operator(tmp_path: Path):
    xml_file = tmp_path / "data.xml"
    xml_file.write_text(
        """
        <items>
          <item id="a"><title>First</title></item>
          <item id="b"><title>Second</title></item>
        </items>
        """,
        encoding="utf-8",
    )

    mappings = {"id": "@id", "title": "title"}

    op = XmlSourceOperator(str(xml_file), iterator_query=".//item", attribute_mappings=mappings)
    rows = list(op.execute())

    assert len(rows) == 2
    assert rows[0]["id"] == "a"
    assert rows[0]["title"] == "First"
    assert rows[1]["id"] == "b"
    assert rows[1]["title"] == "Second"

    info = op.explain_json()
    assert info["parameters"]["attribute_mappings"] == mappings
    assert info["parameters"]["iterator"] == ".//item"
