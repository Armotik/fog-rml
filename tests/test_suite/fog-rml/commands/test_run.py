from __future__ import annotations

from argparse import Namespace
from io import StringIO
from pathlib import Path

import pytest

from fog_rml.algebra.Terms import IRI, Literal
from fog_rml.algebra.Tuple import MappingTuple
from fog_rml.commands.run import RunCommand
from fog_rml.serializers.NQuadsSerializer import NQuadsSerializer
from fog_rml.serializers.NTriplesSerializer import NTriplesSerializer
from fog_rml.serializers.TurtleSerializer import TurtleSerializer
from fog_rml.commands.run import MappingRunCommand


class _Pipeline:
    def __init__(self, rows):
        self.rows = rows

    def execute(self):
        return iter(self.rows)

    def explain(self):
        return "pipeline"


@pytest.mark.coverage_suite
def test_run_command_serializes_rows(monkeypatch, tmp_path: Path):
    mapping = tmp_path / "mapping.ttl"
    mapping.write_text("@prefix rr: <http://www.w3.org/ns/r2rml#> .", encoding="utf-8")
    output = tmp_path / "out.nt"
    row = MappingTuple(
        {
            "subject": IRI("http://example.org/s"),
            "predicate": IRI("http://example.org/p"),
            "object": Literal("value"),
        }
    )

    monkeypatch.setattr(RunCommand, "_build_pipeline", lambda self, path: _Pipeline([row]))
    RunCommand().execute(Namespace(mapping=str(mapping), output=str(output), explain=False))

    assert "<http://example.org/s> <http://example.org/p> \"value\" ." in output.read_text(encoding="utf-8")


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_run_command_requires_existing_mapping_file():
    with pytest.raises(SystemExit) as exc:
        RunCommand().execute(Namespace(mapping="missing.ttl", output=None, explain=False))
    assert exc.value.code == 1


@pytest.mark.coverage_suite
def test_run_command_helpers_cover_serialization_paths(monkeypatch, tmp_path: Path):
    command = RunCommand()
    parser = __import__("argparse").ArgumentParser()
    command.configure_parser(parser)
    parsed = parser.parse_args(["-m", "mapping.ttl", "--explain"])
    assert parsed.explain is True
    assert command._resolve_output_path(None) is None
    assert isinstance(command._create_serializer(tmp_path / "out.nq"), NQuadsSerializer)
    assert isinstance(command._create_serializer(tmp_path / "out.nt"), NTriplesSerializer)
    assert isinstance(command._create_serializer(tmp_path / "out.ttl"), TurtleSerializer)

    serializer = type(
        "_Serializer",
        (),
        {"serialize": lambda self, row: row},
    )()
    entries = command._collect_entries(
        [
            ("line-1", ("s", "p", "o"), False),
            None,
            ("line-2", ("s", "p", "o"), True),
        ],
        serializer,
    )
    assert entries == [("line-1", ("s", "p", "o"), False), ("line-2", ("s", "p", "o"), True)]
    assert command._finalize_lines(entries) == ["line-2"]


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_run_command_writes_stdout_and_explain_mode(monkeypatch, capsys, tmp_path: Path):
    mapping = tmp_path / "mapping.ttl"
    mapping.write_text("@prefix rr: <http://www.w3.org/ns/r2rml#> .", encoding="utf-8")
    monkeypatch.setattr(RunCommand, "_build_pipeline", lambda self, path: _Pipeline([]))

    command = RunCommand()
    assert command._write_output(["a", "b"], None) == 2
    out = capsys.readouterr().out
    assert "a\nb\n" == out

    command.execute(Namespace(mapping=str(mapping), output=None, explain=True))
    assert "pipeline" in capsys.readouterr().out


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_run_command_exits_on_unexpected_errors(monkeypatch, tmp_path: Path):
    mapping = tmp_path / "mapping.ttl"
    mapping.write_text("@prefix rr: <http://www.w3.org/ns/r2rml#> .", encoding="utf-8")
    monkeypatch.setattr(RunCommand, "_build_pipeline", lambda self, path: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(SystemExit) as exc:
        RunCommand().execute(Namespace(mapping=str(mapping), output=None, explain=False))
    assert exc.value.code == 1


@pytest.mark.coverage_suite
def test_mapping_run_command_quotes_and_preserves_env_values(tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "OPENALEX_AUTHOR=Pascal Molli\n"
        "SERPAPI_KEY=abc123\n",
        encoding="utf-8",
    )

    command = MappingRunCommand()
    command._update_env_file(env_file, {"OPENALEX_AUTHOR": "Olaf Hartig"})

    content = env_file.read_text(encoding="utf-8")
    assert "OPENALEX_AUTHOR='Olaf Hartig'" in content
    assert "SERPAPI_KEY=abc123" in content

