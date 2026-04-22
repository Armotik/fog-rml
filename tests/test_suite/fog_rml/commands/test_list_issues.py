from __future__ import annotations

import io
import json
from argparse import ArgumentParser
from argparse import Namespace
from pathlib import Path

import pytest

from fog_rml.commands.list_issues import ListIssuesCommand


@pytest.mark.coverage_suite
def test_list_issues_converts_repo_urls_and_merges_payloads(monkeypatch, tmp_path: Path):
    command = ListIssuesCommand()

    class _Response(io.StringIO):
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda request: _Response(json.dumps([{"id": 1}, {"id": 2}])),
    )

    merged_path = command._fetch_and_merge(["https://github.com/acme/demo"])
    merged = json.loads(Path(merged_path).read_text(encoding="utf-8"))
    assert command._convert_repo_url_to_api("https://github.com/acme/demo").endswith("/issues?state=all")
    assert command._convert_repo_url_to_api("https://gitlab.example.org/group/demo").endswith("/api/v4/projects/group%2Fdemo/issues")
    assert len(merged) == 2


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_list_issues_fails_when_no_supported_repo_is_provided(tmp_path: Path):
    command = ListIssuesCommand()
    mapping = tmp_path / "mapping.ttl"
    mapping.write_text("", encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        command.execute(Namespace(repos=["https://example.org/repo"], mapping=str(mapping), output=None, explain=False))
    assert exc.value.code == 1


@pytest.mark.coverage_suite
def test_list_issues_configures_parser_and_executes_full_flow(monkeypatch, tmp_path: Path):
    command = ListIssuesCommand()
    parser = ArgumentParser()
    command.configure_parser(parser)
    parsed = parser.parse_args(["https://github.com/acme/demo", "-m", "mapping.ttl", "--explain"])
    assert parsed.explain is True

    mapping = tmp_path / "mapping.ttl"
    mapping.write_text("github={{GITHUB_SOURCE}}\ngitlab={{GITLAB_SOURCE}}", encoding="utf-8")

    created = []

    def _fake_fetch(urls):
        path = tmp_path / f"{len(created)}.json"
        path.write_text(json.dumps([{"id": len(created)}]), encoding="utf-8")
        created.append(path)
        return str(path)

    executed = {}

    class _RunCommand:
        def execute(self, args):
            executed["mapping"] = args.mapping
            executed["output"] = args.output
            executed["explain"] = args.explain
            executed["content"] = Path(args.mapping).read_text(encoding="utf-8")

    monkeypatch.setattr(command, "_fetch_and_merge", _fake_fetch)
    monkeypatch.setattr("fog_rml.commands.list_issues.RunCommand", _RunCommand)

    command.execute(
        Namespace(
            repos=["https://github.com/acme/demo", "https://gitlab.com/acme/demo"],
            mapping=str(mapping),
            output="out.nt",
            explain=True,
        )
    )

    assert "github=" in executed["content"]
    assert "gitlab=" in executed["content"]
    assert executed["output"] == "out.nt"
    assert executed["explain"] is True


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_list_issues_handles_fetch_errors_and_non_list_payloads(monkeypatch):
    command = ListIssuesCommand()

    class _Response(io.StringIO):
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    calls = {"count": 0}

    def _fake_urlopen(_request):
        calls["count"] += 1
        if calls["count"] == 1:
            return _Response(json.dumps({"error": "unexpected"}))
        raise RuntimeError("network error")

    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen)
    merged_path = command._fetch_and_merge(["https://github.com/acme/demo", "https://example.org/raw"])
    assert json.loads(Path(merged_path).read_text(encoding="utf-8")) == []
