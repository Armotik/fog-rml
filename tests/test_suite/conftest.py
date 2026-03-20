"""Shared pytest configuration for the rebuilt module-aligned test suite."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pytest
from rdflib import Dataset


def pytest_configure(config):
    """Registers the suite markers used by the rebuilt test suite."""
    config.addinivalue_line("markers", "coverage_suite: tests for the SonarQube coverage suite")
    config.addinivalue_line("markers", "edge_case: tests for robustness and unusual inputs")


def pytest_addoption(parser):
    """Adds the suite selector used by local runs and CI."""
    parser.addoption(
        "--suite",
        action="store",
        default="all",
        choices=("all", "coverage", "edge_case"),
        help="Run a specific test category: all, coverage, or edge_case.",
    )


def pytest_collection_modifyitems(config, items):
    """Filters collected tests according to the selected suite marker."""
    selected_suite = config.getoption("--suite")
    if selected_suite == "all":
        return

    wanted_marker = "coverage_suite" if selected_suite == "coverage" else "edge_case"
    kept = []
    deselected = []
    for item in items:
        if wanted_marker in item.keywords:
            kept.append(item)
        else:
            deselected.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
    items[:] = kept


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Prints a compact terminal summary aligned with the project workflow."""
    if exitstatus == 0:
        terminalreporter.write_sep("=", "TEST SUITE SUMMARY", green=True, bold=True)
        terminalreporter.write_line("All tests passed successfully!")
        terminalreporter.write_line("Debug traces available in test output.")
    else:
        terminalreporter.write_sep("=", "TEST SUITE SUMMARY", red=True, bold=True)
        terminalreporter.write_line(f"Tests completed with status: {exitstatus}")


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Returns the project root path."""
    return Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def data_dir(project_root: Path) -> Path:
    """Returns the repository data directory."""
    return project_root / "data"


@pytest.fixture()
def dataset() -> Dataset:
    """Provides a fresh rdflib Dataset for tests that need named-graph operations."""
    return Dataset()


@pytest.fixture()
def write_mapping_files(tmp_path: Path):
    """Creates a small mapping workspace from a mapping string and source files."""

    def _write(mapping_text: str, files: dict[str, str]) -> Path:
        for relative_path, content in files.items():
            target = tmp_path / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")

        mapping_path = tmp_path / "mapping.ttl"
        mapping_path.write_text(mapping_text, encoding="utf-8")
        return mapping_path

    return _write


@pytest.fixture()
def stream_to_list():
    """Materializes iterable operator results to a plain list for assertions."""

    def _materialize(rows: Iterable):
        return list(rows)

    return _materialize
