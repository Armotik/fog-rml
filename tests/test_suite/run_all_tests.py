#!/usr/bin/env python3
"""Run the mirrored pytest suite and print a compact structure summary."""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def print_section(title: str, char: str = "=") -> None:
    """Print a formatted section header."""
    width = 80
    print(f"\n{char * width}")
    print(f"{title:^{width}}")
    print(f"{char * width}\n")


def list_test_modules(test_root: Path) -> list[Path]:
    """Return all mirrored test modules under the `pyhartig` test tree."""
    return sorted(path for path in (test_root / "pyhartig").rglob("test_*.py"))


def generate_test_summary(test_root: Path) -> None:
    """Print the mirrored test-suite structure."""
    print_section("Mirrored Test Modules", "-")
    test_modules = list_test_modules(test_root)
    print(f"Total mirrored test modules: {len(test_modules)}\n")
    for path in test_modules:
        print(f"- {path.relative_to(test_root)}")
    print("\nCategories:")
    print("- coverage: module-level coverage suite used by SonarQube")
    print("- edge_case: robustness suite focused on unusual or invalid inputs")


def run_tests_with_output(test_root: Path, suite: str) -> int:
    """Execute pytest for the selected suite and stream output directly."""
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        str(test_root),
        "-v",
        "-s",
        "--tb=short",
        "--color=yes",
        "-p",
        "no:warnings",
    ]
    if suite != "all":
        cmd.extend(["--suite", suite])

    print_section(f"PyHartig {suite.title()} Suite Execution")
    print(f"Command: {' '.join(cmd)}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Test root: {test_root}\n")

    try:
        result = subprocess.run(cmd, cwd=test_root.parent.parent, capture_output=False, text=True)
    except Exception as exc:
        print(f"Error while running pytest: {exc}")
        return 1
    return int(result.returncode)


def parse_args():
    """Parse the CLI arguments of the local test runner."""
    parser = argparse.ArgumentParser(description="Run the mirrored PyHartig pytest suite.")
    parser.add_argument(
        "--suite",
        choices=("all", "coverage", "edge_case"),
        default="all",
        help="Test category to execute.",
    )
    return parser.parse_args()


def main() -> int:
    """Run the requested suite and return a shell-friendly exit code."""
    args = parse_args()
    test_root = Path(__file__).parent

    print_section("PyHartig Mirrored Test Suite")
    generate_test_summary(test_root)
    return_code = run_tests_with_output(test_root, args.suite)

    print_section("Execution Complete")
    if return_code == 0:
        print("All tests passed successfully.")
    else:
        print(f"Tests completed with errors (exit code: {return_code}).")
    return int(return_code)


if __name__ == "__main__":
    sys.exit(main())
