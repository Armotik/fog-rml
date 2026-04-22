#!/usr/bin/env python3
"""Run the mirrored pytest suite and print a compact structure summary."""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    """Configure simple stderr logging for the helper script."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")


def print_section(title: str, char: str = "=") -> None:
    """Print a formatted section header."""
    width = 80
    logger.info("")
    logger.info("%s", char * width)
    logger.info("%s", f"{title:^{width}}")
    logger.info("%s", char * width)
    logger.info("")


def list_test_modules(test_root: Path) -> list[Path]:
    """Return all mirrored test modules under the `fog_rml` test tree."""
    return sorted(path for path in (test_root / "fog_rml").rglob("test_*.py"))


def generate_test_summary(test_root: Path) -> None:
    """Print the mirrored test-suite structure."""
    print_section("Mirrored Test Modules", "-")
    test_modules = list_test_modules(test_root)
    logger.info("Total mirrored test modules: %s", len(test_modules))
    logger.info("")
    for path in test_modules:
        logger.info("- %s", path.relative_to(test_root))
    logger.info("")
    logger.info("Categories:")
    logger.info("- coverage: module-level coverage suite used by SonarQube")
    logger.info("- edge_case: robustness suite focused on unusual or invalid inputs")


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

    print_section(f"fog-rml {suite.title()} Suite Execution")
    logger.info("Command: %s", " ".join(cmd))
    logger.info("Timestamp: %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    logger.info("Test root: %s", test_root)
    logger.info("")

    try:
        result = subprocess.run(cmd, cwd=test_root.parent.parent, capture_output=False, text=True)
    except Exception as exc:
        logger.error("Error while running pytest: %s", exc)
        return 1
    return int(result.returncode)


def parse_args():
    """Parse the CLI arguments of the local test runner."""
    parser = argparse.ArgumentParser(description="Run the mirrored fog-rml pytest suite.")
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
    configure_logging()
    test_root = Path(__file__).parent

    print_section("fog-rml Mirrored Test Suite")
    generate_test_summary(test_root)
    return_code = run_tests_with_output(test_root, args.suite)

    print_section("Execution Complete")
    if return_code == 0:
        logger.info("All tests passed successfully.")
    else:
        logger.info("Tests completed with errors (exit code: %s).", return_code)
    return int(return_code)


if __name__ == "__main__":
    sys.exit(main())

