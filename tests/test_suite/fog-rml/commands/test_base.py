from __future__ import annotations

from argparse import ArgumentParser, Namespace

import pytest

from fog_rml.commands.base import BaseCommand


class _Command(BaseCommand):
    name = "demo"
    help = "demo"

    def configure_parser(self, parser: ArgumentParser) -> None:
        parser.add_argument("--value")

    def execute(self, args: Namespace) -> None:
        self.args = args


@pytest.mark.coverage_suite
def test_base_command_contract_can_be_implemented():
    parser = ArgumentParser()
    command = _Command()
    command.configure_parser(parser)
    args = parser.parse_args(["--value", "42"])
    command.execute(args)
    assert command.args.value == "42"


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_base_command_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        BaseCommand()

