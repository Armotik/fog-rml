from __future__ import annotations

from argparse import Namespace

import pytest

from pyhartig import __main__
from pyhartig.commands.base import BaseCommand


class _DummyCommand(BaseCommand):
    name = "dummy"
    help = "dummy command"

    def __init__(self):
        self.executed_with = None

    def configure_parser(self, parser):
        parser.add_argument("--flag", action="store_true")

    def execute(self, args: Namespace) -> None:
        self.executed_with = args


@pytest.mark.coverage_suite
def test_main_loads_commands_and_dispatches(monkeypatch):
    command = _DummyCommand()
    monkeypatch.setattr(__main__, "load_commands", lambda: {"dummy": lambda: command})
    monkeypatch.setattr(__main__.sys, "argv", ["pyhartig", "dummy", "--flag"])
    __main__.main()
    assert command.executed_with.flag is True


@pytest.mark.coverage_suite
@pytest.mark.edge_case
def test_main_exits_when_no_command_is_selected(monkeypatch):
    monkeypatch.setattr(__main__, "load_commands", lambda: {})
    monkeypatch.setattr(__main__.sys, "argv", ["pyhartig"])
    with pytest.raises(SystemExit) as exc:
        __main__.main()
    assert exc.value.code == 1
