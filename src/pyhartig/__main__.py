import argparse
import logging
import sys
import pkgutil
import importlib
import inspect
from typing import Dict, Type

from pyhartig.commands.base import BaseCommand

logger = logging.getLogger("pyhartig.cli")


def setup_logging(verbosity: int) -> None:
    """
    Configures the logging level based on verbosity.
    :param verbosity: Verbosity level (0=WARNING, 1=INFO (-v), 2=DEBUG (-vv))
    :return: None
    """
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG

    logging.basicConfig(
        level=level,
        format="[%(levelname)s] %(name)s - %(message)s",
        stream=sys.stderr
    )


def load_commands() -> Dict[str, Type[BaseCommand]]:
    """
    Dynamically discover and load command classes from the 'pyhartig.commands' package.
    Only classes inheriting from BaseCommand (and not BaseCommand itself) are loaded.
    :return: A dictionary mapping command names to their respective classes.
    """
    commands = {}
    package_path = "pyhartig.commands"

    # Import the package to locate it
    try:
        package = importlib.import_module(package_path)
    except ImportError:
        # Fallback if running from source without install
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
        package = importlib.import_module(package_path)

    # Iterates over all modules in the commands directory
    for _, module_name, _ in pkgutil.iter_modules(package.__path__):
        full_module_name = f"{package_path}.{module_name}"
        module = importlib.import_module(full_module_name)

        # Inspect module to find BaseCommand subclasses
        for name, obj in inspect.getmembers(module):
            if (inspect.isclass(obj)
                    and issubclass(obj, BaseCommand)
                    and obj is not BaseCommand):
                commands[obj.name] = obj

    return commands


def main():
    parser = argparse.ArgumentParser(
        description="PyHartig: An Algebra-Based RML Mapper engine."
    )

    parser.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Increase output verbosity (-v for INFO, -vv for DEBUG)"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # 1. Load available commands dynamically
    available_commands = load_commands()

    # 2. Register commands to argparse
    command_instances = {}
    for name, cmd_class in available_commands.items():
        cmd_instance = cmd_class()
        command_instances[name] = cmd_instance

        subparser = subparsers.add_parser(name, help=cmd_instance.help)
        cmd_instance.configure_parser(subparser)

    # 3. Parse arguments
    args = parser.parse_args()
    setup_logging(args.verbose)

    if args.command:
        # Execute the selected command
        command_instances[args.command].execute(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()