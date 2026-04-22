from abc import ABC, abstractmethod
from argparse import ArgumentParser, Namespace
import logging

# Logger for commands
logger = logging.getLogger("fog_rml.cli")

class BaseCommand(ABC):
    """
    Abstract base class for all fog-rml CLI commands.
    Developers can create new commands by inheriting from this class
    and placing them in the 'commands' package.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """
        The command name (used in CLI: fog-rml <name>)
        :return: Command name as a string
        """
        pass

    @property
    @abstractmethod
    def help(self) -> str:
        """
        Short description displayed in the help message
        :return: Help description as a string
        """
        pass

    @abstractmethod
    def configure_parser(self, parser: ArgumentParser) -> None:
        """
        Register arguments specific to this command.
        :param parser: The argparse subparser for this command.
        :return: None
        """
        pass

    @abstractmethod
    def execute(self, args: Namespace) -> None:
        """
        Execute the command logic.
        :param args: Parsed command-line arguments.
        :return: None
        """
        pass
