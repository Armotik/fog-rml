import sys
import time
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Iterable

from pyhartig.commands.base import BaseCommand, logger
from pyhartig.mapping.MappingParser import MappingParser
from pyhartig.serializers.NQuadsSerializer import NQuadsSerializer
from pyhartig.serializers.NTriplesSerializer import NTriplesSerializer

class RunCommand(BaseCommand):
    name = "run"
    help = "Execute a standard RML mapping file"

    def configure_parser(self, parser: ArgumentParser) -> None:
        """
        Configure command-line arguments for the 'run' command.
        :param parser: The argparse subparser for this command.
        :return: None
        """
        parser.add_argument(
            "-m", "--mapping",
            required=True,
            help="Path to the RML mapping file (.ttl, .rml)"
        )
        parser.add_argument(
            "-o", "--output",
            help="Path to output file (default: stdout)",
            default=None
        )
        parser.add_argument(
            "--explain",
            action="store_true",
            help="Print the algebraic execution plan structure instead of running it"
        )

    def execute(self, args: Namespace) -> None:
        """
        Execute the 'run' command logic.
        :param args: Parsed command-line arguments.
        :return: None
        """
        try:
            start_time = time.time()
            mapping_path = self._require_mapping_path(args.mapping)
            pipeline = self._build_pipeline(mapping_path)
            if args.explain:
                print(pipeline.explain())
                return

            logger.info("Executing algebraic pipeline...")
            output_path = self._resolve_output_path(args.output)
            serializer = self._create_serializer(output_path)
            entries = self._collect_entries(pipeline.execute(), serializer)
            final_lines = self._finalize_lines(entries)
            count = self._write_output(final_lines, output_path)
            self._log_success(count, start_time)
        except Exception:
            logger.exception("An unexpected error occurred during execution.")
            sys.exit(1)

    def _require_mapping_path(self, mapping_arg: str) -> Path:
        mapping_path = Path(mapping_arg)
        if not mapping_path.exists():
            logger.critical(f"Mapping file not found: {mapping_path}")
            sys.exit(1)
        return mapping_path

    def _build_pipeline(self, mapping_path: Path):
        logger.info(f"Initializing MappingParser for {mapping_path}")
        parser_engine = MappingParser(str(mapping_path))
        return parser_engine.parse()

    def _resolve_output_path(self, output_arg: str | None) -> Path | None:
        if output_arg:
            output_path = Path(output_arg)
            logger.info(f"Writing output to file: {output_path}")
            return output_path
        logger.info("Writing output to stdout")
        return None

    def _create_serializer(self, output_path: Path | None):
        if output_path and output_path.suffix.lower() == ".nq":
            return NQuadsSerializer()
        return NTriplesSerializer()

    def _collect_entries(self, results_iterator: Iterable, serializer) -> list[tuple[str, tuple[str, str, str], bool]]:
        entries = []
        for row in results_iterator:
            serialized_row = serializer.serialize(row)
            if not serialized_row:
                continue
            line, key, is_quad = serialized_row
            entries.append((line, key, is_quad))
        return entries

    def _finalize_lines(self, entries: list[tuple[str, tuple[str, str, str], bool]]) -> list[str]:
        quad_keys = {key for (_line, key, is_quad) in entries if is_quad}
        final_lines = []
        seen_lines = set()

        for line, key, is_quad in entries:
            if (not is_quad) and (key in quad_keys):
                continue
            if line in seen_lines:
                continue
            seen_lines.add(line)
            final_lines.append(line)

        return final_lines

    def _write_output(self, final_lines: list[str], output_path: Path | None) -> int:
        out_stream = sys.stdout if output_path is None else output_path.open("w", encoding="utf-8")
        try:
            for i, line in enumerate(final_lines, start=1):
                out_stream.write(line + "\n")
                if i % 1000 == 0:
                    logger.info(f"Generated {i} triples/quads...")
            return len(final_lines)
        finally:
            if out_stream is not sys.stdout:
                out_stream.close()

    def _log_success(self, count: int, start_time: float) -> None:
        duration = time.time() - start_time
        logger.info(f"Success. Generated {count} triples in {duration:.2f}s.")
