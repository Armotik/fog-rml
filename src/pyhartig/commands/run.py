import sys
import time
from argparse import ArgumentParser, Namespace
from pathlib import Path

from pyhartig.commands.base import BaseCommand, logger
from pyhartig.mapping.MappingParser import MappingParser
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
        mapping_path = Path(args.mapping)
        if not mapping_path.exists():
            logger.critical(f"Mapping file not found: {mapping_path}")
            sys.exit(1)

        try:
            logger.info(f"Initializing MappingParser for {mapping_path}")
            start_time = time.time()

            parser_engine = MappingParser(str(mapping_path))
            pipeline = parser_engine.parse()

            # Explain mode
            if args.explain:
                print(pipeline.explain())
                return

            # Execute pipeline
            logger.info("Executing algebraic pipeline...")
            results_iterator = pipeline.execute()

            # Output handling
            out_stream = sys.stdout
            if args.output:
                out_path = Path(args.output)
                logger.info(f"Writing output to file: {out_path}")
                out_stream = out_path.open("w", encoding="utf-8")
            else:
                logger.info("Writing output to stdout")

            # Choose serializer by output extension: use N-Quads when writing .nq
            from pathlib import Path as _Path
            out_path = _Path(args.output) if args.output else None
            if out_path and out_path.suffix.lower() == '.nq':
                from pyhartig.serializers.NQuadsSerializer import NQuadsSerializer
                serializer = NQuadsSerializer()
            else:
                serializer = NTriplesSerializer()

            # Buffer serialized rows so we can prefer quads over plain triples
            entries = []  # list of (line, key, is_quad)
            try:
                for row in results_iterator:
                    res = serializer.serialize(row)
                    if not res:
                        continue
                    line, key, is_quad = res
                    entries.append((line, key, is_quad))
            finally:
                if args.output and out_stream is not sys.stdout:
                    out_stream.close()

            # Post-process: if a triple (s,p,o) also exists as a quad, drop the triple
            quad_keys = {key for (_l, key, is_q) in entries if is_q}
            final_lines = []
            seen = set()
            for line, key, is_q in entries:
                # skip plain triple when a quad exists for same (s,p,o)
                if (not is_q) and (key in quad_keys):
                    continue
                if line in seen:
                    continue
                seen.add(line)
                final_lines.append(line)

            # Write out final lines
            out_stream = sys.stdout
            if args.output:
                out_path = Path(args.output)
                out_stream = out_path.open("w", encoding="utf-8")

            try:
                for i, line in enumerate(final_lines, start=1):
                    out_stream.write(line + "\n")
                    if i % 1000 == 0:
                        logger.info(f"Generated {i} triples/quads...")
                count = len(final_lines)
            finally:
                if args.output and out_stream is not sys.stdout:
                    out_stream.close()

            duration = time.time() - start_time
            logger.info(f"Success. Generated {count} triples in {duration:.2f}s.")

        except Exception:
            logger.exception("An unexpected error occurred during execution.")
            sys.exit(1)