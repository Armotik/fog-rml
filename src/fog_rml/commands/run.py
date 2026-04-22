import sys
import time
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Iterable
import json
from typing import Optional

from fog_rml.commands.base import BaseCommand, logger
from fog_rml.mapping.MappingParser import MappingParser
from fog_rml.serializers.NQuadsSerializer import NQuadsSerializer
from fog_rml.serializers.NTriplesSerializer import NTriplesSerializer
from fog_rml.serializers.TurtleSerializer import TurtleSerializer

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
        if output_path and output_path.suffix.lower() in {".ttl", ".turtle"}:
            return TurtleSerializer()
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

class MappingRunCommand(RunCommand):
	@staticmethod
	def _parse_env_value(raw: str) -> str:
		import shlex

		value = raw.strip()
		if not value:
			return ""
		if value[0] in ("'", '"'):
			try:
				parts = shlex.split(value, posix=True)
			except ValueError:
				return value
			if parts:
				return parts[0]
		return value

	@staticmethod
	def _format_env_value(value: object) -> str:
		import shlex

		return shlex.quote("" if value is None else str(value))

	@classmethod
	def _load_env_file(cls, env_file: Path) -> dict:
		p = Path(env_file)
		existing = {}
		if not p.exists():
			return existing
		try:
			with open(p, 'r', encoding='utf-8') as f:
				for L in f:
					if '=' in L:
						k, v = L.split('=', 1)
						existing[k.strip()] = cls._parse_env_value(v)
		except Exception:
			return {}
		return existing

	def _write_json(self, obj, outpath: Path):
		p = Path(outpath)
		p.parent.mkdir(parents=True, exist_ok=True)
		with open(p, 'w', encoding='utf-8') as fh:
			json.dump(obj, fh, ensure_ascii=False, indent=2)

	def _update_env_file(self, env_file: Path, updates: dict):
		p = Path(env_file)
		existing = self._load_env_file(p)
		for k, v in updates.items():
			existing[k] = v
		p.parent.mkdir(parents=True, exist_ok=True)
		with open(p, 'w', encoding='utf-8') as f:
			for k, v in existing.items():
				f.write(f"{k}={self._format_env_value(v)}\n")

	def _apply_year_filter(self, raw: object, src_name: str, start: Optional[int], end: Optional[int]) -> object:
		def in_range(year):
			if year is None:
				return True
			try:
				y = int(year)
			except Exception:
				return True
			if start is not None and y < start:
				return False
			if end is not None and y > end:
				return False
			return True

		if src_name == 'openalex':
			results = raw.get('results') or raw.get('data') or []
			filtered = [r for r in results if in_range(r.get('publication_year'))]
			return {'results': filtered}
		if src_name == 'hal':
			docs = raw.get('response', {}).get('docs') or raw.get('docs') or []
			docs = [d for d in docs if in_range(d.get('producedDateY_i') or d.get('pubYear') or d.get('producedDateY'))]
			return {'response': {'docs': docs}}
		if src_name == 'dblp':
			hits = raw.get('result', {}).get('hits', {}).get('hit') or []
			new_hits = []
			for h in hits:
				info = h.get('info') or {}
				year = info.get('year')
				if in_range(year):
					new_hits.append(h)
			return {'result': {'hits': {'hit': new_hits}}}
		if src_name == 'serpapi':
			# SerpAPI rarely includes year info; keep as-is
			return raw
		return raw

	def _run_mapping_on_dir(self, mapping_path: str, outdir: Optional[Path], author: Optional[str] = None):
		outdir_p = Path(outdir) if outdir else Path('.')
		outdir_p.mkdir(parents=True, exist_ok=True)
		# Determine output filename: prefer sanitized author if provided, else fall back to mapping stem
		if author:
			# sanitize author to a safe filename: replace spaces with underscore and keep common safe chars
			import re
			safe = re.sub(r"[^A-Za-z0-9._-]", "_", author.strip())
			if not safe:
				safe = Path(mapping_path).stem
			output_file = outdir_p / (safe + '.nq')
		else:
			output_file = outdir_p / (Path(mapping_path).stem + '.nq')
		args = Namespace(mapping=mapping_path, output=str(output_file), explain=False)
		RunCommand().execute(args)
