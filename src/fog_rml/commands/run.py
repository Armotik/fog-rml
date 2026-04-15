
"""Compatibility shim: provide a MappingRunCommand that adapts
to the older expectations of `list_articles` while delegating
execution to `pyhartig.commands.run.RunCommand`.
"""
import json
from argparse import Namespace
from pathlib import Path
from typing import Optional

from pyhartig.commands.run import RunCommand


class MappingRunCommand(RunCommand):
	def _write_json(self, obj, outpath: Path):
		p = Path(outpath)
		p.parent.mkdir(parents=True, exist_ok=True)
		with open(p, 'w', encoding='utf-8') as fh:
			json.dump(obj, fh, ensure_ascii=False, indent=2)

	def _update_env_file(self, env_file: Path, updates: dict):
		p = Path(env_file)
		existing = {}
		if p.exists():
			try:
				with open(p, 'r', encoding='utf-8') as f:
					for L in f:
						if '=' in L:
							k, v = L.split('=', 1)
							existing[k.strip()] = v.strip()
			except Exception:
				existing = {}
		for k, v in updates.items():
			existing[k] = v
		p.parent.mkdir(parents=True, exist_ok=True)
		with open(p, 'w', encoding='utf-8') as f:
			for k, v in existing.items():
				f.write(f"{k}={v}\n")

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

