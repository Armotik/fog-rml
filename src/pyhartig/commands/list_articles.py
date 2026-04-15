import sys
import json
import subprocess
import platform
import os
import requests
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Optional

from fog_rml.commands.base import BaseCommand, logger
from fog_rml.commands.run import RunCommand, MappingRunCommand


class ListArticlesCommand(BaseCommand):
    """
    Fetch articles from multiple sources for an author, apply year filtering,
    then run the mapping pipeline on the fetched data.
    This delegates common helpers to `RunCommand` so logic is shared.
    """
    name = "list-articles"
    help = "Fetch articles for author and run mapping"

    def configure_parser(self, parser: ArgumentParser) -> None:
        parser.add_argument("author", help="Author name to search for (quoted) ")
        parser.add_argument("--start-year", type=int, help="Start year (optional)")
        parser.add_argument("--end-year", type=int, help="End year (optional)")
        parser.add_argument("--mapping", required=True, help="Path to the RML mapping file to apply")
        parser.add_argument("--outdir", default=None, help="Directory to write output and fetched JSON files")
        parser.add_argument("--sources", default="openalex,hal,dblp,serpapi",
                            help="Comma-separated sources to query (default: openalex,hal,dblp,serpapi)")

    # --- Fetch helpers (moved here from run.py) ---
    def _fetch_openalex(self, run_cmd, author: str, start: Optional[int], end: Optional[int], outpath: Path):
        base = 'https://api.openalex.org/works'
        attempts = []
        attempts.append({'per-page': 200, 'sort': 'publication_date:desc', 'search': f'authorships.author.display_name:{author}'})
        attempts.append({'per-page': 200, 'sort': 'publication_date:desc', 'search': f'author.display_name:{author}'})
        attempts.append({'per-page': 200, 'sort': 'publication_date:desc', 'search': author})
        attempts.append({'per-page': 200, 'sort': 'publication_date:desc', 'query': author})

        if start or end:
            df = []
            if start:
                df.append(f'from_publication_date:{start}-01-01')
            if end:
                df.append(f'to_publication_date:{end}-12-31')
            if df:
                date_filter = ','.join(df)
                for p in attempts:
                    p['filter'] = date_filter

        for params in attempts:
            try:
                resp = requests.get(base, params=params, timeout=60)
                logger.debug('OpenAlex request URL: %s status=%s', resp.request.url, resp.status_code)
                resp.raise_for_status()
                data = resp.json()
                run_cmd._write_json(data, outpath)
                results = data.get('results') or data.get('data') or []
                if results:
                    return
            except Exception:
                logger.debug('OpenAlex attempt failed', exc_info=True)
        logger.warning('OpenAlex returned no successful response for any query variant; skipping writing a file')
        return

    def _fetch_dblp(self, run_cmd, author: str, start: Optional[int], end: Optional[int], outpath: Path):
        url = 'https://dblp.org/search/publ/api'
        params = {'q': author, 'format': 'json', 'h': 200}
        if start is None:
            senv = os.environ.get('YEAR_START') or os.environ.get('OPENALEX_YEAR_START')
            try:
                start = int(senv) if senv else None
            except Exception:
                start = None
        if end is None:
            eenv = os.environ.get('YEAR_END') or os.environ.get('OPENALEX_YEAR_END')
            try:
                end = int(eenv) if eenv else None
            except Exception:
                end = None

        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        run_cmd._write_json(resp.json(), outpath)

    def _fetch_hal(self, run_cmd, author: str, start: Optional[int], end: Optional[int], outpath: Path):
        base = 'https://api.archives-ouvertes.fr/search/'
        params = {'q': f'authFullName_t:"{author}"', 'wt': 'json', 'rows': 200}
        if start is None:
            senv = os.environ.get('YEAR_START') or os.environ.get('OPENALEX_YEAR_START')
            try:
                start = int(senv) if senv else None
            except Exception:
                start = None
        if end is None:
            eenv = os.environ.get('YEAR_END') or os.environ.get('OPENALEX_YEAR_END')
            try:
                end = int(eenv) if eenv else None
            except Exception:
                end = None

        try:
            resp = requests.get(base, params=params, timeout=60)
            logger.debug('HAL request URL: %s status=%s', resp.request.url, resp.status_code)
            resp.raise_for_status()
            data = resp.json()
            docs = None
            if isinstance(data.get('response'), dict):
                docs = data['response'].get('docs')
            elif isinstance(data.get('docs'), list):
                docs = data.get('docs')

            num_found = data.get('response', {}).get('numFound') or data.get('numFound')
            if (not docs) and num_found:
                params_retry = dict(params)
                params_retry['start'] = 0
                params_retry['rows'] = max(int(params_retry.get('rows', 200)), 500)
                resp2 = requests.get(base, params=params_retry, timeout=60)
                logger.debug('HAL retry URL: %s status=%s', resp2.request.url, resp2.status_code)
                resp2.raise_for_status()
                data = resp2.json()

            run_cmd._write_json(data, outpath)
        except Exception:
            logger.exception('HAL fetch failed')
            return

    def _fetch_serpapi(self, run_cmd, author: str, start: Optional[int], end: Optional[int], outpath: Path):
        key = os.environ.get('SERPAPI_API_KEY') or os.environ.get('SERPAPI_KEY')
        if not key:
            logger.info('SERPAPI key not found in environment; skipping SerpAPI fetch')
            return
        url = 'https://serpapi.com/search.json'
        if start is None:
            senv = os.environ.get('YEAR_START') or os.environ.get('OPENALEX_YEAR_START')
            try:
                start = int(senv) if senv else None
            except Exception:
                start = None
        if end is None:
            eenv = os.environ.get('YEAR_END') or os.environ.get('OPENALEX_YEAR_END')
            try:
                end = int(eenv) if eenv else None
            except Exception:
                end = None

        params = {'q': author, 'api_key': key}
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        run_cmd._write_json(data, outpath)

    def execute(self, args: Namespace) -> None:
        run_cmd = MappingRunCommand()

        author = args.author
        start = getattr(args, 'start_year', None)
        end = getattr(args, 'end_year', None)
        mapping_arg = args.mapping
        if not mapping_arg:
            logger.critical('Mapping file required for list-articles (--mapping)')
            sys.exit(1)
        mapping_path = Path(mapping_arg).resolve()
        if not mapping_path.exists():
            logger.critical(f"Mapping file not found: {mapping_path}")
            sys.exit(1)

        # default to the mapping folder itself (do not create an 'out' subdirectory)
        outdir = Path(args.outdir) if args.outdir else mapping_path.parent
        data_dir = mapping_path.parent / 'data'
        sources = [s.strip().lower() for s in args.sources.split(',') if s.strip()]

        logger.info(f'Preparing data directory {data_dir} for fetches')
        try:
            if data_dir.exists():
                for p in data_dir.glob('*.json'):
                    try:
                        p.unlink()
                    except Exception:
                        logger.debug(f'Failed to remove {p}', exc_info=True)
            else:
                data_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            logger.debug('Error cleaning data directory', exc_info=True)

        # look for a fetch_all.sh either in the mapping folder or its parent (tests/use_cases/list_articles)
        fetch_script = None
        candidate1 = mapping_path.parent / 'fetch_all.sh'
        candidate2 = mapping_path.parent.parent / 'fetch_all.sh'
        if candidate1.exists():
            fetch_script = candidate1
        elif candidate2.exists():
            fetch_script = candidate2
        ran_script = False
        if fetch_script.exists():
            logger.info(f'Found fetch script {fetch_script}; attempting to run it')
            shell_cmds = []
            if platform.system() == 'Windows':
                shell_cmds.extend([
                    ['powershell', '-NoProfile', '-Command', '& ./fetch_all.sh'],
                    ['pwsh', '-NoProfile', '-Command', '& ./fetch_all.sh'],
                ])
            shell_cmds.extend([
                ['bash', str(fetch_script)],
                ['sh', str(fetch_script)],
            ])

            try:
                res = subprocess.run(str(fetch_script), shell=True, check=True, cwd=str(mapping_path.parent), capture_output=True, text=True)
                logger.info('Fetch script ran successfully via shell invocation')
                logger.debug('fetch stdout: %s', res.stdout)
                logger.debug('fetch stderr: %s', res.stderr)
                ran_script = True
            except subprocess.CalledProcessError as e:
                logger.warning('Fetch script failed with shell invocation: %s', e)
                logger.debug('fetch stdout: %s', getattr(e, 'stdout', None))
                logger.debug('fetch stderr: %s', getattr(e, 'stderr', None))
                for shell_cmd in shell_cmds:
                    try:
                        res = subprocess.run(shell_cmd, check=True, cwd=str(mapping_path.parent), capture_output=True, text=True)
                        ran_script = True
                        logger.info(f'Fetch script ran successfully with {shell_cmd[0]}')
                        logger.debug('fetch stdout: %s', res.stdout)
                        logger.debug('fetch stderr: %s', res.stderr)
                        break
                    except FileNotFoundError:
                        continue
                    except subprocess.CalledProcessError as e2:
                        logger.warning(f'Fetch script {fetch_script} failed when run with {shell_cmd[0]}: {e2}')
                        logger.debug('fetch stdout: %s', getattr(e2, 'stdout', None))
                        logger.debug('fetch stderr: %s', getattr(e2, 'stderr', None))
                        ran_script = False
                        break

        if not ran_script:
            logger.info(f'Fetching sources {sources} for author "{author}" into {data_dir}')
            for src in sources:
                try:
                    if src == 'openalex':
                        self._fetch_openalex(run_cmd, author, start, end, data_dir / 'openalex_works.json')
                    elif src == 'dblp':
                        self._fetch_dblp(run_cmd, author, start, end, data_dir / 'dblp_results.json')
                    elif src == 'hal':
                        self._fetch_hal(run_cmd, author, start, end, data_dir / 'hal_results.json')
                    elif src == 'serpapi':
                        self._fetch_serpapi(run_cmd, author, start, end, data_dir / 'serpapi_scholar.json')
                    else:
                        logger.info(f"Unknown source '{src}', skipping")
                except Exception as e:
                    logger.warning(f'Failed to fetch {src}: {e}')

        # Update .env to make fetch scripts consistent
        env_file = mapping_path.parent / '.env'
        updates = {}
        if author:
            updates['OPENALEX_AUTHOR'] = author
            updates['HAL_AUTHOR'] = author
            updates['DBLP_AUTHOR'] = author
            updates['SERPAPI_AUTHOR'] = author
        if start is not None:
            updates['OPENALEX_YEAR_START'] = str(start)
            updates['YEAR_START'] = str(start)
            updates['HAL_YEAR_START'] = str(start)
            updates['DBLP_YEAR_START'] = str(start)
            updates['SERPAPI_YEAR_START'] = str(start)
        if end is not None:
            updates['OPENALEX_YEAR_END'] = str(end)
            updates['YEAR_END'] = str(end)
            updates['HAL_YEAR_END'] = str(end)
            updates['DBLP_YEAR_END'] = str(end)
            updates['SERPAPI_YEAR_END'] = str(end)
        try:
            run_cmd._update_env_file(env_file, updates)
            logger.info(f'Updated .env at {env_file} with author/year')
        except Exception:
            logger.debug('Failed to update .env file', exc_info=True)

        # Apply year filter
        if start is not None or end is not None:
            logger.info('Applying year filter to fetched JSON files')
            src_map = {
                'openalex': data_dir / 'openalex_works.json',
                'hal': data_dir / 'hal_results.json',
                'dblp': data_dir / 'dblp_results.json',
                'serpapi': data_dir / 'serpapi_scholar.json'
            }
            for src_name, path in src_map.items():
                try:
                    if not path.exists():
                        continue
                    raw = json.loads(path.read_text(encoding='utf-8'))
                    filtered = run_cmd._apply_year_filter(raw, src_name, start, end)
                    if src_name == 'dblp' or filtered != raw:
                        run_cmd._write_json(filtered, path)
                        logger.info(f'Filtered {src_name} results -> {path.name}')
                except Exception:
                    logger.debug(f'Failed to apply year filter for {src_name}', exc_info=True)

        logger.info('Running mapping against fetched data...')
        try:
            run_cmd._run_mapping_on_dir(str(mapping_path), outdir, author)
        except Exception:
            logger.exception('Mapping execution failed after fetching')
            sys.exit(1)
