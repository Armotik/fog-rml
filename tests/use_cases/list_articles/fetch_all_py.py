import json
import logging
import shlex
from pathlib import Path
import requests

BASE = Path(__file__).parent
logger = logging.getLogger(__name__)


def _parse_env_value(raw: str) -> str:
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


def _load_env_files() -> dict:
    env = {}
    # Prefer the checked-in data/.env for shared secrets, then let the
    # command-generated parent .env override author/year values.
    for candidate in [BASE / 'data' / '.env', BASE / '.env']:
        try:
            if candidate.is_file():
                for line in candidate.read_text(encoding='utf-8').splitlines():
                    if '=' in line:
                        k, v = line.split('=', 1)
                        env[k.strip()] = _parse_env_value(v)
        except FileNotFoundError:
            continue
    return env


env = _load_env_files()

DATA_DIR = BASE / 'data'

def write(path: Path, text: str):
    (DATA_DIR).mkdir(parents=True, exist_ok=True)
    target = DATA_DIR / path
    target.write_text(text, encoding='utf-8')

def fetch_openalex():
    author = env.get('OPENALEX_AUTHOR')
    start = env.get('OPENALEX_YEAR_START')
    end = env.get('OPENALEX_YEAR_END')
    q = author.replace(' ', '%20')
    from_date = f"{start}-01-01"
    until_date = f"{end}-12-31"
    url = f"https://api.openalex.org/works?filter=raw_author_name.search:{q},from_publication_date:{from_date},to_publication_date:{until_date}&per-page=200"
    r = requests.get(url, timeout=30)
    write(Path('openalex_works.json'), r.text)
    logger.info("openalex saved %s", DATA_DIR / 'openalex_works.json')

def fetch_hal():
    author = env.get('HAL_AUTHOR')
    q = author.replace(' ', '+')
    url = f"https://api.archives-ouvertes.fr/search/?q=authFullName_t:{q}&wt=json&rows=100"
    r = requests.get(url, timeout=30)
    write(Path('hal_results.json'), r.text)
    logger.info("hal saved %s", DATA_DIR / 'hal_results.json')

def fetch_dblp():
    author = env.get('DBLP_AUTHOR')
    q = author.replace(' ', '+')
    url = f"https://dblp.org/search/publ/api?q={q}&format=json"
    r = requests.get(url, timeout=30)
    write(Path('dblp_results.json'), r.text)
    logger.info("dblp saved %s", DATA_DIR / 'dblp_results.json')

def fetch_serpapi():
    key = env.get('SERPAPI_KEY')
    author = env.get('SERPAPI_AUTHOR') or env.get('OPENALEX_AUTHOR')
    q = author.replace(' ', '+')
    url = f"https://serpapi.com/search.json?engine=google_scholar&q={q}&api_key={key}"
    r = requests.get(url, timeout=30)
    write(Path('serpapi_scholar.json'), r.text)
    logger.info("serpapi saved %s", DATA_DIR / 'serpapi_scholar.json')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    fetch_openalex()
    fetch_hal()
    fetch_dblp()
    fetch_serpapi()
