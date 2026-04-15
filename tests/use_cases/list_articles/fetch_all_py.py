import json
from pathlib import Path
import requests

BASE = Path(__file__).parent
env = {}
# load possible .env files: parent folder and data/ subfolder (data/.env commonly used)
for candidate in [BASE / '.env', BASE / 'data' / '.env', BASE / 'data' ]:
    try:
        if candidate.is_file():
            for line in candidate.read_text(encoding='utf-8').splitlines():
                if '=' in line:
                    k,v = line.split('=',1)
                    env[k.strip()] = v.strip()
    except FileNotFoundError:
        continue

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
    print('openalex saved', (DATA_DIR / 'openalex_works.json'))

def fetch_hal():
    author = env.get('HAL_AUTHOR')
    q = author.replace(' ', '+')
    url = f"https://api.archives-ouvertes.fr/search/?q=authFullName_t:{q}&wt=json&rows=100"
    r = requests.get(url, timeout=30)
    write(Path('hal_results.json'), r.text)
    print('hal saved', (DATA_DIR / 'hal_results.json'))

def fetch_dblp():
    author = env.get('DBLP_AUTHOR')
    q = author.replace(' ', '+')
    url = f"https://dblp.org/search/publ/api?q={q}&format=json"
    r = requests.get(url, timeout=30)
    write(Path('dblp_results.json'), r.text)
    print('dblp saved', (DATA_DIR / 'dblp_results.json'))

def fetch_serpapi():
    key = env.get('SERPAPI_KEY')
    author = env.get('SERPAPI_AUTHOR') or env.get('OPENALEX_AUTHOR')
    q = author.replace(' ', '+')
    url = f"https://serpapi.com/search.json?engine=google_scholar&q={q}&api_key={key}"
    r = requests.get(url, timeout=30)
    write(Path('serpapi_scholar.json'), r.text)
    print('serpapi saved', (DATA_DIR / 'serpapi_scholar.json'))

if __name__ == '__main__':
    fetch_openalex()
    fetch_hal()
    fetch_dblp()
    fetch_serpapi()
