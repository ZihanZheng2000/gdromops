
import requests
from .config import DEFAULT_RULES_BASE, TIMEOUT
from .cache import cache_path

def _url(kind: str, name: str) -> str:
    return f"{DEFAULT_RULES_BASE}/{kind}/{name}"

def fetch_text(kind: str, name: str, use_cache=True) -> str:
    cp = cache_path(kind, name)
    if use_cache and cp.exists():
        return cp.read_text(encoding="utf-8")
    url = _url(kind, name)
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    text = r.text
    cp.write_text(text, encoding="utf-8")
    return text

def load_ct_text(grand_id: str | int) -> str:
    return fetch_text("module_conditions", f"{grand_id}.txt")

def load_module_text(grand_id: str | int, module_id: str | int) -> str:
    return fetch_text("modules", f"{grand_id}_{module_id}.txt")
