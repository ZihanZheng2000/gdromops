
import os
from pathlib import Path

def cache_dir() -> Path:
    d = Path(os.getenv("GDROMOPS_CACHE", Path.home() / ".gdromops" / "rules"))
    d.mkdir(parents=True, exist_ok=True)
    return d

def cache_path(kind: str, name: str) -> Path:
    d = cache_dir() / kind
    d.mkdir(parents=True, exist_ok=True)
    return d / name
