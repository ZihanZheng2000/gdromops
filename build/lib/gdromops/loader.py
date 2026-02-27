import os
from typing import Union

def _get_data_path(*paths: str) -> str:
    """
    Return the absolute path to a file inside the package's data directory.
    """
    base_dir = os.path.join(os.path.dirname(__file__), "data")
    return os.path.join(base_dir, *paths)


def _read_text(path: str) -> str:
    """
    Safely read text content from a file.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_ct_text(grand_id: Union[str, int]) -> str:
    """
    Load the classification tree (module condition) text
    for a given reservoir ID from data/module_conditions.
    """
    path = _get_data_path("module_conditions", f"{grand_id}.txt")
    return _read_text(path)


def load_module_text(grand_id: Union[str, int], module_id: Union[str, int]) -> str:
    """
    Load the module rule text for a specific reservoir and module ID
    from data/modules.
    """
    path = _get_data_path("modules", f"{grand_id}_{module_id}.txt")
    return _read_text(path)
