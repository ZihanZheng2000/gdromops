
from .engine import RuleEngine

__all__ = ["RuleEngine", "train_res_r_from_paths"]


def train_res_r_from_paths(*args, **kwargs):
    from .training import train_res_r_from_paths as _train_res_r_from_paths

    return _train_res_r_from_paths(*args, **kwargs)
