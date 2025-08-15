
import pandas as pd
from .loader import load_ct_text, load_module_text
from .parser import build_ct_function_from_text, build_module_function_from_text

class RuleEngine:
    def __init__(self, grand_id: str | int):
        self.grand_id = str(grand_id)
        self._ct = None
        self._modules = {}

    def _ensure_ct(self):
        if self._ct is None:
            ct_txt = load_ct_text(self.grand_id)
            self._ct = build_ct_function_from_text(self.grand_id, ct_txt)

    def _get_module(self, module_id):
        mid = "0" if module_id in (None, "") else str(module_id)
        if mid not in self._modules:
            txt = load_module_text(self.grand_id, mid)
            self._modules[mid] = build_module_function_from_text(self.grand_id, mid, txt)
        return self._modules[mid]

    def simulate_release(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        self._ensure_ct()
        sim = []
        for _, row in out.iterrows():
            inflow = float(row["Inflow"])
            storage = float(row["Storage"])
            doy = int(row["DOY"])
            pdsi = float(row["PDSI"])
            module_id = self._ct(inflow, pdsi, doy, storage)
            mod = self._get_module(module_id)
            sim.append(mod(inflow, storage))
        out["simulated_release"] = sim
        return out

    def simulate_release_and_storage(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        self._ensure_ct()
        pre_R, pre_S = [], []
        temp_storage = float(out["Storage"].iloc[0])
        for _, row in out.iterrows():
            inflow = float(row["Inflow"])
            doy = int(row["DOY"])
            pdsi = float(row["PDSI"])
            module_id = self._ct(inflow, pdsi, doy, temp_storage)
            mod = self._get_module(module_id)
            rel = mod(inflow, temp_storage)
            temp_storage = temp_storage + inflow - (rel if rel is not None else 0.0)
            pre_R.append(rel)
            pre_S.append(temp_storage)
        out["simulated_release"] = pre_R
        out["simulated_storage"] = pre_S
        return out
