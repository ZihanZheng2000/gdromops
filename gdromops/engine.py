
import pandas as pd
import numpy as np
from datetime import datetime
import xarray as xr


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

    def simulate_release(self, df: pd.DataFrame):
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

    # def simulate_release_and_storage(self, df: pd.DataFrame, initial_storage: float):
    #     out = df.copy()
    #     self._ensure_ct()
    #     pre_R, pre_S = [], []
    #     temp_storage = initial_storage
    #     for _, row in out.iterrows():
    #         inflow = float(row["Inflow"])
    #         doy = int(row["DOY"])
    #         pdsi = float(row["PDSI"])
    #         module_id = self._ct(inflow, pdsi, doy, temp_storage)
    #         mod = self._get_module(module_id)
    #         rel = mod(inflow, temp_storage)
    #         temp_storage = temp_storage + inflow - (rel if rel is not None else 0.0)
    #         pre_R.append(rel)
    #         pre_S.append(temp_storage)
    #     out["simulated_release"] = pre_R
    #     out["simulated_storage"] = pre_S
    #     return out

    def simulate_release_and_storage(self, df: pd.DataFrame, initial_storage: float):
        out = df.copy()
        self._ensure_ct()
        pre_R, pre_S = [], []
        temp_storage = initial_storage
        for _, row in out.iterrows():
            inflow = float(row["Inflow"])
            doy = int(row["DOY"])
            pdsi = float(row["PDSI"])
            module_id = self._ct(inflow, pdsi, doy, temp_storage)
            mod = self._get_module(module_id)
            rel = mod(inflow, temp_storage)

            # log BEFORE updating
            pre_R.append(rel)
            pre_S.append(temp_storage)

            # update storage for next step
            temp_storage = temp_storage + inflow - (rel if rel is not None else 0.0)

        out["simulated_release"] = pre_R
        out["simulated_storage"] = pre_S
        return out

    def GDROM_simulate(
        self,
        inflow_series: pd.Series,
        storage_series: pd.Series = None,
        pdsi_series: pd.Series = None,
        initial_storage: float = None,
        latitude: float = None,
        longitude: float = None,
        pdsi_nc_path: str = r"D:\GDROM\GDROM v2\gdromops\pdsi.mon.mean.nc"
    ) -> pd.DataFrame:
        
        """
        Simulate using GDROM rules.

        Parameters
        ----------
        inflow_series : pd.Series with DatetimeIndex (daily)
            Inflow time series.
        storage_series : pd.Series, optional
            Storage time series. If not provided, simulation will be dynamic using initial_storage.
        pdsi_series : pd.Series, optional
            PDSI time series. If not provided, it will be extracted from NetCDF using latitude & longitude.
        initial_storage : float, optional
            Required if storage_series is not provided.
        latitude : float, optional
            Latitude of the site, required if pdsi_series is None.
        longitude : float, optional
            Longitude of the site, required if pdsi_series is None.
        pdsi_nc_path : str
            Path to NetCDF PDSI dataset.

        Returns
        -------
        pd.DataFrame
            DataFrame with simulated results, indexed by Date.
        """
        if not isinstance(inflow_series.index, pd.DatetimeIndex):
            raise ValueError("inflow_series must have a DatetimeIndex.")

        df = pd.DataFrame({"Inflow": inflow_series})

        if storage_series is not None:
            df["Storage"] = storage_series

        # Handle PDSI
        if pdsi_series is not None:
            # Reindex provided PDSI to match inflow time axis
            df["PDSI"] = pdsi_series.reindex(df.index, method="nearest")
        else:
            if latitude is None or longitude is None:
                raise ValueError("Must provide latitude and longitude if pdsi_series is not given.")

            # Extract monthly PDSI from NetCDF
            ds = xr.open_dataset(pdsi_nc_path)
            point_series = ds['pdsi'].sel(lat=latitude, lon=longitude, method="nearest")
            pdsi_monthly = point_series.to_series()

            # Expand to daily frequency using forward fill
            pdsi_daily = pdsi_monthly.resample("D").ffill()

            # Reindex to match inflow dates
            df["PDSI"] = pdsi_daily.reindex(df.index, method="nearest")

        # Compute day of year
        df["DOY"] = df.index.dayofyear

        # Choose simulation mode
        if "Storage" in df.columns:
            return self.simulate_release(df)
        else:
            if initial_storage is None:
                raise ValueError("Need initial_storage if 'Storage' not provided.")
            return self.simulate_release_and_storage(df, initial_storage)