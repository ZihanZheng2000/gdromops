
import pandas as pd
import numpy as np
import os
import xarray as xr

from datetime import datetime

from gdromops.loader import load_ct_text, load_module_text
from gdromops.parser import build_ct_function_from_text, build_module_function_from_text

def _get_data_path(*paths: str) -> str:
    """Return absolute path to a file inside the package's data directory."""
    base_dir = os.path.join(os.path.dirname(__file__), "data")
    return os.path.join(base_dir, *paths)

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
    
    def GDROM_simulate_one_day(self, inflow: float, doy: int, pdsi: float, storage: float):
        
        """
        Simulate release and updated storage for a single day using GDROM rules.

        Parameters
        ----------
        inflow : float
            Daily inflow to the reservoir.
        doy : int
            Day of year (1–366).
        pdsi : float
            Palmer Drought Severity Index value for the day.
        storage : float
            Current storage before release.

        Returns
        -------
        release : float
            Simulated reservoir release for the day.
        new_storage : float
            Updated storage after applying inflow and release.
        """

        self._ensure_ct()
        module_id = self._ct(inflow, pdsi, doy, storage)
        mod = self._get_module(module_id)
        release = mod(inflow, storage)
        new_storage = storage + inflow - (release if release is not None else 0.0)
        return release, new_storage
    
    def GDROM_simulate_5min(self, inflow: float, doy: int, pdsi: float, storage: float):
        """
        Simulate release and updated storage for a single 5-minute interval using GDROM rules.

        Parameters
        ----------
        inflow : float
            5-minute inflow to the reservoir.
        """
        inflow_daily = inflow * 288  # Convert to daily inflow
        release_daily, _ = self.GDROM_simulate_one_day(
            inflow_daily, doy, pdsi, storage
        )
        release = release_daily / 288  # Convert back to 5-minute  
        new_storage = storage + inflow - (release if release is not None else 0.0)
        return release, new_storage
    
    def GDROM_simulate_1hr(self, inflow: float, doy: int, pdsi: float, storage: float):
        """
        Simulate release and updated storage for a single 1 hour interval using GDROM rules.

        Parameters
        ----------
        inflow : float
            1-hour inflow to the reservoir.
        """
        inflow_daily = inflow * 24  # Convert to daily inflow
        release_daily, _ = self.GDROM_simulate_one_day(
            inflow_daily, doy, pdsi, storage
        )
        release = release_daily / 24 # Convert back to 1 hr
        new_storage = storage + inflow - (release if release is not None else 0.0)
        return release, new_storage
    
    def GDROM_simulate_timestep(self, inflow: float, doy: int, pdsi: float, storage: float, timestep_hours: float):
        """
        Simulate release and updated storage for a single time step using GDROM rules.

        Parameters
        ----------
        inflow : float
            Inflow volume for the given timestep (e.g., 5min, 1hr, etc.).
        doy : int
            Day of year (1–366).
        pdsi : float
            Palmer Drought Severity Index value for the day.
        storage : float
            Current storage before release.
        timestep_hours : float
            Length of the timestep in hours (e.g., 1 for hourly, 0.0833 for 5min).

        Returns
        -------
        release : float
            Simulated release for the timestep.
        new_storage : float
            Updated storage after applying inflow and release.
        """

        inflow_daily = inflow * (24 / timestep_hours)
        release_daily, _ = self.GDROM_simulate_one_day(inflow_daily, doy, pdsi, storage)
        release = release_daily * (timestep_hours / 24)
        new_storage = storage + inflow - (release if release is not None else 0.0)

        return release, new_storage
        

    def GDROM_simulate(
        self,
        inflow_series: pd.Series,
        storage_series: pd.Series = None,
        pdsi_series: pd.Series = None,
        initial_storage: float = None,
        latitude: float = None,
        longitude: float = None,
        pdsi_nc_path: str = None,  
    ) -> pd.DataFrame:
        """
        Simulate using GDROM rules (with PDSI handling).
        """
        if not isinstance(inflow_series.index, pd.DatetimeIndex):
            raise ValueError("inflow_series must have a DatetimeIndex.")

        df = pd.DataFrame({"Inflow": inflow_series})
        df["DOY"] = df.index.dayofyear

        if storage_series is not None:
            df["Storage"] = storage_series

        # --- PDSI (simple & strict) ---
        if pdsi_series is not None:
            # Use provided daily PDSI directly
            df["PDSI"] = pdsi_series.reindex(df.index, method="nearest")
        else:
            # Load package-internal NetCDF (path is guaranteed to exist in your package)
            if pdsi_nc_path is None:
                pdsi_nc_path = _get_data_path("pdsi.mon.mean.nc")

            # Require lat/lon if no PDSI series was given
            if latitude is None or longitude is None:
                raise ValueError(
                    "latitude and longitude are required when pdsi_series is not provided."
                )

            # Extract nearest grid point for (lat, lon)
            with xr.open_dataset(pdsi_nc_path) as ds:
                point_series = ds["pdsi"].sel(lat=latitude, lon=longitude, method="nearest").to_series()

            # Monthly → daily via forward fill; align to inflow dates
            pdsi_daily = point_series.resample("D").ffill()
            df["PDSI"] = pdsi_daily.reindex(df.index, method="nearest")

        # Run simulation
        if storage_series is not None:
            releases = []
            for r in df.itertuples():
                release, _ = self.GDROM_simulate_one_day(r.Inflow, r.DOY, r.PDSI, r.Storage)
                releases.append(release)
            df["simulated_release"] = releases
            df["simulated_storage"] = storage_series

        else:
            if initial_storage is None:
                raise ValueError("Need initial_storage if Storage time series not provided.")
            temp_storage = initial_storage
            sim_releases, sim_storages = [], []
            for r in df.itertuples():
                release, new_storage = self.GDROM_simulate_one_day(r.Inflow, r.DOY, r.PDSI, temp_storage)
                sim_releases.append(release)
                sim_storages.append(temp_storage)
                temp_storage = new_storage
            df["simulated_release"] = sim_releases
            df["simulated_storage"] = sim_storages

        return df