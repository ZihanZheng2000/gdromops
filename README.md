# gdromops

`gdromops` provides a reproducible and lightweight framework for applying pre-trained GDROM rules to reservoir operation simulation.  
It is designed for research and applications in large-scale reservoir system modeling, water resources management, and data-driven hydrology.

---

## Installation

Install from PyPI (stable release):

```bash
pip install gdromops
```

Or from GitHub (latest development version):

```bash
pip install git+https://github.com/ZihanZheng2000/gdromops.git
```

---

## Package Structure

```
gdromops/
├── __init__.py
├── engine.py          # Core simulation engine
├── loader.py          # Rule loading utilities
├── parser.py          # Text → callable conversion
├── data/
│   ├── pdsi.mon.mean.nc              # Internal PDSI dataset
│   ├── module_conditions/            # Decision-tree conditions
│   └── modules/                      # Rule modules (release formulas)
└── ...
tests/
├── test_demo_reservoir41.py          # Example usage
├── example_data_reservoir41.csv
```

---

## Quick Start

```python
import pandas as pd
from gdromops import RuleEngine

# Initialize RuleEngine for a given reservoir
engine = RuleEngine("41")

# Load input data
df = pd.read_csv("example_data_reservoir41.csv", parse_dates=["Date"]).set_index("Date")

inflow = df["Inflow"]
storage = df["Storage"]
pdsi = df["PDSI"] if "PDSI" in df.columns else None
initial_storage = float(storage.iloc[0])

# ---- Case 1: Single-day simulation ----
date = df.index[0]
release, new_storage = engine.GDROM_simulate_one_day(
    inflow=float(df.loc[date, "Inflow"]),
    doy=int(date.dayofyear),
    pdsi=float(df.loc[date, "PDSI"]),
    storage=float(df.loc[date, "Storage"])
)
print(f"Release = {release:.2f}, New Storage = {new_storage:.2f}")

# ---- Case 2: Multi-day (with observed storage) ----
result_case2 = engine.GDROM_simulate(inflow_series=inflow, storage_series=storage, pdsi_series=pdsi)

# ---- Case 3: Multi-day (with initial storage only) ----
result_case3 = engine.GDROM_simulate(inflow_series=inflow, initial_storage=initial_storage, pdsi_series=pdsi)

# ---- Case 4: Automatic PDSI extraction from lat/lon ----
result_case4 = engine.GDROM_simulate(
    inflow_series=inflow,
    initial_storage=initial_storage,
    latitude=48.7325,
    longitude=-121.0673,
)
```

---

## Output Example

Each simulation returns a DataFrame with:

| Column | Description |
|:---|:---|
| Inflow | Daily inflow (input) |
| PDSI | Palmer Drought Severity Index |
| DOY | Day of year |
| simulated_release | GDROM-predicted release |
| simulated_storage | Updated storage (after inflow/release balance) |

---

## Demo

Example data (`example_data_reservoir41.csv`) and test script (`test_demo_reservoir41.py`) are included under `tests/`.  
Run all four demo cases with:

```bash
python -m tests.test_demo_reservoir41
```

---

## Features

- Hybrid rule-based simulation (CART + module functions)  
- Automatic PDSI lookup from NetCDF (lat/lon)  
- Support for data-rich and data-limited reservoirs  
- Seamless integration with GDROM v2 dataset  
- Lightweight and fully open-source

---

## Citation

If you use `gdromops` or the GDROM v2 dataset in your research, please cite the dataset or the software:

**Dataset citation**  
Zheng, Z., X. Cai, Y. Chen (2025). GDROM v2: An Inventory of Operation Variables Time Series and Rules for 2,017 Large Reservoirs across the CONUS, HydroShare, https://doi.org/10.4211/hs.5293674cb83b4ec698db0eb4777467b8

**Software citation**  
Zheng, Z., et al. (2025). gdromops: A Python package for simulating reservoir operations using GDROM rules.
Journal of Open Source Software. Under Review.

---

## License

This project is released under the **MIT License**.  
See [`LICENSE`](LICENSE) for details.

---

## Contributing

Contributions, feedback, and pull requests are welcome!  
Please open an issue or submit a PR on GitHub at:  
[https://github.com/ZihanZheng2000/gdromops](https://github.com/ZihanZheng2000/gdromops)

