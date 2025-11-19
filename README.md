# gdromops

`gdromops` provides a reproducible and lightweight framework for applying pre-trained GDROM rules to reservoir operation simulation.  
It is designed for research and applications in large-scale reservoir system modeling, water resources management, and data-driven hydrology.

---

## Installation

Install from GitHub (latest development version):

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
├── test_demo_reservoir449.py          # Example usage
├── example_data_reservoir449.csv
```

---

## Quick Start

```python
import pandas as pd
from gdromops import RuleEngine

# Initialize RuleEngine for a given reservoir
engine = RuleEngine("449")

# Load input data (example)
df = pd.read_csv("example_data_reservoir449.csv", parse_dates=["Date"]).set_index("Date")
inflow = df["Inflow"]
storage = df["Storage"]
pdsi = df["PDSI"]
initial_storage = float(storage.iloc[0])

# ---- Case 1: Simulate a Single Day ----
date = df.index[0]
release, new_storage = engine.GDROM_simulate_one_day(
    inflow=5,            # inflow for one timestep
    doy=150,             # day of year
    pdsi=-1.2,           # drought index
    storage=120.0,       # current storage
)

# ---- Case 2: Simulate a Multi-day Period (with observed storage) ----
result_case2 = engine.GDROM_simulate(
    inflow_series=inflow,
    storage_series=storage,
    pdsi_series=pdsi,
)

# ---- Case 3: Multi-day Simulation (with initial storage only) ----
result_case3 = engine.GDROM_simulate(
    inflow_series=inflow,
    initial_storage=initial_storage,
    pdsi_series=pdsi,
)

# ---- Case 4: Auto-fetch PDSI from Location ----
result_case4 = engine.GDROM_simulate(
    inflow_series=inflow,
    initial_storage=initial_storage,
    latitude=48.7325,
    longitude=-121.0673,
)

# ---- Case 5: Use a Different Timestep (e.g., 1 hour or 5 min) ----
release_t, new_storage_t = engine.GDROM_simulate_timestep(
    inflow=0.5,          
    doy=150,             
    pdsi=-1.2,           
    storage=120.0,       
    timestep_hours=1.0,  # e.g., 1 hr (0.0833 for 5 min)
)
```

---

## Demo

Example data (`example_data_reservoir449.csv`) and test script (`test_demo_reservoir449.py`) are included under `tests/`.  
Run all four demo cases with:

```bash
python -m tests.test_demo_reservoir449
```

---


## Citation

If you use `gdromops` or the GDROM v2 dataset in your research, please cite the dataset or the software:

**Dataset citation**  
Zheng, Z., X. Cai, Y. Chen (2025). GDROM v2: An Inventory of Operation Variables Time Series and Rules for 2,017 Large Reservoirs across the CONUS, HydroShare, https://doi.org/10.4211/hs.5293674cb83b4ec698db0eb4777467b8

**Software citation**  
Zheng, Z., et al. (2025). gdromops: A Python package for simulating reservoir operations using GDROM rules. Journal of Open Source Software. Under Review.

---

## License

This project is released under the **MIT License**.  
See [`LICENSE`](LICENSE) for details.

---

## Contributing

Contributions, feedback, and pull requests are welcome!  
Please open an issue or submit a PR on GitHub at:  
[https://github.com/ZihanZheng2000/gdromops](https://github.com/ZihanZheng2000/gdromops)

