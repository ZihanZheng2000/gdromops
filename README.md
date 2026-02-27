# gdromops

`gdromops` is a Python package for:

1. Running reservoir operation simulation from GDROM rule text files (`RuleEngine`)
2. Training GDROM **Res_R** models and exporting rule files

It is designed for reproducible data-driven reservoir operation workflows.

## Installation

```bash
pip install git+https://github.com/ZihanZheng2000/gdromops.git
```

## Core Capabilities

- Rule-based simulation using packaged or custom rule files
- Res_R training pipeline (HMDT + CART)
- Automatic model selection across ITS and module count K
- Rule export to:
  - `Res_R/module_conditions/<GRAND_ID>.txt`
  - `Res_R/modules/<GRAND_ID>_<module_id>.txt`
- Training summary export (`training_summary.csv`)

## Quick Start: RuleEngine

```python
import pandas as pd
from gdromops import RuleEngine

engine = RuleEngine("449")

df = pd.read_csv("tests/example_data_reservoir449.csv", parse_dates=["Date"]).set_index("Date")
inflow = df["Inflow"]
storage = df["Storage"]
pdsi = df["PDSI"]

# one-step simulation
release, new_storage = engine.GDROM_simulate_one_day(
    inflow=5.0,
    doy=150,
    pdsi=-1.2,
    storage=120.0,
)

# multi-step simulation with observed storage
result_obs_storage = engine.GDROM_simulate(
    inflow_series=inflow,
    storage_series=storage,
    pdsi_series=pdsi,
)

# multi-step simulation with iterative storage update
result_iter_storage = engine.GDROM_simulate(
    inflow_series=inflow,
    initial_storage=float(storage.iloc[0]),
    pdsi_series=pdsi,
)
```

Demo script:

```bash
python -m tests.test_demo_reservoir449
```

## Train Res_R (CLI)

Entry point:

```bash
gdromops-train-res-r --help
```

### Mode A: Use `reservoir_metadata.csv` storage cap

Use this when your input file is already normalized by storage capacity.

```bash
gdromops-train-res-r \
  --target-id 449 \
  --target-data-path "path/to/449.csv" \
  --summary-path "path/to/reservoir_metadata.csv" \
  --storage-cap-source summary \
  --output-root "path/to/output" \
  --its-values 6,7,8
```

### Mode B: Auto derive storage cap from data (`data_max`)

Use this when storage cap should be inferred from the input file itself:

- `storage_cap = max(Storage)` from target data
- training normalization: `Inflow/Storage/Release /= storage_cap`
- rule export: values are denormalized back by the same `storage_cap`

```bash
gdromops-train-res-r \
  --target-id 449 \
  --target-data-path "path/to/example_data_reservoir449.csv" \
  --storage-cap-source data_max \
  --output-root "path/to/output" \
  --its-values 6,7,8
```

### Important optional arguments

- `--best-its` / `--best-num-state`: manually fix hyperparameters
- `--complexity-free-k`: no complexity penalty when `K <= this value` (default `3`)
- `--nse-gain-per-extra-k`: required NSE gain per extra module above `complexity-free-k` (default `0.02`)
- `--save-models-dir`: save trained `HMDT` and `CART` model objects
- `--training-summary-path`: custom location of training summary CSV

## Train Res_R (Python API)

```python
from gdromops.training import train_res_r_from_paths

result = train_res_r_from_paths(
    target_id="449",
    target_data_path="path/to/example_data_reservoir449.csv",
    summary_path=None,                 # not needed when storage_cap_source="data_max"
    output_root="path/to/output",
    its_values=(6, 7, 8),
    storage_cap_source="data_max",     # "summary" or "data_max"
)

print(result["best_its"], result["best_num_state"])
print(result["storage_cap_used"], result["normalization_applied"])
```

## Minimal Reproducible Example (MRE)

Run the following command from the repository root to reproduce a full Res_R training using the included demo data:

```bash
gdromops-train-res-r \
  --target-id 449 \
  --target-data-path "tests/example_data_reservoir449.csv" \
  --storage-cap-source data_max \
  --output-root "tests/mre_out_449" \
  --its-values 6,7,8
```

Expected outputs:

- `tests/mre_out_449/Res_R/module_conditions/449.txt` (if selected K > 1)
- `tests/mre_out_449/Res_R/modules/449_0.txt`, `449_1.txt`, ...
- `tests/mre_out_449/Res_R/training_summary.csv`

The CLI JSON output should also include:

- `"storage_cap_source": "data_max"`
- `"storage_cap_used": <max Storage in input csv>`
- `"normalization_applied": true`

## Outputs

After training, output folder contains:

- `Res_R/module_conditions/<GRAND_ID>.txt` (if K > 1)
- `Res_R/modules/<GRAND_ID>_<i>.txt`
- `Res_R/training_summary.csv`

The returned training result includes:

- selected hyperparameters (`best_its`, `best_num_state`)
- validation traces (`nse_scores`, `pbias_scores`)
- test metrics (`NSE`, `KGE`, etc.)
- storage-cap provenance (`storage_cap_source`, `storage_cap_used`, `normalization_applied`)

## Citation

If you use `gdromops` or GDROM v2 data in research, please cite:

- Zheng, Z., X. Cai, Y. Chen (2025). GDROM v2: An Inventory of Operation Variables Time Series and Rules for 2,017 Large Reservoirs across the CONUS, HydroShare, https://doi.org/10.4211/hs.5293674cb83b4ec698db0eb4777467b8
- Zheng, Z., et al. (2025). gdromops: A Python package for simulating reservoir operations using GDROM rules. Journal of Open Source Software. Under review.

## License

MIT License. See `LICENSE`.

## Contributing

Issues and pull requests are welcome:
https://github.com/ZihanZheng2000/gdromops
