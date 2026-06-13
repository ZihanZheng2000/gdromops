# gdromops

`gdromops` runs reservoir operation simulations from already-trained GDROM rule
files.

The package supports two rule layouts:

- Multi-module rules with a condition tree:
  `data/module_conditions/<GRAND_ID>.txt` selects one module from
  `data/modules/<GRAND_ID>_<module_id>.txt`.
- Single-module rules without a condition tree:
  if `data/module_conditions/<GRAND_ID>.txt` is absent, `RuleEngine` uses
  `data/modules/<GRAND_ID>_0.txt` directly.

## Install

```bash
pip install -r requirements.txt
pip install .
```

## Example Data

Two short example time series are included:

- `gdromops/data/example_time_series/example_data_reservoir449.csv`
  - reservoir `449`
  - has `gdromops/data/module_conditions/449.txt`
  - demonstrates condition-tree module selection
- `gdromops/data/example_time_series/85.csv`
  - reservoir `85`
  - has no `gdromops/data/module_conditions/85.txt`
  - demonstrates direct single-module simulation with `modules/85_0.txt`

Both CSV files use the same columns:

```text
Date,Storage,Inflow,Release,DOY,PDSI
```

## One-Step Simulation

```python
from gdromops import RuleEngine

engine = RuleEngine("449")

release, new_storage = engine.GDROM_simulate_one_day(
    inflow=5.0,
    doy=150,
    pdsi=-1.2,
    storage=120.0,
)
```

## Time-Series Simulation: Reservoir 449

Use this example when the reservoir has a condition tree.

```python
import pandas as pd
from gdromops import RuleEngine

df = pd.read_csv(
    "gdromops/data/example_time_series/example_data_reservoir449.csv",
    parse_dates=["Date"],
).set_index("Date")

engine = RuleEngine("449")
result = engine.GDROM_simulate(
    inflow_series=df["Inflow"],
    storage_series=df["Storage"],
    pdsi_series=df["PDSI"],
)

print(result[["simulated_release", "simulated_storage"]].head())
```

## Time-Series Simulation: Reservoir 85

Use this example when the reservoir has only one module and no condition tree.
`RuleEngine("85")` automatically uses `gdromops/data/modules/85_0.txt`.

```python
import pandas as pd
from gdromops import RuleEngine

df = pd.read_csv(
    "gdromops/data/example_time_series/85.csv",
    parse_dates=["Date"],
).set_index("Date")

engine = RuleEngine("85")
result = engine.GDROM_simulate(
    inflow_series=df["Inflow"],
    storage_series=df["Storage"],
    pdsi_series=df["PDSI"],
)

print(result[["simulated_release", "simulated_storage"]].head())
```

If `pdsi_series` is not provided, pass `latitude` and `longitude`; the packaged
`gdromops/data/pdsi.mon.mean.nc` file will be used.

## Included Rule Data

- `gdromops/data/module_conditions/<GRAND_ID>.txt`
- `gdromops/data/modules/<GRAND_ID>_<module_id>.txt`
- `gdromops/data/pdsi.mon.mean.nc`
- `gdromops/data/example_time_series/*.csv`

## License

MIT License. See `LICENSE`.
