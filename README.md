# gdromops

`gdromops` is a lightweight Python package for simulating reservoir operations
with pre-trained Generic Data-Driven Reservoir Operation Model (GDROM) rules.

Reservoirs strongly affect streamflow, but many hydrologic models still use
simplified reservoir representations. GDROM rules provide realistic,
interpretable, and computationally efficient operation rules learned from
historical reservoir records. This package makes those rules easy to use without
running the original training workflow.

With a reservoir ID and time series inputs, `gdromops` can:

- load the matching pre-trained GDROM rule files;
- simulate reservoir release for each time step;
- optionally update storage through mass balance;

The package is designed for standalone reservoir simulation, historical
analysis, scenario testing, or embedding GDROM rules in larger routing and
hydrologic modeling workflows.

## Installation

Install directly from GitHub:

```bash
pip install git+https://github.com/ZihanZheng2000/gdromops.git
```

Or clone the repository and install locally:

```bash
git clone https://github.com/ZihanZheng2000/gdromops.git
cd gdromops
pip install -r requirements.txt
pip install .
```

## Rule Structure and Examples

`gdromops` supports two rule layouts.

For reservoirs with one operation module, only one module file is needed. The
included reservoir `85` example uses this layout:

```text
gdromops/data/modules/<GRAND_ID>_0.txt
```

Example files:

- `gdromops/data/example_time_series/85.csv`
- `gdromops/data/modules/85_0.txt`

For reservoirs with multiple operation modules, a condition tree selects the
module to use at each time step. The included reservoir `449` example uses this
layout:

```text
gdromops/data/module_conditions/<GRAND_ID>.txt
gdromops/data/modules/<GRAND_ID>_<module_id>.txt
```

Example files:

- `gdromops/data/example_time_series/449.csv`
- `gdromops/data/module_conditions/449.txt`
- `gdromops/data/modules/449_0.txt`, `449_1.txt`, `449_2.txt`, `449_3.txt`

If no condition file exists for a reservoir, `RuleEngine` automatically uses
`gdromops/data/modules/<GRAND_ID>_0.txt`.

Both example CSV files use the same columns:

```text
Date,Storage,Inflow,Release,DOY,PDSI
```

`Release` is included for comparison only; it is not required for simulation.

## Time-Series Simulation

The time-series API is `RuleEngine.GDROM_simulate`. It requires an inflow series
with a `DatetimeIndex`. The same code works for both single-module reservoirs
such as `85` and condition-tree reservoirs such as `449`.

There are two common ways to use storage.

### 1. Use Observed Storage

Use this mode when you already have reservoir storage at each time step. GDROM
uses the observed/current storage to choose and evaluate the rule, then returns
the simulated release. In this mode, `simulated_storage` is the input storage
series.

```python
import pandas as pd
from gdromops import RuleEngine

reservoir_id = "85"
csv_path = f"gdromops/data/example_time_series/{reservoir_id}.csv"

df = pd.read_csv(csv_path, parse_dates=["Date"]).set_index("Date")

engine = RuleEngine(reservoir_id)
result = engine.GDROM_simulate(
    inflow_series=df["Inflow"],
    storage_series=df["Storage"],
    pdsi_series=df["PDSI"],
)

print(result[["simulated_release", "simulated_storage"]].head())
```

To run the condition-tree example, change the ID:

```python
reservoir_id = "449"
csv_path = f"gdromops/data/example_time_series/{reservoir_id}.csv"
```

### 2. Use Iterated Storage

Use this mode when you only know the starting storage. GDROM simulates release
sequentially and updates storage at each time step:

```text
new_storage = previous_storage + inflow - simulated_release
```

```python
import pandas as pd
from gdromops import RuleEngine

reservoir_id = "85"
csv_path = f"gdromops/data/example_time_series/{reservoir_id}.csv"

df = pd.read_csv(csv_path, parse_dates=["Date"]).set_index("Date")

engine = RuleEngine(reservoir_id)
result = engine.GDROM_simulate(
    inflow_series=df["Inflow"],
    initial_storage=float(df["Storage"].iloc[0]),
    pdsi_series=df["PDSI"],
)

print(result[["simulated_release", "simulated_storage"]].head())
```

### 3. PDSI From the Packaged NetCDF File

If you already have a PDSI time series, pass it directly with `pdsi_series`.
If not, provide the reservoir latitude and longitude. `gdromops` will read the
packaged `gdromops/data/pdsi.mon.mean.nc` file, select the nearest grid cell,
and align monthly PDSI to the simulation dates.

```python
result = engine.GDROM_simulate(
    inflow_series=df["Inflow"],
    storage_series=df["Storage"],
    latitude=40.9,
    longitude=-111.4,
)
```

## One-Step Simulation

For coupling with another routing or hydrologic model, you may want to call
`gdromops` one time step at a time instead of passing a full time series. In
this mode, the model receives the current inflow, day of year, PDSI, and storage,
then returns the simulated release and updated storage.

### 1. Daily One-Step Simulation

```python
from gdromops import RuleEngine

engine = RuleEngine("449")

release, new_storage = engine.GDROM_simulate_one_day(
    inflow=5.0,      # daily inflow volume
    doy=150,         # day of year, 1-366
    pdsi=-1.2,       # Palmer Drought Severity Index
    storage=120.0,   # current reservoir storage
)
```

The returned values are:

- `release`: simulated reservoir release for this step
- `new_storage`: storage after applying `storage + inflow - release`

### 2. Sub-Daily Helper Methods

Single-step sub-daily helper methods are available when the calling model uses
shorter time steps:

- `GDROM_simulate_1hr`
- `GDROM_simulate_5min`
- `GDROM_simulate_timestep`

For a time step of length `timestep_hours`, `gdromops` converts the sub-daily
inflow to a daily equivalent, applies the daily GDROM rule, and scales the
release back to the requested time step:

```text
inflow_daily = inflow_timestep * (24 / timestep_hours)
release_daily = GDROM_daily_rule(inflow_daily, doy, pdsi, storage)
release_timestep = release_daily * (timestep_hours / 24)
new_storage = storage + inflow_timestep - release_timestep
```

For `GDROM_simulate_1hr`, `timestep_hours = 1`. For `GDROM_simulate_5min`,
`timestep_hours = 1 / 12`.

For an arbitrary time step, use `GDROM_simulate_timestep`:

```python
release, new_storage = engine.GDROM_simulate_timestep(
    inflow=2.0,
    doy=150,
    pdsi=-1.2,
    storage=120.0,
    timestep_hours=0.5,  # 30 minutes
)
```

This sub-daily option is a temporal scaling convenience for coupling with
models that run at shorter time steps. It preserves consistency with the daily
rule when daily inflow is evenly split across sub-daily steps, but the GDROM
rules themselves were trained at the daily scale. Sub-daily performance has not
been independently validated against observed sub-daily reservoir operations.

## Citation

If you use `gdromops` or GDROM v2 data in research, please cite:

Dataset:

- Zheng, Z., Cai, X., & Chen, Y. (2025). *GDROM v2: An inventory of operation variables time series and rules for 2,017 large reservoirs across the CONUS*. HydroShare. [https://doi.org/10.4211/hs.5293674cb83b4ec698db0eb4777467b8](https://doi.org/10.4211/hs.5293674cb83b4ec698db0eb4777467b8)

Data descriptor article:

- Zheng, Z., Cai, X., Zhang, L., et al. (2025). *GDROM v2: An inventory of operation variables time series and rules for 2,017 large reservoirs across the CONUS*. Scientific Data, 12, 1891. [https://doi.org/10.1038/s41597-025-06162-7](https://doi.org/10.1038/s41597-025-06162-7)

## License

MIT License. See `LICENSE`.

## Contributing

Issues and pull requests are welcome:
https://github.com/ZihanZheng2000/gdromops
