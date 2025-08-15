
# gdromops

Load reservoir operation rules (CT + modules) from GitHub on demand and simulate releases/storage.

## Install (editable dev)
```bash
pip install -e .
```

## Quick start
```python
from gdromops import RuleEngine
import pandas as pd

# Timeseries must include columns: Date, Inflow, Storage, DOY, PDSI
df = pd.read_csv("your_timeseries.csv", parse_dates=["Date"])

eng = RuleEngine(grand_id=41)  # downloads rules from GitHub (cached locally)
out = eng.simulate_release_and_storage(df)
print(out.head())
```

## Configuration
Default rules base (GitHub raw) is set in `gdromops/config.py`. Set env var `GDROMOPS_CACHE`
to change the cache directory.
