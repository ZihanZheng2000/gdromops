import pandas as pd
from gdromops import RuleEngine 

grand_id = "41"
engine = RuleEngine(grand_id)

# Load example data
df = pd.read_csv("example_data_reservoir41.csv", parse_dates=["Date"])
df = df.set_index("Date")

# Common input
inflow = df["Inflow"]
storage = df["Storage"]

# -----------------
# Case 1: with storage series
# -----------------
result_case1 = engine.GDROM_simulate(
    inflow_series=inflow,
    storage_series=storage,
    latitude=48.7325,
    longitude=-121.0673
)
print("Case 1: With storage")
print(result_case1.head())

# -----------------
# Case 2: without storage series
# -----------------

initial_storage = float(df["Storage"].iloc[0]) 

result_case2 = engine.GDROM_simulate(
    inflow_series=inflow,
    initial_storage=initial_storage,
    latitude=48.7325,
    longitude=-121.0673
)
print("Case 2: Without storage")
print(result_case2.head())
