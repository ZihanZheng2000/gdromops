import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import pandas as pd
import matplotlib.pyplot as plt
from gdromops import RuleEngine

# ==========================================================
# ============== Initialize and load data ==================
# ==========================================================
grand_id = "449"
engine = RuleEngine(grand_id)

current_dir = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(current_dir, "example_data_reservoir449.csv")

df = pd.read_csv(data_path, parse_dates=["Date"])
df = df.set_index("Date")

# Common input data
inflow = df["Inflow"]
storage = df["Storage"]
release = df["Release"] if "Release" in df.columns else None
initial_storage = float(storage.iloc[0])
pdsi = df["PDSI"] if "PDSI" in df.columns else None

# ==========================================================
# ============== Case 1: One-day simulation ================
# ==========================================================
print("=== Case 1: One-day simulation ===")

test_date = df.index[0]
inflow_1d = float(df.loc[test_date, "Inflow"])
storage_1d = float(df.loc[test_date, "Storage"])
doy_1d = int(test_date.dayofyear)
pdsi_1d = float(df.loc[test_date, "PDSI"]) if "PDSI" in df.columns else 0.0

release_1d, new_storage_1d = engine.GDROM_simulate_one_day(
    inflow=inflow_1d,
    doy=doy_1d,
    pdsi=pdsi_1d,
    storage=storage_1d,
)

print(f"Date: {test_date.date()} | Inflow: {inflow_1d:.2f} | Storage: {storage_1d:.2f} "
      f"| Release: {release_1d:.2f} | New Storage: {new_storage_1d:.2f}\n")

# ==========================================================
# ============== Case 2: With observed storage ==============
# ==========================================================
print("=== Case 2: Multi-day simulation with observed storage ===")

result_case2 = engine.GDROM_simulate(
    inflow_series=inflow,
    storage_series=storage,
    pdsi_series=pdsi,
)

print(result_case2.head(), "\n")

# --- Plot Case 2 ---
plt.style.use("seaborn-v0_8-whitegrid")
plt.figure(figsize=(12, 5))
plt.plot(df.index, release, label="Observed Release", color="black", linewidth=1.5)
plt.plot(result_case2.index, result_case2["simulated_release"],
         label="Simulated Release (Case 2)", color="tab:blue", linestyle="--", linewidth=1.5)
plt.xlabel("Date")
plt.ylabel("Release")
plt.title("Case 2: Observed vs Simulated Release (with observed storage)")
plt.legend()
plt.tight_layout()
plt.show()

# ==========================================================
# ============== Case 3: With initial storage ===============
# ==========================================================
print("=== Case 3: Multi-day simulation with initial storage ===")

result_case3 = engine.GDROM_simulate(
    inflow_series=inflow,
    initial_storage=initial_storage,
    pdsi_series=pdsi,
)

print(result_case3.head(), "\n")

# --- Plot Case 3 (release + storage) ---
fig, ax1 = plt.subplots(figsize=(12, 5))
ax1.plot(df.index, release, label="Observed Release", color="black", linewidth=1.5)
ax1.plot(result_case3.index, result_case3["simulated_release"],
         label="Simulated Release (Case 3)", color="tab:orange", linestyle="--", linewidth=1.5)
ax1.set_xlabel("Date")
ax1.set_ylabel("Release")
ax1.set_title("Case 3: Observed vs Simulated Release and Storage (initial storage)")

# Secondary axis for storage comparison
ax2 = ax1.twinx()
ax2.plot(df.index, storage, label="Observed Storage", color="gray", linewidth=1.2)
ax2.plot(result_case3.index, result_case3["simulated_storage"],
         label="Simulated Storage (Case 3)", color="tab:green", linestyle="--", linewidth=1.5)
ax2.set_ylabel("Storage")

# Combine legends
lines_1, labels_1 = ax1.get_legend_handles_labels()
lines_2, labels_2 = ax2.get_legend_handles_labels()
ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper right")

plt.tight_layout()
plt.show()

# ==========================================================
# ============== Case 4: With latitude & longitude ==========
# ==========================================================
print("=== Case 4: Multi-day simulation with latitude/longitude PDSI extraction ===")

# Coordinates for reservoir 41
latitude = 48.7325
longitude = -121.0673

# Run simulation using internal NetCDF PDSI file
result_case4 = engine.GDROM_simulate(
    inflow_series=inflow,
    initial_storage=initial_storage,
    latitude=latitude,
    longitude=longitude,
)

print(result_case4.head(), "\n")

# --- Plot Case 4 (release + storage) ---
fig, ax1 = plt.subplots(figsize=(12, 5))
ax1.plot(df.index, release, label="Observed Release", color="black", linewidth=1.5)
ax1.plot(result_case4.index, result_case4["simulated_release"],
         label="Simulated Release (Case 4, lat/lon PDSI)", color="tab:purple", linestyle="--", linewidth=1.5)
ax1.set_xlabel("Date")
ax1.set_ylabel("Release")
ax1.set_title("Case 4: Observed vs Simulated Release and Storage (PDSI from lat/lon)")

# Secondary axis for storage comparison
ax2 = ax1.twinx()
ax2.plot(df.index, storage, label="Observed Storage", color="gray", linewidth=1.2)
ax2.plot(result_case4.index, result_case4["simulated_storage"],
         label="Simulated Storage (Case 4)", color="tab:green", linestyle="--", linewidth=1.5)
ax2.set_ylabel("Storage")

# Combine legends
lines_1, labels_1 = ax1.get_legend_handles_labels()
lines_2, labels_2 = ax2.get_legend_handles_labels()
ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper right")

plt.tight_layout()
plt.show()

# ==========================================================
# ============== Case 5: With Different Timestep ==========
# ==========================================================
print("=== Case 5: Variable-timestep simulation ===")

test_date = df.index[0]
inflow = float(df.loc[test_date, "Inflow"])
storage = float(df.loc[test_date, "Storage"])
doy = int(test_date.dayofyear)
pdsi = float(df.loc[test_date, "PDSI"]) if "PDSI" in df.columns else 0.0

# === choose your timestep_hours ===
timestep_hours = 0.0833   
inflow_t = inflow * timestep_hours

release_t, new_storage_t = engine.GDROM_simulate_timestep(
    inflow=inflow_t,
    doy=doy,
    pdsi=pdsi,
    storage=storage,
    timestep_hours=timestep_hours,
)

print(
    f"Date: {test_date.date()} | Δt = {timestep_hours:.4f} hr | "
    f"Inflow: {inflow_t:.2f} | Storage: {storage:.2f} | "
    f"Release: {release_t:.4f} | New Storage: {new_storage_t:.2f}\n"
)
