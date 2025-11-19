#%%
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
# ============== Case 3: With initial storage ===============
# ==========================================================
print("=== Case 3: Multi-day simulation with initial storage ===")

result_case3 = engine.GDROM_simulate(
    inflow_series=inflow,
    initial_storage=initial_storage,
    pdsi_series=pdsi,
)

print(result_case3.head(), "\n")

#%%
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

fig, ax1 = plt.subplots(figsize=(14, 5))

# ----- Primary axis: Release -----
ax1.plot(df.index, release,
         label="Observed Release", color="black", linewidth=1.8)

ax1.plot(result_case3.index, result_case3["simulated_release"],
         label="Simulated Release", color="#E67E22",
         linestyle="--", linewidth=2.0)

# Axis labels (bigger + bold)
ax1.set_xlabel("Date", fontsize=14, fontweight="bold")
ax1.set_ylabel("Release", fontsize=14, fontweight="bold")

# Set y-limits with padding
release_min, release_max = 0, 3500
padding = (release_max - release_min) * 0.1
ax1.set_ylim(release_min - padding, release_max + padding)

ax1.grid(alpha=0.25)

# x-axis formatting
ax1.xaxis.set_major_locator(mdates.YearLocator())
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
plt.setp(ax1.get_xticklabels(), rotation=0, fontsize=12, fontweight="bold")
plt.setp(ax1.get_yticklabels(), fontsize=12, fontweight="bold")


# ----- Secondary axis: Storage -----
ax2 = ax1.twinx()

ax2.plot(df.index, storage,
         label="Observed Storage", color="#7F8C8D", linewidth=1.6)

ax2.plot(result_case3.index, result_case3["simulated_storage"],
         label="Simulated Storage", color="#27AE60",
         linestyle="--", linewidth=2.0)

ax2.set_ylabel("Storage", fontsize=14, fontweight="bold")

storage_min, storage_max = 0, 80000
storage_padding = (storage_max - storage_min) * 0.1
ax2.set_ylim(storage_min - storage_padding, storage_max + storage_padding)

plt.setp(ax2.get_yticklabels(), fontsize=12, fontweight="bold")


# ----- Legend (bigger + bold, inside plot) -----
lines_1, labels_1 = ax1.get_legend_handles_labels()
lines_2, labels_2 = ax2.get_legend_handles_labels()

legend = ax1.legend(
    lines_1 + lines_2, labels_1 + labels_2,
    loc="upper center",
    bbox_to_anchor=(0.5, 0.97),
    ncol=4,
    frameon=True,
    fontsize=12,
    fancybox=True
)
legend.get_frame().set_alpha(0.9)

# Make legend text bold
for text in legend.get_texts():
    text.set_fontweight("bold")


# # ----- Title -----
# ax1.set_title(
#     "Case 3: Observed vs Simulated Release and Storage for Echo (GRanD_ID = 449)",
#     fontsize=16, fontweight="bold", pad=12
# )

plt.tight_layout()
plt.show()
