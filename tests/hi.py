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
from matplotlib import ticker
from matplotlib.font_manager import FontProperties
import os

# -------------------------
# Font setup (Aldhabi)
# -------------------------
FONT_PATH = r"C:\Windows\Fonts\Aldhabi.ttf"

if os.path.exists(FONT_PATH):
    font_label  = FontProperties(fname=FONT_PATH, weight="bold", size=15)
    font_tick   = FontProperties(fname=FONT_PATH, weight="bold", size=12)
    font_legend = FontProperties(fname=FONT_PATH, weight="bold", size=12)
else:
    font_label  = FontProperties(family="DejaVu Serif", weight="bold", size=15)
    font_tick   = FontProperties(family="DejaVu Serif", weight="bold", size=12)
    font_legend = FontProperties(family="DejaVu Serif", weight="bold", size=12)

# -------------------------
# Helpers
# -------------------------
def apply_tick_font(ax):
    for lab in ax.get_xticklabels() + ax.get_yticklabels():
        lab.set_fontproperties(font_tick)

def set_xlabel(ax, text):
    ax.set_xlabel(text)
    ax.xaxis.label.set_fontproperties(font_label)

def set_ylabel(ax, text):
    ax.set_ylabel(text)
    ax.yaxis.label.set_fontproperties(font_label)

# -------------------------
# Plot
# -------------------------
fig, ax1 = plt.subplots(figsize=(14, 5))

# Release
ax1.plot(df.index, release,
         label="Observed Release",
         color="black", linewidth=1.8)

ax1.plot(result_case3.index,
         result_case3["simulated_release"],
         label="Simulated Release",
         color="#E67E22", linestyle="--", linewidth=2.0)

set_xlabel(ax1, "Year")
set_ylabel(ax1, "Release")

# Limits
rmin, rmax = 0, 3500
pad = (rmax - rmin) * 0.1
ax1.set_ylim(rmin - pad, rmax + pad)

ax1.grid(alpha=0.25)

# X axis
ax1.xaxis.set_major_locator(mdates.YearLocator())
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

# Thousands separator
ax1.yaxis.set_major_formatter(
    ticker.StrMethodFormatter('{x:,.0f}')
)

apply_tick_font(ax1)

# Storage
ax2 = ax1.twinx()

ax2.plot(df.index, storage,
         label="Observed Storage",
         color="#7F8C8D", linewidth=1.6)

ax2.plot(result_case3.index,
         result_case3["simulated_storage"],
         label="Simulated Storage",
         color="#27AE60", linestyle="--", linewidth=2.0)

set_ylabel(ax2, "Storage")

smin, smax = 0, 80000
pad2 = (smax - smin) * 0.1
ax2.set_ylim(smin - pad2, smax + pad2)

ax2.yaxis.set_major_formatter(
    ticker.StrMethodFormatter('{x:,.0f}')
)

apply_tick_font(ax2)

# Legend
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()

legend = ax1.legend(
    lines1 + lines2,
    labels1 + labels2,
    loc="upper center",
    bbox_to_anchor=(0.5, 0.97),
    ncol=4,
    frameon=True,
    fancybox=True
)

for txt in legend.get_texts():
    txt.set_fontproperties(font_legend)

plt.tight_layout()
plt.show()
