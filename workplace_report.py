# -*- coding: utf-8 -*-
"""
Created by ARTTC at 15.04.2023

Description: workplace report generation script

"""

import sys
import pandas as pd
import os
import matplotlib.pyplot as plt
from pathlib import Path

path = str(Path(os.path.expanduser('~/Desktop/Python/ad_hoc')))
sys.path.append(path)

# =============================================================================
# Run parameters
# =============================================================================

# =============================================================================
# User-defined variables
# =============================================================================
file_name=r'CGI_transactions.xlsx'
ROLLING_DAYS = 90
OUTPUT_DIR = "plots_weekday_hour"   # folder to save PNGs
SHOW_PLOTS = False                  # set True if you really want pop-up windows

# =============================================================================
# Run parameters
# =============================================================================

# =============================================================================
# User-defined functions
# =============================================================================


# =============================================================================
# Code
# =============================================================================

df=pd.read_excel(file_name, sheet_name='Final')
df = df.rename(columns={"Total events": "transactions"})

df["date"] = pd.to_datetime(dict(year=df["Year"], month=df["Month"], day=df["Day"]))
df = df.sort_values("date")

# -------------------------
# 90-DAY MOVING AVERAGE per (Hour, Weekday)
# -------------------------
df["ma_90d_hour_weekday"] = (
    df.groupby(["Hour", "Weekday"])["transactions"]
      .transform(lambda x: x.rolling(window=ROLLING_DAYS, min_periods=1).mean())
)

# -------------------------
# PLOTTING: for each weekday & each hour
# -------------------------
os.makedirs(OUTPUT_DIR, exist_ok=True)

weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
weekdays = [w for w in weekday_order if w in df["Weekday"].unique()]
hours = sorted(df["Hour"].unique())

for weekday in weekdays:
    for hour in hours:
        plot_df = df[(df["Weekday"] == weekday) & (df["Hour"] == hour)]
        if plot_df.empty:
            continue

        plt.figure(figsize=(12, 6))
        plt.plot(plot_df["date"], plot_df["transactions"], label="Actual transactions", linewidth=1.5)
        plt.plot(plot_df["date"], plot_df["ma_90d_hour_weekday"], label="90-day moving average", linewidth=2)

        plt.title(f"Actual vs 90-Day Moving Average — {weekday}, Hour {hour}")
        plt.xlabel("Date")
        plt.ylabel("Number of transactions")
        plt.legend()
        plt.grid(True)

        # Safe filename
        safe_hour = str(hour).replace(":", "").replace("/", "-").replace("\\", "-").replace(" ", "_")
        out_path = os.path.join(OUTPUT_DIR, f"{weekday}__{safe_hour}.png")
        plt.savefig(out_path, dpi=150, bbox_inches="tight")

        if SHOW_PLOTS:
            plt.show()

        plt.close()

print(f"Saved charts to: {OUTPUT_DIR}/")