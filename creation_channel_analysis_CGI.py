# -*- coding: utf-8 -*-
"""
Created by ARTTC at 15.04.2023

Description: workplace report generation script

"""

import sys
import pandas as pd
import os
import numpy as np
from pathlib import Path

import matplotlib
matplotlib.use("MacOSX")

import matplotlib.pyplot as plt
plt.ioff()

path = str(Path(os.path.expanduser('~/Desktop/Python/ad_hoc')))
sys.path.append(path)

# =============================================================================
# Run parameters
# =============================================================================

# =============================================================================
# User-defined variables
# =============================================================================
file_name=r'CGI_transactions.xlsx'
save_folder=r'plots_weekday_hour'
offset_months=6 # based on how many months a "norm" is calculated
from_period='2025-10-27'
until_period='2025-11-02'

hour_dict={'00-01':1,
           '01-02':2,
           '02-03':3,
           '03-04':4,
           '04-05':5,
           '05-06':6,
           '06-07':7,
           '07-08':8,
           '08-09':9,
           '09-10':10,
           '10-11':11,
           '11-12':12,
           '12-13':13,
           '13-14':14,
           '14-15':15,
           '15-16':16,
           '16-17':17,
           '17-18':18,
           '18-19':19,
           '19-20':20,
           '20-21':21,
           '21-22':22,
           '22-23':23,
           '23-00':24
           }

# =============================================================================
# Run parameters
# =============================================================================

# =============================================================================
# User-defined functions
# =============================================================================

def prepare_data(file_name):
    df = pd.read_excel(file_name, sheet_name='Final')
    df = df.rename(columns={"Total events": "Transactions"})

    df["Date"] = pd.to_datetime(dict(year=df["Year"], month=df["Month"], day=df["Day"]))
    df = df.sort_values(["Date", 'Hour'])
    df = df.loc[df['Month'] >= 5]
    df = df.loc[df['Date'] < '2025-12-09 00:00:00']
    df['Hour'] = df['Hour'].map(hour_dict)
    df = df[['Date', 'Year', 'Month', 'Day', 'Hour', 'Weekday', 'Transactions']]

    # Ensure correct types
    df['Weekday'] = df['Weekday'].astype(str)
    print("Data prepared.")
    return df

def full_timeseries_analysis(file_name,offset_months):
    df = prepare_data(file_name)
    last_date = df['Date'].max()
    start_date = last_date - pd.DateOffset(months=offset_months)
    df_baseline = df[df['Date'] >= start_date]
    df_baseline = (
        df_baseline.groupby(['Weekday', 'Hour'])['Transactions']
        .agg(
            median='median',
            p25=lambda x: np.percentile(x, 25),  # 25% of values are below this
            p75=lambda x: np.percentile(x, 75),  # 75% of values are below this
            p10=lambda x: np.percentile(x, 10),  # extreme outliers: 10% of values are below this
            p5=lambda x: np.percentile(x, 5),  # very extreme outliers: 5% of values are below this
        )
        .reset_index()
    )
    # calculate interquartile range: we exclude 25% of the data on both ends
    df_baseline['iqr'] = df_baseline['p75'] - df_baseline['p25']

    # Main alert threshold (boxplot-style robust lower bound)

    # Optional alternative thresholds (more/less strict)
    df_baseline['lower_threshold'] = df_baseline['p5'].clip()
    df_output=df.merge(df_baseline, on=['Weekday', 'Hour'], how='left')

    df_output = df_output.sort_values('Date')  # important!

    df_output['alert_raw'] = (
            (df_output['Transactions'] < df_output['lower_threshold']) &
            (df_output['Transactions'] < 0.5 * df_output['median'])
    )  # checking alert conditions for all hours

    df_output['is_alert'] = (
            df_output['alert_raw'] &
            df_output['alert_raw'].shift(1).fillna(False).astype(bool)
    )  # checking if previous hour was also an alert

    df_issue=df_output[df_output['is_alert']==True]
    print("Full timeseries analysis completed.")
    return df_output,df_issue


def prepare_baseline_with_stats(df,df_target,offset_months):
    df_baseline = df.loc[df['Date'] != df_target['Date'].unique()[0]]

    # offset 3 months from the last date in the data
    last_date = df['Date'].max()
    start_date = last_date - pd.DateOffset(months=offset_months)
    df_baseline = df_baseline[df_baseline['Date'] >= start_date]
    df_baseline = (
        df_baseline.groupby(['Weekday', 'Hour'])['Transactions']
        .agg(
            median='median',
            p25=lambda x: np.percentile(x, 25),  # 25% of values are below this
            p75=lambda x: np.percentile(x, 75),  # 75% of values are below this
            p10=lambda x: np.percentile(x, 10),  # extreme outliers: 10% of values are below this
            p5=lambda x: np.percentile(x, 5),  # very extreme outliers: 5% of values are below this
        )
        .reset_index()
    )
    # calculate interquartile range: we exclude 25% of the data on both ends
    df_baseline['iqr'] = df_baseline['p75'] - df_baseline['p25']

    # Main alert threshold (boxplot-style robust lower bound)

    # Optional alternative thresholds (more/less strict)
    df_baseline['lower_threshold'] = df_baseline['p5'].clip(
        lower=0)  # This hour is worse than 95% (or 90%) of historical observations
    print("Baseline with statistics prepared.")
    return df_baseline

# =============================================================================
# Code
# =============================================================================

df_full,df_issues=full_timeseries_analysis(file_name,offset_months)

for target_day in ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']:
    # target_day='Monday'
    # select only one day of the week for analysis
    df = prepare_data(file_name)
    df_target = df.loc[df['Date'].between(from_period, until_period)]
    df_target=df_target[df_target['Weekday']==target_day]

    df_baseline=prepare_baseline_with_stats(df,df_target,offset_months)

    # Merge back
    df_output = df_target.merge(df_baseline, on=['Weekday', 'Hour'], how='left')

    # --- Build the plot ---
    fig, ax = plt.subplots(figsize=(12, 5))

    # IQR band (Q1..Q3) in yellow
    ax.fill_between(
        df_output["Hour"],
        df_output["p25"],
        df_output["p75"],
        alpha=0.35,
        label="IQR (Q1–Q3)",
    )

    # Median dotted red line
    ax.plot(
        df_output["Hour"],
        df_output["median"],
        linestyle="--",
        linewidth=2,
        label=f"Median ({target_day}s)",
    )

    # Lower p10 solid red line
    ax.plot(
        df_output["Hour"],
        df_output["lower_threshold"],
        linewidth=2,
        label=f"p10 ({target_day}s)",
    )

    # Actual transactions for Monday 8th December (black line with markers)
    # If some hours are missing, you can reindex to 1..24 for a clean line.
    if not df_output.empty:
        output_series = (
            df_output.groupby("Hour")["Transactions"]
            .sum()  # if there is 1 row/hour, sum == value; otherwise it aggregates safely
            .reindex(range(1, 25))
        )
        ax.plot(
            output_series.index,
            output_series.values,
            marker="o",
            linewidth=2,
            label=f"Actuals",
        )
    else:
        print(f"WARNING: No rows found for {target_date.date()} in the file.")

    # --- Cosmetics to match your wireframe intent ---
    ax.set_title("Monday: Transactions vs historical baseline (Median, IQR, p10)")
    ax.set_xlabel("Hour")
    ax.set_ylabel("# of transactions")
    ax.set_xlim(1, 24)
    ax.set_xticks(range(1, 25))

    ax.grid(True, alpha=0.25)
    ax.legend(loc="upper right")

    plt.tight_layout()
    # save the plot

    plt.savefig(
        os.path.join(save_folder, f"weekday_hour{target_day}_{from_period}-{until_period}.png"),
        dpi=300
    )
    plt.close()
    print(f"Plot saved for {target_day}.")




