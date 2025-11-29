"""
etl_functions.py
Contains: Extract, Transform, Validate, Load functions
Used by airflow_dag_weather_etl.py
"""

import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# ===== PATHS =====
AIRFLOW_NEW_HOME = os.path.expanduser("~/airflow_new")

RAW_CSV = os.path.join(AIRFLOW_NEW_HOME, "data", "weatherHistory.csv")
PROCESSED_DIR = os.path.join(AIRFLOW_NEW_HOME, "processed")
DB_PATH = os.path.join(AIRFLOW_NEW_HOME, "db", "weather.db")
DB_URL = f"sqlite:///{DB_PATH}"

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


# ============= WIND STRENGTH HELPER =============
def wind_strength(kmh):
    if pd.isna(kmh):
        return np.nan
    mps = kmh / 3.6
    scale = [
        (0, 1.5, "Calm"), (1.6, 3.3, "Light Air"), (3.4, 5.4, "Light Breeze"),
        (5.5, 7.9, "Gentle Breeze"), (8.0, 10.7, "Moderate Breeze"),
        (10.8, 13.8, "Fresh Breeze"), (13.9, 17.1, "Strong Breeze"),
        (17.2, 20.7, "Near Gale"), (20.8, 24.4, "Gale"),
        (24.5, 28.4, "Strong Gale"), (28.5, 32.6, "Storm"),
        (32.7, float("inf"), "Violent Storm")
    ]
    for lo, hi, label in scale:
        if lo <= mps <= hi:
            return label


# ============= EXTRACT =============
def extract():
    """Return path to raw CSV file."""
    if not os.path.exists(RAW_CSV):
        raise FileNotFoundError("weatherHistory.csv not found!")
    return RAW_CSV


# ============= TRANSFORM =============
def transform(csv_path):
    """Clean, aggregate, enrich weather dataset into daily and monthly datasets."""
    df = pd.read_csv(csv_path)

    # Convert date column & set index
    df["Formatted Date"] = pd.to_datetime(df["Formatted Date"], utc=True, errors="coerce")
    df = df.dropna(subset=["Formatted Date"]).drop_duplicates()
    df = df.set_index("Formatted Date")

    # Normalize humidity
    if df["Humidity"].max() > 1:
        df["Humidity"] = df["Humidity"] / 100.0

    # Fill numeric NaNs
    df = df.fillna(df.mean(numeric_only=True))

    # Select numeric columns only to avoid string aggregation issues
    numeric_cols = df.select_dtypes(include="number").columns

    # ===== DAILY AGGREGATION =====
    daily = df[numeric_cols].resample("D").mean().reset_index()

    # Find the wind speed column by partial match to avoid column rename issues
    wind_col_candidates = [col for col in daily.columns if "wind" in col.lower()]
    if not wind_col_candidates:
        raise KeyError("Wind speed column not found in DAILY dataset!")

    wind_col = wind_col_candidates[0]
    daily["wind_strength"] = daily[wind_col].apply(wind_strength)

    daily_path = os.path.join(PROCESSED_DIR, "daily_weather.csv")
    daily.to_csv(daily_path, index=False)

    # ===== MONTHLY AGGREGATION =====
    monthly = df[numeric_cols].resample("M").mean().reset_index()
    monthly["month"] = monthly["Formatted Date"].dt.to_period("M").astype(str)

    # Mode precip type per month
    mode = (
        df.groupby(df.index.to_period("M"))["Precip Type"]
        .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan)
    )

    # Convert PeriodIndex -> str for merge compatibility
    mode.index = mode.index.astype(str)

    # Merge modes into monthly
    monthly = monthly.merge(
        mode.rename("mode_precip_type"),
        left_on="month",
        right_index=True,
        how="left"
    )

    monthly_path = os.path.join(PROCESSED_DIR, "monthly_weather.csv")
    monthly.to_csv(monthly_path, index=False)

    return daily_path, monthly_path


# ============= VALIDATE =============
def validate(daily_path):
    """Validate weather ranges and data sanity."""
    daily = pd.read_csv(daily_path)

    if not daily["Temperature (C)"].between(-50, 50).all():
        raise ValueError("Temperature out of range!")

    if not daily["Humidity"].between(0, 1).all():
        raise ValueError("Humidity not in [0,1]!")

    wind_col_candidates = [col for col in daily.columns if "wind" in col.lower()]
    if wind_col_candidates:
        if (daily[wind_col_candidates[0]] < 0).any():
            raise ValueError("Negative wind speed detected!")

    return True


# ============= LOAD =============

import sqlite3

def load_daily(daily_path):
    conn = sqlite3.connect(DB_PATH)  # <-- direct SQLite DBAPI connection
    df = pd.read_csv(daily_path)

    df.to_sql(
        "daily_weather",
        con=conn,
        if_exists="replace",
        index=False,
    )

    conn.close()


def load_monthly(monthly_path):
    conn = sqlite3.connect(DB_PATH)  # <-- same here
    df = pd.read_csv(monthly_path)

    df.to_sql(
        "monthly_weather",
        con=conn,
        if_exists="replace",
        index=False,
    )

    conn.close()


