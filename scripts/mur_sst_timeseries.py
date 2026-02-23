#!/usr/bin/env python3
"""
MUR SST Daily Time Series Extractor
====================================
Fetches daily 1-km sea surface temperature (SST) from the JPL MUR SST v4.1
dataset via NOAA CoastWatch ERDDAP (no authentication required).

Dataset: Multi-scale Ultra-high Resolution (MUR) SST Analysis fv04.1
  - Resolution: 0.01° (~1 km), daily
  - Archive:    June 2002 – present
  - Sensors:    MODIS, AMSR2, AVHRR, VIIRS + in-situ
  - Provider:   NASA JPL / GHRSST

Usage:
    python mur_sst_timeseries.py --lat 36.95 --lon -122.0 --start 2020-01-01 --end 2024-12-31
    python mur_sst_timeseries.py --lat 36.95 --lon -122.0 --start 2020-01-01 --end 2024-12-31 --output my_sst.csv --plot

Dependencies:
    pip install pandas matplotlib requests
"""

import argparse
import sys
import time
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import requests
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for saving plots
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ERDDAP_BASE = "https://coastwatch.pfeg.noaa.gov/erddap/griddap"
DATASET_ID = "jplMURSST41"

# ERDDAP can time out on very large requests, so we chunk by time
MAX_DAYS_PER_REQUEST = 365  # ~1 year per chunk (safe for single-point requests)
REQUEST_TIMEOUT = 120       # seconds
RETRY_COUNT = 3
RETRY_DELAY = 5             # seconds between retries


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def build_erddap_url(lat: float, lon: float,
                     start_date: str, end_date: str,
                     file_type: str = "csv") -> str:
    """
    Build a CoastWatch ERDDAP griddap URL to request MUR SST at a single point.

    ERDDAP snaps to the nearest grid cell, so the exact lat/lon of the
    returned data may differ slightly (by up to ~0.005°) from the request.

    Parameters
    ----------
    lat : float   Latitude  (-89.99 to 89.99)
    lon : float   Longitude (-179.99 to 180.0)
    start_date : str  ISO date, e.g. "2020-01-01"
    end_date   : str  ISO date, e.g. "2020-12-31"
    file_type  : str  "csv" or "nc" (NetCDF)

    Returns
    -------
    str : fully formed ERDDAP URL
    """
    # analysed_sst is in Kelvin in the raw NetCDF but ERDDAP returns °C
    # We also grab analysis_error (uncertainty estimate)
    url = (
        f"{ERDDAP_BASE}/{DATASET_ID}.{file_type}"
        f"?analysed_sst[({start_date}T09:00:00Z):1:({end_date}T09:00:00Z)]"
        f"[({lat}):1:({lat})]"
        f"[({lon}):1:({lon})],"
        f"analysis_error[({start_date}T09:00:00Z):1:({end_date}T09:00:00Z)]"
        f"[({lat}):1:({lat})]"
        f"[({lon}):1:({lon})]"
    )
    return url


def fetch_chunk(lat: float, lon: float,
                start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch one time-chunk of SST data and return a DataFrame."""
    url = build_erddap_url(lat, lon, start_date, end_date)

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            print(f"  Requesting {start_date} to {end_date} (attempt {attempt}) ...")
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            break
        except requests.exceptions.RequestException as exc:
            if attempt == RETRY_COUNT:
                print(f"  ERROR: Failed after {RETRY_COUNT} attempts: {exc}")
                raise
            print(f"  Retrying in {RETRY_DELAY}s ...")
            time.sleep(RETRY_DELAY)

    # ERDDAP CSV has a header row, then a units row, then data
    # Skip the units row (row index 1 after header)
    df = pd.read_csv(StringIO(resp.text), skiprows=[1], parse_dates=["time"])
    return df


def fetch_sst_timeseries(lat: float, lon: float,
                         start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch a full daily SST time series, chunking long requests automatically.

    Returns
    -------
    pd.DataFrame with columns:
        time            datetime64  (UTC)
        latitude        float       (actual grid-cell latitude)
        longitude       float       (actual grid-cell longitude)
        analysed_sst    float       (°C, foundation SST)
        analysis_error  float       (°C, estimated uncertainty)
    """
    dt_start = datetime.strptime(start_date, "%Y-%m-%d")
    dt_end = datetime.strptime(end_date, "%Y-%m-%d")

    chunks = []
    chunk_start = dt_start

    while chunk_start <= dt_end:
        chunk_end = min(chunk_start + timedelta(days=MAX_DAYS_PER_REQUEST - 1),
                        dt_end)
        df = fetch_chunk(lat, lon,
                         chunk_start.strftime("%Y-%m-%d"),
                         chunk_end.strftime("%Y-%m-%d"))
        chunks.append(df)
        chunk_start = chunk_end + timedelta(days=1)

    result = pd.concat(chunks, ignore_index=True)
    result.sort_values("time", inplace=True)
    result.reset_index(drop=True, inplace=True)

    # Report the actual grid-cell coordinates used by ERDDAP
    actual_lat = result["latitude"].iloc[0]
    actual_lon = result["longitude"].iloc[0]
    print(f"\nActual grid cell: lat={actual_lat:.4f}, lon={actual_lon:.4f}")
    print(f"Records retrieved: {len(result)}")
    print(f"Date range: {result['time'].iloc[0].date()} to "
          f"{result['time'].iloc[-1].date()}")
    print(f"SST range: {result['analysed_sst'].min():.2f} to "
          f"{result['analysed_sst'].max():.2f} °C")

    return result


def plot_timeseries(df: pd.DataFrame, lat: float, lon: float,
                    plot_file: str = "mur_sst_timeseries.png"):
    """Create a publication-quality time series plot and save to file."""
    fig, ax = plt.subplots(figsize=(14, 5))

    ax.plot(df["time"], df["analysed_sst"],
            linewidth=0.5, color="#1f77b4", alpha=0.8, label="Daily SST")

    # Optional: add uncertainty band
    if "analysis_error" in df.columns:
        ax.fill_between(
            df["time"],
            df["analysed_sst"] - df["analysis_error"],
            df["analysed_sst"] + df["analysis_error"],
            alpha=0.15, color="#1f77b4", label="±1σ uncertainty"
        )

    ax.set_xlabel("Date")
    ax.set_ylabel("Sea Surface Temperature (°C)")
    ax.set_title(
        f"MUR SST v4.1 Daily Time Series\n"
        f"Lat {lat:.4f}°, Lon {lon:.4f}°",
        fontsize=13
    )
    ax.legend(loc="upper right", framealpha=0.9)
    ax.grid(True, alpha=0.3)

    # Smart date formatting
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate(rotation=45)

    plt.tight_layout()
    plt.savefig(plot_file, dpi=150)
    plt.close()
    print(f"Plot saved to: {plot_file}")


# ---------------------------------------------------------------------------
# Main / CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch daily MUR SST (1 km) time series at a point via ERDDAP.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Santa Cruz, CA coastal waters — full year
  python mur_sst_timeseries.py --lat 36.95 --lon -122.0 --start 2023-01-01 --end 2023-12-31

  # Multi-year with plot and custom output filename
  python mur_sst_timeseries.py --lat 25.0 --lon -80.0 --start 2010-01-01 --end 2023-12-31 \\
      --output florida_keys_sst.csv --plot --plotfile florida_keys_sst.png

  # Great Barrier Reef
  python mur_sst_timeseries.py --lat -18.3 --lon 147.7 --start 2015-01-01 --end 2024-12-31 --plot
        """
    )
    parser.add_argument("--lat", type=float, required=True,
                        help="Latitude (-89.99 to 89.99)")
    parser.add_argument("--lon", type=float, required=True,
                        help="Longitude (-179.99 to 180.0)")
    parser.add_argument("--start", type=str, required=True,
                        help="Start date (YYYY-MM-DD). Earliest: 2002-06-01")
    parser.add_argument("--end", type=str, required=True,
                        help="End date (YYYY-MM-DD).")
    parser.add_argument("--output", type=str, default="mur_sst_timeseries.csv",
                        help="Output CSV filename (default: mur_sst_timeseries.csv)")
    parser.add_argument("--plot", action="store_true",
                        help="Generate a time series plot")
    parser.add_argument("--plotfile", type=str, default="mur_sst_timeseries.png",
                        help="Plot filename (default: mur_sst_timeseries.png)")

    args = parser.parse_args()

    # --- Validate inputs ---
    if not (-90 <= args.lat <= 90):
        sys.exit("ERROR: Latitude must be between -90 and 90.")
    if not (-180 <= args.lon <= 180):
        sys.exit("ERROR: Longitude must be between -180 and 180.")

    start_dt = datetime.strptime(args.start, "%Y-%m-%d")
    end_dt = datetime.strptime(args.end, "%Y-%m-%d")
    earliest = datetime(2002, 6, 1)

    if start_dt < earliest:
        print(f"WARNING: MUR SST starts 2002-06-01. Adjusting start date.")
        args.start = "2002-06-01"
    if end_dt <= start_dt:
        sys.exit("ERROR: End date must be after start date.")

    # --- Fetch data ---
    print(f"Fetching MUR SST for lat={args.lat}, lon={args.lon}")
    print(f"Period: {args.start} to {args.end}\n")

    df = fetch_sst_timeseries(args.lat, args.lon, args.start, args.end)

    # --- Save CSV ---
    df.to_csv(args.output, index=False)
    print(f"Data saved to: {args.output}")

    # --- Plot ---
    if args.plot:
        plot_timeseries(df, args.lat, args.lon, args.plotfile)

    return df


if __name__ == "__main__":
    main()
