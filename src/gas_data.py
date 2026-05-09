"""
gas_data.py
-----------
Pulls EU aggregated gas storage data from the GIE AGSI+ API.
Endpoint: /api/data/eu  (EU aggregate across all member states)

Why GIE AGSI+?
  The Gas Infrastructure Europe (GIE) AGSI+ platform is the authoritative
  source for EU gas storage transparency data, mandated under EU Regulation
  715/2009. It covers ~90% of EU storage capacity and is published daily
  (with a ~1 day lag — data for gas day T is available on T+1 morning).

Outputs:
  data/raw/gas_storage.csv      -- trailing N days (refreshed on every run)
  data/raw/gas_storage_5yr.csv  -- 5-year history (cached, re-fetched if >7 days stale)

Requires: GIE_API_KEY in .env   
Install:  pip install requests pandas python-dotenv
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("GIE_API_KEY")

BASE_URL = "https://agsi.gie.eu/api/data/eu"


def fetch_gas_storage(days_back: int = 180) -> pd.DataFrame:
    """
    Pulls EU aggregated gas storage data from GIE AGSI+.

    We default to 180 days (rather than 90) so that derived metrics such as
    the 90-day TTF momentum and rolling correlations have a full window of
    data to compute against without producing NaN-heavy outputs.

    Returns
    -------
    pd.DataFrame with columns:
        date, storage_pct_full, gas_in_storage_twh, working_gas_volume_twh,
        injection_gwh, withdrawal_gwh, trend_ppt, consumption_gwh
    """
    if not API_KEY:
        raise EnvironmentError("GIE_API_KEY not found in .env file.")

    end_date   = datetime.today()
    start_date = end_date - timedelta(days=days_back)

    params = {
        "from": start_date.strftime("%Y-%m-%d"),
        "till": end_date.strftime("%Y-%m-%d"),
        "size": 300,   # max records per page; 300 comfortably covers 180 days
    }
    headers = {"x-key": API_KEY}

    print(f"[gas_data] Fetching EU gas storage ({start_date.date()} → {end_date.date()}) ...")

    response = requests.get(BASE_URL, params=params, headers=headers, timeout=15)
    response.raise_for_status()
    payload = response.json()

    records = payload.get("data", [])
    if not records:
        raise ValueError(
            f"[gas_data] No data returned. Check your API key.\n"
            f"Response: {payload}"
        )

    df = pd.DataFrame(records)

    # Rename API field names to readable column names
    keep = {
        "gasDayStart":      "date",
        "full":             "storage_pct_full",      # % of working gas volume filled
        "gasInStorage":     "gas_in_storage_twh",    # absolute level (TWh)
        "workingGasVolume": "working_gas_volume_twh",# total EU working gas capacity
        "injection":        "injection_gwh",         # net gas injected that day (GWh)
        "withdrawal":       "withdrawal_gwh",        # net gas withdrawn that day (GWh)
        "trend":            "trend_ppt",             # day-on-day change in fill % (ppt)
        "consumption":      "consumption_gwh",       # estimated EU gas consumption (GWh)
    }
    df = df[[c for c in keep if c in df.columns]].rename(columns=keep)

    # Type coercion — API returns all fields as strings
    df["date"] = pd.to_datetime(df["date"])
    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values("date").reset_index(drop=True)

    out_path = os.path.join("data", "raw", "gas_storage.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False)

    latest = df.iloc[-1]
    print(f"[gas_data] ✓ {len(df)} rows saved to {out_path}")
    print(f"[gas_data]   Latest ({latest['date'].date()}): "
          f"storage = {latest['storage_pct_full']:.1f}% full | "
          f"trend = {latest['trend_ppt']:+.2f} ppt")

    return df


def fetch_gas_storage_5yr() -> pd.DataFrame:
    """
    Pulls ~5 years of EU gas storage history from GIE AGSI+ (paginated).

    Why 5 years?
      The 5yr window captures at least one full seasonal cycle in each storage
      year (injection Apr–Oct, withdrawal Nov–Mar), giving a statistically
      robust seasonal average and ±1 std band for Chart 1 and the
      storage-vs-seasonal-average metric.

    The API paginates at 300 records per page, so we loop until we've received
    all records (checked against the `total` field in the response payload).

    Saves to data/raw/gas_storage_5yr.csv
    """
    if not API_KEY:
        raise EnvironmentError("GIE_API_KEY not found in .env file.")

    end_date   = datetime.today()
    start_date = end_date.replace(year=end_date.year - 5)

    headers   = {"x-key": API_KEY}
    all_records = []
    page      = 1
    page_size = 300

    print(f"[gas_data] Fetching 5yr EU storage history "
          f"({start_date.date()} -> {end_date.date()}) ...")

    while True:
        params = {
            "from": start_date.strftime("%Y-%m-%d"),
            "till": end_date.strftime("%Y-%m-%d"),
            "size": page_size,
            "page": page,
        }
        response = requests.get(BASE_URL, params=params, headers=headers, timeout=20)
        response.raise_for_status()
        payload = response.json()

        records = payload.get("data", [])
        if not records:
            break                      # no more pages — exit loop
        all_records.extend(records)

        # `total` is the full result count across all pages; stop when we have it all
        total = payload.get("total", len(all_records))
        if len(all_records) >= total:
            break
        page += 1

    if not all_records:
        raise ValueError("[gas_data] No 5yr history returned. Check API key.")

    df = pd.DataFrame(all_records)
    # We only need date and fill % for the seasonal average calculation
    df = df[["gasDayStart", "full"]].rename(
        columns={"gasDayStart": "date", "full": "storage_pct_full"}
    )
    df["date"]             = pd.to_datetime(df["date"])
    df["storage_pct_full"] = pd.to_numeric(df["storage_pct_full"], errors="coerce")
    df = df.dropna().sort_values("date").reset_index(drop=True)

    out_path = os.path.join("data", "raw", "gas_storage_5yr.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"[gas_data] 5yr history: {len(df)} rows saved to {out_path}")

    return df


def get_latest_storage_pct() -> float:
    """Quick helper — returns today's EU storage % full as a float."""
    df = fetch_gas_storage(days_back=5)
    return float(df["storage_pct_full"].iloc[-1])


if __name__ == "__main__":
    df = fetch_gas_storage()
    print("\n--- Last 5 rows ---")
    print(df.tail(5).to_string(index=False))
