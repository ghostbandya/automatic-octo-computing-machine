"""
gas_data.py
-----------
Pulls EU aggregated gas storage data from GIE AGSI+ API.
Endpoint: /api/data/eu  (EU aggregate)
Outputs:  data/raw/gas_storage.csv

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


def fetch_gas_storage(days_back: int = 90) -> pd.DataFrame:
    """
    Pulls EU aggregated gas storage data from GIE AGSI+.

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
        "size": 300,
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

    keep = {
        "gasDayStart":      "date",
        "full":             "storage_pct_full",
        "gasInStorage":     "gas_in_storage_twh",
        "workingGasVolume": "working_gas_volume_twh",
        "injection":        "injection_gwh",
        "withdrawal":       "withdrawal_gwh",
        "trend":            "trend_ppt",
        "consumption":      "consumption_gwh",
    }
    df = df[[c for c in keep if c in df.columns]].rename(columns=keep)

    # Type coercion
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


def get_latest_storage_pct() -> float:
    """Quick helper — returns today's EU storage % full as a float."""
    df = fetch_gas_storage(days_back=5)
    return float(df["storage_pct_full"].iloc[-1])


if __name__ == "__main__":
    df = fetch_gas_storage()
    print("\n--- Last 5 rows ---")
    print(df.tail(5).to_string(index=False))
