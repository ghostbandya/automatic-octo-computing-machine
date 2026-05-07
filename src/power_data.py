"""
power_data.py
-------------
Pulls Day-Ahead electricity prices for Germany and France from
the ENTSO-E Transparency Platform.

Outputs: data/raw/power_da.csv

Requires: ENTSO_E_TOKEN in .env
Install:  pip install entsoe-py pandas python-dotenv
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
ENTSO_E_TOKEN = os.getenv("ENTSO_E_TOKEN")

# Bidding zone codes (entsoe-py string keys)
AREAS = {
    "DE": "DE_LU",   # Germany-Luxembourg bidding zone
    "FR": "FR",      # France
}


def fetch_day_ahead_prices(days_back: int = 90) -> pd.DataFrame:
    """
    Pulls hourly Day-Ahead prices for DE and FR, resamples to daily averages.

    Returns
    -------
    pd.DataFrame with columns:
        date, de_da_price_eur_mwh, fr_da_price_eur_mwh
    """
    if not ENTSO_E_TOKEN:
        raise EnvironmentError("ENTSO_E_TOKEN not found in .env file.")

    try:
        from entsoe import EntsoePandasClient
    except ImportError:
        raise ImportError("Run: pip install entsoe-py")

    client = EntsoePandasClient(api_key=ENTSO_E_TOKEN)
    end    = pd.Timestamp(datetime.today().strftime("%Y-%m-%d"), tz="Europe/Berlin")
    start  = end - pd.Timedelta(days=days_back)

    print(f"[power_data] Fetching Day-Ahead prices ({start.date()} → {end.date()}) ...")

    frames = {}
    for country, area in AREAS.items():
        try:
            raw = client.query_day_ahead_prices(area, start=start, end=end)
            daily = raw.resample("D").mean().reset_index()
            daily.columns = ["timestamp", f"{country.lower()}_da_price_eur_mwh"]
            daily["date"] = pd.to_datetime(daily["timestamp"]).dt.tz_localize(None).dt.normalize()
            daily = daily[["date", f"{country.lower()}_da_price_eur_mwh"]]
            frames[country] = daily
            latest = daily.iloc[-1]
            print(f"[power_data]   {country}: {len(daily)} rows | "
                  f"latest €{latest[f'{country.lower()}_da_price_eur_mwh']:.2f}/MWh "
                  f"on {latest['date'].date()}")
        except Exception as e:
            print(f"[power_data]   {country}: FAILED — {e}")

    if not frames:
        raise RuntimeError("[power_data] No power data retrieved. Check ENTSO_E_TOKEN.")

    # Merge DE + FR on date
    df = frames["DE"].merge(frames["FR"], on="date", how="outer").sort_values("date")
    df = df.reset_index(drop=True)

    out_path = os.path.join("data", "raw", "power_da.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"[power_data] ✓ {len(df)} rows saved to {out_path}")

    return df


if __name__ == "__main__":
    df = fetch_day_ahead_prices()
    print("\n--- Last 5 rows ---")
    print(df.tail(5).to_string(index=False))
