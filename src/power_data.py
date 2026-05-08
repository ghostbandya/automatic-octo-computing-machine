"""
power_data.py
-------------
Pulls Day-Ahead electricity prices for six European bidding zones
from the ENTSO-E Transparency Platform.

Zones: DE_LU (Germany), FR (France), GB (Great Britain),
       NL (Netherlands), BE (Belgium), ES (Spain)

Outputs: data/raw/power_da.csv

Requires: ENTSO_E_TOKEN in .env
Install:  pip install entsoe-py pandas python-dotenv
"""

import os
import time
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
ENTSO_E_TOKEN = os.getenv("ENTSO_E_TOKEN")

# Bidding zone codes (entsoe-py string keys)
# Note: GB removed — post-Brexit, GB (N2EX market) no longer publishes
#       Day-Ahead prices on ENTSO-E Transparency Platform.
AREAS = {
    "DE": "DE_LU",   # Germany-Luxembourg bidding zone
    "FR": "FR",      # France
    "NL": "NL",      # Netherlands
    "BE": "BE",      # Belgium
    "ES": "ES",      # Spain
}

RETRY_ATTEMPTS = 3   # retries per zone on 503/timeout
RETRY_DELAY_S  = 5   # seconds between retries
ZONE_DELAY_S   = 2   # seconds between zone fetches (reduces server load)


def fetch_day_ahead_prices(days_back: int = 90) -> pd.DataFrame:
    """
    Pulls hourly Day-Ahead prices for all zones in AREAS, resamples to daily averages.
    Zones that fail (e.g. GB returning GBP) are skipped with a warning.

    Returns
    -------
    pd.DataFrame with columns:
        date, de_da_price_eur_mwh, fr_da_price_eur_mwh, gb_da_price_eur_mwh,
        nl_da_price_eur_mwh, be_da_price_eur_mwh, es_da_price_eur_mwh
    Note: GB prices are in GBP/MWh from ENTSO-E; column is labelled accordingly.
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
        col = f"{country.lower()}_da_price_eur_mwh"
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                raw = client.query_day_ahead_prices(area, start=start, end=end)
                if raw is None or raw.empty:
                    raise ValueError("Empty response from ENTSO-E")
                daily = raw.resample("D").mean().reset_index()
                daily.columns = ["timestamp", col]
                daily["date"] = pd.to_datetime(daily["timestamp"]).dt.tz_localize(None).dt.normalize()
                daily = daily[["date", col]]
                frames[country] = daily
                latest = daily.iloc[-1]
                print(f"[power_data]   {country}: {len(daily)} rows | "
                      f"latest EUR {latest[col]:.2f}/MWh on {latest['date'].date()}")
                break  # success — no more retries
            except Exception as e:
                err_msg = str(e) or type(e).__name__
                if attempt < RETRY_ATTEMPTS:
                    print(f"[power_data]   {country}: attempt {attempt} failed ({err_msg}) "
                          f"— retrying in {RETRY_DELAY_S}s ...")
                    time.sleep(RETRY_DELAY_S)
                else:
                    print(f"[power_data]   {country}: FAILED after {RETRY_ATTEMPTS} attempts — {err_msg}")
        time.sleep(ZONE_DELAY_S)  # brief pause between zones to reduce 503 risk

    if not frames:
        raise RuntimeError("[power_data] No power data retrieved. Check ENTSO_E_TOKEN.")

    # Dynamically merge all successful frames on date
    countries_fetched = [c for c in AREAS if c in frames]
    df = frames[countries_fetched[0]]
    for c in countries_fetched[1:]:
        df = df.merge(frames[c], on="date", how="outer")
    df = df.sort_values("date").reset_index(drop=True)

    out_path = os.path.join("data", "raw", "power_da.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False)
    zones_str = ", ".join(countries_fetched)
    print(f"[power_data] ✓ {len(df)} rows saved to {out_path}  [{zones_str}]")

    return df


if __name__ == "__main__":
    df = fetch_day_ahead_prices()
    print("\n--- Last 5 rows ---")
    print(df.tail(5).to_string(index=False))
