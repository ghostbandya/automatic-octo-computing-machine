"""
power_data.py
-------------
Pulls Day-Ahead electricity prices for five European bidding zones
from the ENTSO-E Transparency Platform.

Why Day-Ahead prices?
  The Day-Ahead (DA) auction clears the following day's hourly power schedule.
  DA prices reflect the marginal cost of power generation — when gas is the
  marginal fuel, DA prices move closely with TTF and EUA. Cross-zone DA spreads
  reveal grid congestion (high spread = interconnector constrained) or renewable
  mix differences (low DA in ES often reflects high Spanish solar generation).

Zones covered: DE_LU (Germany), FR (France), NL (Netherlands), BE (Belgium), ES (Spain)
GB excluded  : Post-Brexit, Great Britain (N2EX market) no longer publishes
               Day-Ahead prices on the ENTSO-E Transparency Platform.

Outputs: data/raw/power_da.csv

Requires: ENTSO_E_TOKEN in .env  (free registration at transparency.entsoe.eu)
Install:  pip install entsoe-py pandas python-dotenv
"""

import os
import time
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
ENTSO_E_TOKEN = os.getenv("ENTSO_E_TOKEN")

# Bidding zone codes used by the entsoe-py client
# DE_LU = Germany-Luxembourg combined zone (merged in 2018)
AREAS = {
    "DE": "DE_LU",
    "FR": "FR",
    "NL": "NL",
    "BE": "BE",
    "ES": "ES",
}

# Retry config — ENTSO-E returns 503 under moderate load, especially for
# peripheral zones (NL, BE, ES). Brief delays reduce the hit rate.
RETRY_ATTEMPTS = 3   # max attempts per zone before giving up
RETRY_DELAY_S  = 5   # seconds between retries (avoids hammering the server)
ZONE_DELAY_S   = 2   # seconds between zone fetches (reduces server load)


def fetch_day_ahead_prices(days_back: int = 180) -> pd.DataFrame:
    """
    Pulls hourly Day-Ahead prices for all zones in AREAS, resamples to daily
    averages. Zones that fail after all retries are skipped with a warning so
    a single failed zone doesn't abort the whole fetch.

    Why daily averages?
      Hourly granularity adds noise for the risk monitor's purpose — daily
      averages smooth out off-peak troughs and on-peak spikes to give a cleaner
      cross-commodity comparison with TTF and EUA (which are also daily).

    Returns
    -------
    pd.DataFrame with columns:
        date, de_da_price_eur_mwh, fr_da_price_eur_mwh,
        nl_da_price_eur_mwh, be_da_price_eur_mwh, es_da_price_eur_mwh
    """
    if not ENTSO_E_TOKEN:
        raise EnvironmentError("ENTSO_E_TOKEN not found in .env file.")

    try:
        from entsoe import EntsoePandasClient
    except ImportError:
        raise ImportError("Run: pip install entsoe-py")

    client = EntsoePandasClient(api_key=ENTSO_E_TOKEN)
    # ENTSO-E requires timezone-aware timestamps — Europe/Berlin is the standard
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

                # Resample hourly → daily mean; drop timezone for CSV compatibility
                daily = raw.resample("D").mean().reset_index()
                daily.columns = ["timestamp", col]
                daily["date"] = (pd.to_datetime(daily["timestamp"])
                                 .dt.tz_localize(None).dt.normalize())
                daily = daily[["date", col]]
                frames[country] = daily

                latest = daily.iloc[-1]
                print(f"[power_data]   {country}: {len(daily)} rows | "
                      f"latest EUR {latest[col]:.2f}/MWh on {latest['date'].date()}")
                break   # success — skip remaining retries

            except Exception as e:
                err_msg = str(e) or type(e).__name__
                if attempt < RETRY_ATTEMPTS:
                    print(f"[power_data]   {country}: attempt {attempt} failed "
                          f"({err_msg}) — retrying in {RETRY_DELAY_S}s ...")
                    time.sleep(RETRY_DELAY_S)
                else:
                    # Final attempt failed — skip zone, don't abort the whole fetch
                    print(f"[power_data]   {country}: FAILED after "
                          f"{RETRY_ATTEMPTS} attempts — {err_msg}")

        # Brief pause between zones to reduce the risk of 503 errors
        time.sleep(ZONE_DELAY_S)

    if not frames:
        raise RuntimeError(
            "[power_data] No power data retrieved for any zone. Check ENTSO_E_TOKEN."
        )

    # Dynamically merge all successfully-fetched frames on date.
    # Using outer merge so a missing day in one zone doesn't drop rows for others.
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
