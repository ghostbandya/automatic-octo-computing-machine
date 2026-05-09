"""
carbon_data.py
--------------
Pulls EUA (EU Emission Allowance) daily price data.

Why EUA prices matter for this monitor:
  EUA is the marginal cost of carbon for EU power generators. Higher EUA
  prices raise the cost of coal-fired generation more than gas-fired
  (coal emits ~2x more CO2/MWh), which compresses the clean dark spread
  and encourages fuel switching from coal to gas — directly supporting
  both gas demand and power prices.

Data source:
  Primary : CO2.L  — WisdomTree Carbon ETP (LSE, GBP-settled but EUR-hedged).
            Tracks the ICE EUA front-month futures roll. Best freely available
            proxy for EUA spot, no API key required.
  Fallback: CARP.PA — Amundi MSCI Carbon ETP (Euronext Paris, EUR-settled).

Note: CO2.L prices are in GBP on Yahoo Finance but closely track EUR EUA.
      For this monitor we use the close price as a directional proxy —
      for precise EUR/tonne levels, an ICE or Bloomberg feed would be used
      in a production environment.

Outputs: data/raw/carbon_eua.csv

Install: pip install yfinance pandas
"""

import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta


PRIMARY_TICKER  = "CO2.L"
FALLBACK_TICKER = "CARP.PA"


def fetch_eua_prices(days_back: int = 180) -> pd.DataFrame:
    """
    Pulls EUA price proxy time series via Yahoo Finance.

    We default to 180 days so that the 90-day momentum metric
    (pct_change(90)) has enough history to produce non-NaN values
    across the visible chart window.

    The end date is set to today + 1 day because yfinance treats
    the end parameter as exclusive — without the +1, today's session
    would be omitted from the pull.

    Parameters
    ----------
    days_back : int
        Calendar days of history to pull (default 180)

    Returns
    -------
    pd.DataFrame with columns: date, eua_price_eur
    """
    # +1 day on end because yfinance range is [start, end) — exclusive end
    end   = datetime.today() + timedelta(days=1)
    start = end - timedelta(days=days_back + 1)

    df = _fetch_ticker(PRIMARY_TICKER, start, end)

    if df is None or df.empty:
        print(f"[carbon_data] Primary ticker {PRIMARY_TICKER} empty, trying fallback ...")
        df = _fetch_ticker(FALLBACK_TICKER, start, end)

    if df is None or df.empty:
        raise RuntimeError(
            "[carbon_data] Both tickers returned no data. "
            "Check your internet connection or try again later."
        )

    out_path = os.path.join("data", "raw", "carbon_eua.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False)

    latest = df.iloc[-1]
    print(f"[carbon_data] ✓ {len(df)} rows saved to {out_path}")
    print(f"[carbon_data]   Latest ({latest['date'].date()}): "
          f"EUA ≈ €{latest['eua_price_eur']:.2f}/tonne")

    return df


def _fetch_ticker(ticker: str, start: datetime, end: datetime) -> pd.DataFrame | None:
    """
    Internal helper — downloads a Yahoo Finance ticker and normalises columns.

    Returns None (rather than raising) so the caller can cleanly try the
    fallback ticker without exception handling noise.
    """
    print(f"[carbon_data] Fetching {ticker} from Yahoo Finance ...")
    try:
        raw = yf.Ticker(ticker).history(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d")
        )
        if raw.empty:
            return None

        df = raw.reset_index()[["Date", "Close"]].copy()
        df.rename(columns={"Date": "date", "Close": "eua_price_eur"}, inplace=True)
        # Strip timezone info — all downstream processing assumes naive UTC dates
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
        df["eua_price_eur"] = pd.to_numeric(df["eua_price_eur"], errors="coerce")
        df = df.dropna().sort_values("date").reset_index(drop=True)
        return df

    except Exception as e:
        print(f"[carbon_data] Error fetching {ticker}: {e}")
        return None


def get_latest_eua_price() -> float:
    """Quick helper — returns latest EUA proxy price as a float."""
    df = fetch_eua_prices(days_back=5)
    return float(df["eua_price_eur"].iloc[-1])


if __name__ == "__main__":
    df = fetch_eua_prices()
    print("\n--- Last 5 rows ---")
    print(df.tail(5).to_string(index=False))
