"""
carbon_data.py
--------------
Pulls EUA (EU Carbon Allowance) daily price data.
Primary source : CO2.L (WisdomTree Carbon ETP, LSE) via yfinance
                 — closely tracks EUA spot, free, no API key required.
Fallback       : CARP.PA (Amundi MSCI Carbon ETP, Euronext Paris)

Outputs: data/raw/carbon_eua.csv

Install: pip install yfinance pandas
"""

import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta


# CO2.L tracks the ICE EUA futures roll — best freely available proxy
PRIMARY_TICKER  = "CO2.L"
FALLBACK_TICKER = "CARP.PA"


def fetch_eua_prices(days_back: int = 90) -> pd.DataFrame:
    """
    Pulls EUA price proxy time series.

    Parameters
    ----------
    days_back : int
        How many calendar days of history to pull

    Returns
    -------
    pd.DataFrame with columns: date, eua_price_eur
    """
    end   = datetime.today()
    start = end - timedelta(days=days_back)

    df = _fetch_ticker(PRIMARY_TICKER, start, end)

    if df is None or df.empty:
        print(f"[carbon_data] Primary ticker {PRIMARY_TICKER} empty, trying fallback ...")
        df = _fetch_ticker(FALLBACK_TICKER, start, end)

    if df is None or df.empty:
        raise RuntimeError(
            "[carbon_data] Both tickers returned no data. "
            "Check your internet connection or try again later."
        )

    # Save
    out_path = os.path.join("data", "raw", "carbon_eua.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False)

    latest = df.iloc[-1]
    print(f"[carbon_data] ✓ {len(df)} rows saved to {out_path}")
    print(f"[carbon_data]   Latest ({latest['date'].date()}): "
          f"EUA ≈ €{latest['eua_price_eur']:.2f}/tonne")

    return df


def _fetch_ticker(ticker: str, start: datetime, end: datetime) -> pd.DataFrame | None:
    """Internal helper — downloads a Yahoo Finance ticker and normalises columns."""
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
