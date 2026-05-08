"""
metrics.py
----------
Computes daily monitor metrics from cleaned data files.
Outputs: data/metrics.csv

Core metrics (always computed):
  1. EU Gas Storage % Full
  2. Storage deviation from 5yr seasonal average (ppt)
  3. Storage injection rate vs 30-day average (GWh/day)
  4. TTF M+1 price (EUR/MWh)
  5. TTF 30-day price momentum (%)
  6. EUA spot price proxy (EUR/tonne)
  7. EUA 30-day price momentum (%)
  8. Gas-Carbon 30-day rolling correlation

Power metrics (computed when data/raw/power_da.csv exists):
  9+. Day-Ahead price per zone: DE, FR, GB, NL, BE, ES
  *   Clean Spark Spread DE (EUR/MWh)  — CSS = DA_DE - TTF/0.49 - 0.202*EUA
  *   DE Power-Gas Spread (EUR/MWh)

Install: pip install pandas numpy
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime


# --- Metric relevance notes (used in the daily brief) ---
METRIC_NOTES = {
    "storage_pct_full":          "Core gas tightness signal — below 35% in May is historically low, supportive of power prices.",
    "storage_vs_5yr_avg_ppt":    "Deviation from seasonal norm — negative value means below 5yr average, bullish for gas/power.",
    "injection_vs_30d_avg_gwh":  "Pace of storage build — slow injection in summer tightens winter supply outlook.",
    "ttf_price_eur_mwh":         "European gas benchmark — direct cost input to gas-fired power generation.",
    "ttf_30d_momentum_pct":      "Gas price trend — rising momentum signals tightening supply or rising demand.",
    "eua_price_eur":             "EU carbon price — higher EUA raises marginal cost of coal generation, supports gas-to-power.",
    "eua_30d_momentum_pct":      "Carbon price trend — accelerating EUA drives fuel switching from coal to gas.",
    "gas_carbon_30d_corr":       "Cross-commodity linkage — high correlation confirms carbon is amplifying gas price moves.",
}


GAS_EFFICIENCY = 0.49    # CCGT thermal efficiency
GAS_CARBON_INT = 0.202   # tCO2/MWh gas burned

# DA price zones — columns are {code}_da_price_eur_mwh
# GB excluded: post-Brexit data no longer published on ENTSO-E
POWER_ZONES = ["de", "fr", "nl", "be", "es"]


def compute_metrics() -> pd.DataFrame:
    """
    Loads raw CSVs, computes all metrics, and saves to data/metrics.csv.
    Power metrics are included when data/raw/power_da.csv exists.

    Returns
    -------
    pd.DataFrame — one row per date with all metric columns
    """

    # --- Load raw data ---
    gas      = pd.read_csv("data/raw/gas_storage.csv",    parse_dates=["date"])
    gas_5yr  = pd.read_csv("data/raw/gas_storage_5yr.csv", parse_dates=["date"])
    carbon   = pd.read_csv("data/raw/carbon_eua.csv",      parse_dates=["date"])
    ttf      = pd.read_csv("data/raw/ttf_prices.csv",      parse_dates=["date"])

    # --- Merge gas + TTF + carbon on date ---
    df = gas[["date", "storage_pct_full", "injection_gwh", "withdrawal_gwh", "trend_ppt"]].copy()
    df = df.merge(ttf[["date", "ttf_price_eur_mwh"]], on="date", how="left")
    df = df.merge(carbon[["date", "eua_price_eur"]],  on="date", how="left")
    df = df.sort_values("date").reset_index(drop=True)

    # Forward-fill any missing prices (weekends / holidays)
    df["ttf_price_eur_mwh"] = df["ttf_price_eur_mwh"].ffill()
    df["eua_price_eur"]     = df["eua_price_eur"].ffill()

    # --- Metric 2: Storage deviation from 5yr seasonal average ---
    # Build day-of-year average from 5yr history (excluding current year)
    gas_5yr["doy"] = gas_5yr["date"].dt.dayofyear
    current_year   = datetime.today().year
    historical     = gas_5yr[gas_5yr["date"].dt.year < current_year]
    doy_avg        = historical.groupby("doy")["storage_pct_full"].mean().rename("storage_5yr_avg")

    df["doy"] = df["date"].dt.dayofyear
    df = df.merge(doy_avg, on="doy", how="left")
    df["storage_vs_5yr_avg_ppt"] = df["storage_pct_full"] - df["storage_5yr_avg"]

    # --- Metric 3: Injection rate vs 30-day rolling average ---
    df["injection_30d_avg_gwh"]  = df["injection_gwh"].rolling(30, min_periods=5).mean()
    df["injection_vs_30d_avg_gwh"] = df["injection_gwh"] - df["injection_30d_avg_gwh"]

    # --- Metric 5: TTF 30-day momentum ---
    df["ttf_30d_momentum_pct"] = df["ttf_price_eur_mwh"].pct_change(30) * 100

    # --- Curve metrics: TTF 90d momentum + curve premium ---
    # ttf_90d_momentum_pct  : medium-term trend (compare to 30d for curve shape)
    # ttf_curve_premium_pct : how far current price sits above its 90d mean.
    #   Positive = market pricing near-term scarcity (backwardation-like signal)
    #   Negative = market relaxed, forward curve in contango
    df["ttf_90d_momentum_pct"]  = df["ttf_price_eur_mwh"].pct_change(90) * 100
    df["ttf_curve_premium_pct"] = (
        (df["ttf_price_eur_mwh"] - df["ttf_price_eur_mwh"].rolling(90, min_periods=30).mean())
        / df["ttf_price_eur_mwh"].rolling(90, min_periods=30).mean() * 100
    )

    # --- Injection seasonal average (needed for Chart 5) ---
    inj_5yr = gas_5yr.copy()
    inj_5yr["doy"] = inj_5yr["date"].dt.dayofyear
    # Map injection_gwh from gas history where available; otherwise use storage trend
    if "injection" in gas_5yr.columns:
        inj_5yr["injection_gwh"] = pd.to_numeric(inj_5yr["injection"], errors="coerce")
        inj_doy_avg = (inj_5yr[inj_5yr["date"].dt.year < current_year]
                       .groupby("doy")["injection_gwh"].mean()
                       .rename("injection_5yr_avg_gwh"))
        df = df.merge(inj_doy_avg, on="doy", how="left")

    # --- Metric 7: EUA 30-day momentum ---
    df["eua_30d_momentum_pct"] = df["eua_price_eur"].pct_change(30) * 100

    # --- Metric 8: 30-day rolling gas-carbon correlation ---
    df["gas_carbon_30d_corr"] = (
        df["ttf_price_eur_mwh"]
        .rolling(30, min_periods=10)
        .corr(df["eua_price_eur"])
    )

    # --- Power metrics (optional) ---
    power_path = os.path.join("data", "raw", "power_da.csv")
    has_power  = os.path.exists(power_path)
    if has_power:
        power = pd.read_csv(power_path, parse_dates=["date"])
        df    = df.merge(power, on="date", how="left")
        # Forward-fill all zone price columns
        zone_cols = [f"{z}_da_price_eur_mwh" for z in POWER_ZONES
                     if f"{z}_da_price_eur_mwh" in df.columns]
        for col in zone_cols:
            df[col] = df[col].ffill()
        # Clean Spark Spread (DE — most liquid EUR-denominated gas-marginal market)
        if "de_da_price_eur_mwh" in df.columns:
            df["clean_spark_spread_eur_mwh"] = (
                df["de_da_price_eur_mwh"]
                - (df["ttf_price_eur_mwh"] / GAS_EFFICIENCY)
                - (GAS_CARBON_INT * df["eua_price_eur"])
            )
            df["de_power_gas_spread_mwh"] = (
                df["de_da_price_eur_mwh"] - df["ttf_price_eur_mwh"]
            )

    # --- Final column selection (keep only columns that exist) ---
    base_cols = [
        "date",
        "storage_pct_full", "storage_vs_5yr_avg_ppt", "storage_5yr_avg",
        "injection_gwh", "injection_30d_avg_gwh", "injection_vs_30d_avg_gwh",
        "injection_5yr_avg_gwh",
        "ttf_price_eur_mwh", "ttf_30d_momentum_pct",
        "ttf_90d_momentum_pct", "ttf_curve_premium_pct",
        "eua_price_eur", "eua_30d_momentum_pct",
        "gas_carbon_30d_corr",
    ]
    power_cols = (
        [f"{z}_da_price_eur_mwh" for z in POWER_ZONES]
        + ["clean_spark_spread_eur_mwh", "de_power_gas_spread_mwh"]
    ) if has_power else []

    output_cols = [c for c in base_cols + power_cols if c in df.columns]
    result = df[output_cols].copy()

    os.makedirs("data", exist_ok=True)
    result.to_csv("data/metrics.csv", index=False)

    # --- Print today's dashboard ---
    latest = result.dropna(subset=["storage_pct_full"]).iloc[-1]
    print("\n" + "="*55)
    print(f"  COBBLESTONE ENERGY — DAILY RISK MONITOR")
    print(f"  {latest['date'].date()}")
    print("="*55)
    print(f"  1. EU Gas Storage          {latest['storage_pct_full']:.1f}% full")
    print(f"     {METRIC_NOTES['storage_pct_full']}")
    print()
    print(f"  2. vs 5yr Seasonal Avg     {latest['storage_vs_5yr_avg_ppt']:+.1f} ppt")
    print(f"     {METRIC_NOTES['storage_vs_5yr_avg_ppt']}")
    print()
    print(f"  3. Injection vs 30d Avg    {latest['injection_vs_30d_avg_gwh']:+.0f} GWh/day")
    print(f"     {METRIC_NOTES['injection_vs_30d_avg_gwh']}")
    print()
    print(f"  4. TTF M+1 Price           EUR {latest['ttf_price_eur_mwh']:.2f}/MWh")
    print(f"     {METRIC_NOTES['ttf_price_eur_mwh']}")
    print()
    print(f"  5. TTF 30d Momentum        {latest['ttf_30d_momentum_pct']:+.1f}%")
    print(f"     {METRIC_NOTES['ttf_30d_momentum_pct']}")
    print()
    print(f"  6. EUA Carbon Price        EUR {latest['eua_price_eur']:.2f}/tonne")
    print(f"     {METRIC_NOTES['eua_price_eur']}")
    print()
    print(f"  7. EUA 30d Momentum        {latest['eua_30d_momentum_pct']:+.1f}%")
    print(f"     {METRIC_NOTES['eua_30d_momentum_pct']}")
    print()
    print(f"  8. Gas-Carbon 30d Corr     {latest['gas_carbon_30d_corr']:.2f}")
    print(f"     {METRIC_NOTES['gas_carbon_30d_corr']}")
    if has_power:
        print()
        zone_labels = {"de": "DE (Germany)", "fr": "FR (France)",
                       "gb": "GB (Gt Britain)", "nl": "NL (Netherlands)",
                       "be": "BE (Belgium)",   "es": "ES (Spain)"}
        n = 9
        for z in POWER_ZONES:
            col = f"{z}_da_price_eur_mwh"
            if col in latest and pd.notna(latest[col]):
                currency = "GBP" if z == "gb" else "EUR"
                print(f"  {n:>2}. DA Power {zone_labels[z]:<18} {currency} {latest[col]:.2f}/MWh")
                n += 1
        if "clean_spark_spread_eur_mwh" in latest and pd.notna(latest["clean_spark_spread_eur_mwh"]):
            css = latest["clean_spark_spread_eur_mwh"]
            print(f"  {n:>2}. Clean Spark Spread (DE)       EUR {css:.2f}/MWh")
            n += 1
        if "de_power_gas_spread_mwh" in latest and pd.notna(latest["de_power_gas_spread_mwh"]):
            print(f"  {n:>2}. DE Power-Gas Spread            EUR {latest['de_power_gas_spread_mwh']:.2f}/MWh")
    print("="*55)
    print(f"  Saved -> data/metrics.csv ({len(result)} rows)\n")

    return result


if __name__ == "__main__":
    compute_metrics()
