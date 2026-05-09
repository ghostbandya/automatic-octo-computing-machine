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
  5b. TTF 90-day price momentum (%)          — medium-term trend context
  5c. TTF curve premium vs 90d mean (%)      — backwardation / contango proxy
  6. EUA spot price proxy (EUR/tonne)
  7. EUA 30-day price momentum (%)
  8. Gas-Carbon 30-day rolling correlation

Power metrics (computed when data/raw/power_da.csv exists):
  9+. Day-Ahead price per zone: DE, FR, NL, BE, ES
  *   Clean Spark Spread DE (EUR/MWh)  — CSS = DA_DE - TTF/0.49 - 0.202*EUA
  *   DE Power-Gas Spread (EUR/MWh)

Install: pip install pandas numpy
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime


# ── Metric explanations (embedded in the daily brief print output) ──────────
# Each string explains the trading implication of its metric — what direction
# is bullish/bearish and why it matters for the cross-commodity risk pack.
METRIC_NOTES = {
    "storage_pct_full":
        "Core gas tightness signal — below 35% in May is historically low, "
        "supportive of power prices.",
    "storage_vs_5yr_avg_ppt":
        "Deviation from seasonal norm — negative value means below 5yr average, "
        "bullish for gas/power.",
    "injection_vs_30d_avg_gwh":
        "Pace of storage build — slow injection in summer tightens winter supply outlook.",
    "ttf_price_eur_mwh":
        "European gas benchmark — direct cost input to gas-fired power generation.",
    "ttf_30d_momentum_pct":
        "Gas price trend — rising momentum signals tightening supply or rising demand.",
    "eua_price_eur":
        "EU carbon price — higher EUA raises marginal cost of coal generation, "
        "supports gas-to-power.",
    "eua_30d_momentum_pct":
        "Carbon price trend — accelerating EUA drives fuel switching from coal to gas.",
    "gas_carbon_30d_corr":
        "Cross-commodity linkage — high correlation confirms carbon is amplifying "
        "gas price moves.",
}

# ── CCGT / generation constants ──────────────────────────────────────────────
# These are standard industry parameters for a Combined Cycle Gas Turbine plant.
# Used in the Clean Spark Spread formula: CSS = DA_power - (TTF/η) - (CI × EUA)
GAS_EFFICIENCY = 0.49    # η: CCGT thermal efficiency (49% — modern plant benchmark)
GAS_CARBON_INT = 0.202   # CI: tCO2 emitted per MWh of gas burned (IPCC default)

# DA price zones to process — column names follow {code}_da_price_eur_mwh convention.
# GB excluded: post-Brexit, N2EX market no longer publishes on ENTSO-E.
POWER_ZONES = ["de", "fr", "nl", "be", "es"]


def compute_metrics() -> pd.DataFrame:
    """
    Loads raw CSVs, computes all metrics, and saves to data/metrics.csv.
    Power metrics are included automatically when data/raw/power_da.csv exists.

    Design principle: all metrics are computed as new columns on a single merged
    DataFrame, allowing easy addition of future metrics without restructuring.
    The final column selection ensures only well-defined columns are saved.

    Returns
    -------
    pd.DataFrame — one row per date with all metric columns
    """

    # ── Load raw data ────────────────────────────────────────────────────────
    gas     = pd.read_csv("data/raw/gas_storage.csv",     parse_dates=["date"])
    gas_5yr = pd.read_csv("data/raw/gas_storage_5yr.csv", parse_dates=["date"])
    carbon  = pd.read_csv("data/raw/carbon_eua.csv",      parse_dates=["date"])
    ttf     = pd.read_csv("data/raw/ttf_prices.csv",      parse_dates=["date"])

    # ── Merge gas + TTF + carbon on date ────────────────────────────────────
    # Start from gas storage as the base (it drives the seasonal context),
    # then left-join prices. Left join preserves all gas storage dates even
    # if price data is missing for weekends/holidays.
    df = gas[["date", "storage_pct_full", "injection_gwh",
              "withdrawal_gwh", "trend_ppt"]].copy()
    df = df.merge(ttf[["date", "ttf_price_eur_mwh"]], on="date", how="left")
    df = df.merge(carbon[["date", "eua_price_eur"]],  on="date", how="left")
    df = df.sort_values("date").reset_index(drop=True)

    # Forward-fill prices across weekends and public holidays — gas storage
    # data is published 7 days/week, but TTF and EUA only trade on business days.
    df["ttf_price_eur_mwh"] = df["ttf_price_eur_mwh"].ffill()
    df["eua_price_eur"]     = df["eua_price_eur"].ffill()

    # ── Metric 2: Storage deviation from 5yr seasonal average ───────────────
    # Compute a day-of-year (DOY) average from historical data, excluding the
    # current year so we're comparing against true historical norms.
    gas_5yr["doy"]  = gas_5yr["date"].dt.dayofyear
    current_year    = datetime.today().year
    historical      = gas_5yr[gas_5yr["date"].dt.year < current_year]
    doy_avg         = (historical
                       .groupby("doy")["storage_pct_full"]
                       .mean()
                       .rename("storage_5yr_avg"))

    df["doy"] = df["date"].dt.dayofyear
    df = df.merge(doy_avg, on="doy", how="left")
    # Positive ppt = above average (bearish gas), negative ppt = below (bullish)
    df["storage_vs_5yr_avg_ppt"] = df["storage_pct_full"] - df["storage_5yr_avg"]

    # ── Metric 3: Injection rate vs 30-day rolling average ──────────────────
    # Measures whether the current injection pace is accelerating or slowing
    # relative to the recent trend. A negative value during injection season
    # (Apr–Oct) signals lagging storage build — bearish supply outlook.
    df["injection_30d_avg_gwh"]    = df["injection_gwh"].rolling(30, min_periods=5).mean()
    df["injection_vs_30d_avg_gwh"] = df["injection_gwh"] - df["injection_30d_avg_gwh"]

    # ── Metric 5: TTF 30-day momentum ───────────────────────────────────────
    # Standard price momentum: % change over the last 30 trading days.
    # >+5% = bullish trend; <-5% = bearish/easing.
    df["ttf_30d_momentum_pct"] = df["ttf_price_eur_mwh"].pct_change(30) * 100

    # ── Metric 5b: TTF 90-day momentum ──────────────────────────────────────
    # Extends the momentum view to a medium-term window. Comparing 30d vs 90d
    # momentum reveals curve shape: if 30d > 90d, the near end is accelerating
    # faster than the medium-term trend (backwardation signal).
    # Requires 180 days of history to produce non-NaN values — hence days_back=180.
    df["ttf_90d_momentum_pct"] = df["ttf_price_eur_mwh"].pct_change(90) * 100

    # ── Metric 5c: TTF curve premium vs 90d mean ────────────────────────────
    # How far the current spot price sits above or below its 90-day mean.
    # Positive = market pricing near-term scarcity (backwardation-like).
    # Negative = market relaxed on prompt supply, forward curve in contango.
    # This is a proxy for the forward curve shape using only spot price history
    # (no forward curve data required).
    df["ttf_curve_premium_pct"] = (
        (df["ttf_price_eur_mwh"] - df["ttf_price_eur_mwh"].rolling(90, min_periods=30).mean())
        / df["ttf_price_eur_mwh"].rolling(90, min_periods=30).mean() * 100
    )

    # ── Seasonal injection average (for Chart 5 cumulative deficit) ─────────
    # Build a DOY average of daily injection volumes from the 5yr history.
    # This allows Chart 5 to show cumulative injection deficit vs seasonal norm.
    inj_5yr = gas_5yr.copy()
    inj_5yr["doy"] = inj_5yr["date"].dt.dayofyear
    if "injection" in gas_5yr.columns:
        inj_5yr["injection_gwh"] = pd.to_numeric(inj_5yr["injection"], errors="coerce")
        inj_doy_avg = (
            inj_5yr[inj_5yr["date"].dt.year < current_year]
            .groupby("doy")["injection_gwh"]
            .mean()
            .rename("injection_5yr_avg_gwh")
        )
        df = df.merge(inj_doy_avg, on="doy", how="left")

    # ── Metric 7: EUA 30-day momentum ───────────────────────────────────────
    # Same momentum logic as TTF. A rising EUA trend widens the coal-gas spread,
    # supporting gas demand and power prices.
    df["eua_30d_momentum_pct"] = df["eua_price_eur"].pct_change(30) * 100

    # ── Metric 8: 30-day rolling gas-carbon correlation ──────────────────────
    # A high positive correlation means carbon is amplifying gas price moves —
    # both commodities pulling the power curve in the same direction.
    # A negative correlation means divergence: e.g. gas softens while carbon
    # firms, which compresses clean dark spreads and supports gas-fired margins.
    df["gas_carbon_30d_corr"] = (
        df["ttf_price_eur_mwh"]
        .rolling(30, min_periods=10)
        .corr(df["eua_price_eur"])
    )

    # ── Power metrics (optional — only if power_da.csv exists) ───────────────
    power_path = os.path.join("data", "raw", "power_da.csv")
    has_power  = os.path.exists(power_path)
    if has_power:
        power = pd.read_csv(power_path, parse_dates=["date"])
        df    = df.merge(power, on="date", how="left")

        # Forward-fill power prices across weekends/holidays (same logic as TTF)
        zone_cols = [f"{z}_da_price_eur_mwh" for z in POWER_ZONES
                     if f"{z}_da_price_eur_mwh" in df.columns]
        for col in zone_cols:
            df[col] = df[col].ffill()

        # Clean Spark Spread (CSS) — DE is the most liquid EUR-denominated
        # gas-marginal market, so it's the standard reference for European CSS.
        # CSS > 0 = gas-fired generation is profitable at current input costs.
        if "de_da_price_eur_mwh" in df.columns:
            df["clean_spark_spread_eur_mwh"] = (
                df["de_da_price_eur_mwh"]
                - (df["ttf_price_eur_mwh"] / GAS_EFFICIENCY)
                - (GAS_CARBON_INT * df["eua_price_eur"])
            )
            # Simple DE power-gas spread (without carbon adjustment) —
            # useful for watching raw market integration between gas and power
            df["de_power_gas_spread_mwh"] = (
                df["de_da_price_eur_mwh"] - df["ttf_price_eur_mwh"]
            )

    # ── Final column selection ───────────────────────────────────────────────
    # Only keep columns that actually exist (power columns may be absent).
    # Ordering follows the monitor metric numbering for readability.
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

    # ── Print today's dashboard ──────────────────────────────────────────────
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
        zone_labels = {
            "de": "DE (Germany)",     "fr": "FR (France)",
            "nl": "NL (Netherlands)", "be": "BE (Belgium)",
            "es": "ES (Spain)",
        }
        n = 9
        for z in POWER_ZONES:
            col = f"{z}_da_price_eur_mwh"
            if col in latest and pd.notna(latest[col]):
                print(f"  {n:>2}. DA Power {zone_labels[z]:<18} EUR {latest[col]:.2f}/MWh")
                n += 1
        if "clean_spark_spread_eur_mwh" in latest and pd.notna(latest["clean_spark_spread_eur_mwh"]):
            css = latest["clean_spark_spread_eur_mwh"]
            profitable = "profitable" if css > 0 else "loss-making"
            print(f"  {n:>2}. Clean Spark Spread (DE)    EUR {css:.2f}/MWh  [{profitable}]")
            n += 1
        if "de_power_gas_spread_mwh" in latest and pd.notna(latest["de_power_gas_spread_mwh"]):
            print(f"  {n:>2}. DE Power-Gas Spread         EUR {latest['de_power_gas_spread_mwh']:.2f}/MWh")

    print("="*55)
    print(f"  Saved -> data/metrics.csv ({len(result)} rows)\n")

    return result


if __name__ == "__main__":
    compute_metrics()
