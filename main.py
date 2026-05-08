"""
main.py - Cobblestone Energy daily brief pipeline orchestrator.
Usage: python main.py
"""
import os, sys, traceback, pandas as pd, yfinance as yf
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
TODAY         = datetime.today().strftime("%Y-%m-%d")
TODAY_DISPLAY = datetime.today().strftime("%d %B %Y")


def run():
    print("\n" + "="*60)
    print(f"  COBBLESTONE ENERGY -- DAILY BRIEF PIPELINE  {TODAY_DISPLAY}")
    print("="*60 + "\n")

    # Step 1: Gas storage
    print("-- Step 1/7: Gas storage --")
    from src.gas_data import fetch_gas_storage, fetch_gas_storage_5yr
    fetch_gas_storage(days_back=180)

    # Refresh 5yr history if missing or more than 7 days stale
    _5yr_path = "data/raw/gas_storage_5yr.csv"
    _5yr_stale = True
    if os.path.exists(_5yr_path):
        _5yr_mtime = datetime.fromtimestamp(os.path.getmtime(_5yr_path))
        _5yr_stale = (datetime.today() - _5yr_mtime).days > 7
    if _5yr_stale:
        print("[main] 5yr storage history missing or >7 days old — refreshing ...")
        fetch_gas_storage_5yr()
    else:
        print("[main] 5yr storage history is fresh — skipping re-fetch")

    # Step 2: Carbon EUA
    print("\n-- Step 2/7: Carbon EUA prices --")
    from src.carbon_data import fetch_eua_prices
    fetch_eua_prices(days_back=180)

    # Step 3: TTF gas
    print("\n-- Step 3/7: TTF gas prices --")
    end   = datetime.today() + timedelta(days=1)   # yfinance end is exclusive — +1 day captures today
    start = end - timedelta(days=182)
    ttf_raw = yf.Ticker("TTF=F").history(start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"))
    ttf = ttf_raw.reset_index()[["Date","Close"]].rename(columns={"Date":"date","Close":"ttf_price_eur_mwh"})
    ttf["date"] = pd.to_datetime(ttf["date"]).dt.tz_localize(None)
    ttf.sort_values("date", inplace=True)
    os.makedirs("data/raw", exist_ok=True)
    ttf.to_csv("data/raw/ttf_prices.csv", index=False)
    print(f"[main] TTF: {len(ttf)} rows | latest EUR {ttf['ttf_price_eur_mwh'].iloc[-1]:.2f}/MWh")

    # Step 4: Power DA (ENTSO-E)
    print("\n-- Step 4/7: Day-Ahead power prices (ENTSO-E) --")
    token = os.getenv("ENTSO_E_TOKEN", "")
    if token and token != "pending":
        from src.power_data import fetch_day_ahead_prices
        fetch_day_ahead_prices(days_back=180)
    else:
        print("[main] ENTSO_E_TOKEN not set -- skipping power data")

    # Data freshness report
    print("\n-- Data Freshness Check --")
    _today = datetime.today().date()
    _sources = {
        "Gas Storage":     ("data/raw/gas_storage.csv",    "date"),
        "Carbon EUA":      ("data/raw/carbon_eua.csv",     "date"),
        "TTF Prices":      ("data/raw/ttf_prices.csv",     "date"),
        "Power DA":        ("data/raw/power_da.csv",       "date"),
    }
    for label, (path, dcol) in _sources.items():
        if os.path.exists(path):
            try:
                _df = pd.read_csv(path, parse_dates=[dcol])
                _latest = _df[dcol].max().date()
                _lag = (_today - _latest).days
                _flag = "✓" if _lag <= 1 else f"⚠  {_lag}d lag"
                print(f"  {label:<18} latest = {_latest}   {_flag}")
            except Exception:
                print(f"  {label:<18} [could not read]")
        else:
            print(f"  {label:<18} [file not found — skipped]")
    print()

    # Step 5: Metrics
    print("\n-- Step 5/7: Computing metrics --")
    from src.metrics import compute_metrics
    metrics_df = compute_metrics()

    # Step 6: Charts
    print("\n-- Step 6/7: Generating charts --")
    from src.charts import (chart1_gas_storage, chart2_eua_vs_ttf,
                             chart3_power_and_spark, chart4_ttf_curve_signal,
                             chart5_injection_pace, chart6_power_spreads)
    chart1_gas_storage()
    chart2_eua_vs_ttf()
    chart3_power_and_spark()
    chart4_ttf_curve_signal()
    chart5_injection_pace()
    chart6_power_spreads()

    # Step 7: Narrative
    print("\n-- Step 7/7: Generating narrative --")
    from src.llm_brief import generate_narrative
    narrative = generate_narrative()

    # Assemble brief
    print("\n-- Assembling daily brief --")
    latest    = metrics_df.dropna(subset=["storage_pct_full","eua_price_eur"]).iloc[-1]
    has_power = ("de_da_price_eur_mwh" in metrics_df.columns and
                 pd.notna(latest.get("de_da_price_eur_mwh")))

    power_rows    = ""
    chart3_section = ""
    if has_power:
        # Build power rows dynamically for all zones present in metrics
        # GB excluded: post-Brexit data no longer on ENTSO-E
        ZONE_LABELS = {
            "de": "DE Day-Ahead Power",
            "fr": "FR Day-Ahead Power",
            "nl": "NL Day-Ahead Power",
            "be": "BE Day-Ahead Power",
            "es": "ES Day-Ahead Power",
        }
        rows = []
        n = 9
        for zone, label in ZONE_LABELS.items():
            col = f"{zone}_da_price_eur_mwh"
            if col in metrics_df.columns and pd.notna(latest.get(col)):
                currency = "GBP" if zone == "gb" else "EUR"
                signal = "HIGH" if latest[col] > 100 else "LOW"
                rows.append(f"| {n:<2} | {label:<20} | {currency} {latest[col]:.2f}/MWh | {signal} |")
                n += 1
        css = latest.get("clean_spark_spread_eur_mwh", float("nan"))
        if pd.notna(css):
            rows.append(f"| {n:<2} | Clean Spark Spread   | EUR {css:.2f}/MWh | {'POSITIVE' if css > 0 else 'NEGATIVE'} |")
            n += 1
        dgs = latest.get("de_power_gas_spread_mwh", float("nan"))
        if pd.notna(dgs):
            rows.append(f"| {n:<2} | DE Power-Gas Spread  | EUR {dgs:.2f}/MWh | |")
        power_rows = "\n".join(rows)
        chart3_section = "\n### Chart 3 -- Power Prices & Clean Spark Spread\n![Power](output/charts/chart3_power_spark.png)\n"

    lines = [
        f"# Cobblestone Energy -- Daily Risk Monitor",
        f"**Date:** {TODAY_DISPLAY}  |  **Theme:** Gas + Carbon -> Power Curve",
        "",
        "---",
        "",
        "## Trading Narrative",
        "",
        narrative,
        "",
        "---",
        "",
        f"## Monitor Metrics -- {TODAY_DISPLAY}",
        "",
        "| # | Metric | Value | Signal |",
        "|---|--------|-------|--------|",
        f"| 1 | EU Gas Storage        | {latest['storage_pct_full']:.1f}% full           | {'BELOW AVG' if latest['storage_vs_5yr_avg_ppt'] < -5 else 'NEAR AVG'} |",
        f"| 2 | vs 5yr Seasonal Avg   | {latest['storage_vs_5yr_avg_ppt']:+.1f} ppt              | {'TIGHT' if latest['storage_vs_5yr_avg_ppt'] < -5 else 'NORMAL'} |",
        f"| 3 | Injection vs 30d Avg  | {latest['injection_vs_30d_avg_gwh']:+.0f} GWh/day        | {'ABOVE AVG' if latest['injection_vs_30d_avg_gwh'] > 0 else 'BELOW AVG'} |",
        f"| 4 | TTF M+1               | EUR {latest['ttf_price_eur_mwh']:.2f}/MWh          | {'RISING' if latest['ttf_30d_momentum_pct'] > 5 else 'EASING' if latest['ttf_30d_momentum_pct'] < -5 else 'STABLE'} |",
        f"| 5 | TTF 30d Momentum      | {latest['ttf_30d_momentum_pct']:+.1f}%                   | {'BULLISH' if latest['ttf_30d_momentum_pct'] > 5 else 'BEARISH' if latest['ttf_30d_momentum_pct'] < -5 else 'NEUTRAL'} |",
        f"| 5b| TTF 90d Momentum      | {latest.get('ttf_90d_momentum_pct', float('nan')):+.1f}%                   | {'BULLISH' if pd.notna(latest.get('ttf_90d_momentum_pct')) and latest['ttf_90d_momentum_pct'] > 5 else 'BEARISH' if pd.notna(latest.get('ttf_90d_momentum_pct')) and latest['ttf_90d_momentum_pct'] < -5 else 'NEUTRAL'} |" if pd.notna(latest.get('ttf_90d_momentum_pct')) else "",
        f"| 5c| TTF Curve Premium     | {latest.get('ttf_curve_premium_pct', float('nan')):+.1f}%                   | {'BACKWARDATION' if pd.notna(latest.get('ttf_curve_premium_pct')) and latest['ttf_curve_premium_pct'] > 2 else 'CONTANGO' if pd.notna(latest.get('ttf_curve_premium_pct')) and latest['ttf_curve_premium_pct'] < -2 else 'FLAT'} |" if pd.notna(latest.get('ttf_curve_premium_pct')) else "",
        f"| 6 | EUA Carbon            | EUR {latest['eua_price_eur']:.2f}/tonne         | {'RISING' if latest['eua_30d_momentum_pct'] > 5 else 'FALLING' if latest['eua_30d_momentum_pct'] < -5 else 'STABLE'} |",
        f"| 7 | EUA 30d Momentum      | {latest['eua_30d_momentum_pct']:+.1f}%                   | {'BULLISH' if latest['eua_30d_momentum_pct'] > 5 else 'BEARISH' if latest['eua_30d_momentum_pct'] < -5 else 'NEUTRAL'} |",
        f"| 8 | Gas-Carbon 30d Corr   | {latest['gas_carbon_30d_corr']:.2f}                  | {'STRONG' if abs(latest['gas_carbon_30d_corr']) > 0.6 else 'MODERATE' if abs(latest['gas_carbon_30d_corr']) > 0.3 else 'WEAK'} |",
        power_rows.rstrip(),
        "",
        "---",
        "",
        "## Charts",
        "",
        "### Chart 1 -- EU Gas Storage vs 5yr Seasonal Average",
        "![Gas Storage](output/charts/chart1_gas_storage.png)",
        "",
        "### Chart 2 -- EUA Carbon vs TTF Gas (90 Days)",
        "![EUA vs TTF](output/charts/chart2_eua_vs_ttf.png)",
        "",
        chart3_section,
        "### Chart 4 -- TTF Curve Signal: 30d vs 90d MA (Backwardation / Contango)",
        "![TTF Curve](output/charts/chart4_ttf_curve.png)",
        "",
        "### Chart 5 -- EU Gas Injection Pace vs Seasonal Average",
        "![Injection Pace](output/charts/chart5_injection_pace.png)",
        "",
        "### Chart 6 -- Cross-Zone Power Spreads (vs DE Day-Ahead)",
        "![Power Spreads](output/charts/chart6_power_spreads.png)",
        "",
        "---",
        "",
        f"*Auto-generated | Data: GIE AGSI+, Yahoo Finance, ENTSO-E | Log: output/logs/{TODAY}_log.json*",
    ]
    brief = "\n".join(lines)

    out_path = os.path.join("output", f"daily_brief_{TODAY}.md")
    os.makedirs("output", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(brief)

    print(f"\n" + "="*60)
    print(f"  PIPELINE COMPLETE")
    print(f"  Brief  -> {out_path}")
    print(f"  Charts -> output/charts/")
    print(f"  Log    -> output/logs/{TODAY}_log.json")
    print("="*60 + "\n")
    return out_path


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"\n[ERROR] {e}")
        traceback.print_exc()
        sys.exit(1)
