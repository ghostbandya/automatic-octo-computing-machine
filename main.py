"""
main.py
-------
Orchestrator — runs the full Cobblestone Energy daily brief pipeline end-to-end.

Usage:
    python main.py

Steps:
    1. Ingest gas storage data   (GIE AGSI+)
    2. Ingest carbon EUA prices  (Yahoo Finance / CO2.L)
    3. Ingest TTF gas prices     (Yahoo Finance / TTF=F)
    4. Compute monitor metrics
    5. Generate charts
    6. Generate LLM narrative
    7. Assemble and save daily brief → output/daily_brief_YYYY-MM-DD.md
"""

import os
import sys
import traceback
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
TODAY = datetime.today().strftime("%Y-%m-%d")
TODAY_DISPLAY = datetime.today().strftime("%d %B %Y")


def run():
    print("\n" + "="*60)
    print("  COBBLESTONE ENERGY — DAILY BRIEF PIPELINE")
    print(f"  {TODAY_DISPLAY}")
    print("="*60 + "\n")

    # ── Step 1: Gas storage ──────────────────────────────────────────
    print("── Step 1/6: Gas storage data ──")
    from src.gas_data import fetch_gas_storage
    gas = fetch_gas_storage(days_back=90)

    # ── Step 2: Carbon EUA ───────────────────────────────────────────
    print("\n── Step 2/6: Carbon EUA prices ──")
    from src.carbon_data import fetch_eua_prices
    carbon = fetch_eua_prices(days_back=90)

    # ── Step 3: TTF gas price ────────────────────────────────────────
    print("\n── Step 3/6: TTF gas prices ──")
    end   = datetime.today()
    start = end - timedelta(days=90)
    ttf_raw = yf.Ticker("TTF=F").history(
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d")
    )
    ttf = ttf_raw.reset_index()[["Date","Close"]].rename(
        columns={"Date":"date","Close":"ttf_price_eur_mwh"}
    )
    ttf["date"] = pd.to_datetime(ttf["date"]).dt.tz_localize(None)
    ttf.sort_values("date", inplace=True)
    os.makedirs("data/raw", exist_ok=True)
    ttf.to_csv("data/raw/ttf_prices.csv", index=False)
    print(f"[main] ✓ TTF: {len(ttf)} rows | latest €{ttf['ttf_price_eur_mwh'].iloc[-1]:.2f}/MWh")

    # ── Step 4: Metrics ──────────────────────────────────────────────
    print("\n── Step 4/6: Computing metrics ──")
    from src.metrics import compute_metrics
    metrics_df = compute_metrics()

    # ── Step 5: Charts ───────────────────────────────────────────────
    print("\n── Step 5/6: Generating charts ──")
    from src.charts import chart1_gas_storage, chart2_eua_vs_ttf
    chart1_path = chart1_gas_storage()
    chart2_path = chart2_eua_vs_ttf()

    # ── Step 6: LLM narrative ─────────────────────────────────────────
    print("\n── Step 6/6: LLM narrative ──")
    from src.llm_brief import generate_narrative
    narrative = generate_narrative()

    # ── Step 7: Assemble brief ────────────────────────────────────────
    print("\n── Assembling daily brief ──")
    latest = metrics_df.dropna(subset=["storage_pct_full", "eua_price_eur"]).iloc[-1]

    brief = f"""# Cobblestone Energy — Daily Risk Monitor
**Date:** {TODAY_DISPLAY}
**Theme:** European Cross-Commodity Risk: Gas + Carbon → Power Curve

---

## Trading Narrative

{narrative}

---

## Monitor Metrics — {TODAY_DISPLAY}

| # | Metric | Value | Signal |
|---|--------|-------|--------|
| 1 | EU Gas Storage | **{latest['storage_pct_full']:.1f}%** full | {"🔴 Below avg" if latest['storage_vs_5yr_avg_ppt'] < -5 else "🟡 Near avg"} |
| 2 | vs 5yr Seasonal Avg | **{latest['storage_vs_5yr_avg_ppt']:+.1f} ppt** | {"🔴 Tight" if latest['storage_vs_5yr_avg_ppt'] < -5 else "🟢 Normal"} |
| 3 | Injection vs 30d Avg | **{latest['injection_vs_30d_avg_gwh']:+.0f} GWh/day** | {"🟢 Above avg" if latest['injection_vs_30d_avg_gwh'] > 0 else "🔴 Below avg"} |
| 4 | TTF M+1 Price | **€{latest['ttf_price_eur_mwh']:.2f}/MWh** | {"🔴 Rising" if latest['ttf_30d_momentum_pct'] > 5 else "🟡 Easing" if latest['ttf_30d_momentum_pct'] < -5 else "🟡 Stable"} |
| 5 | TTF 30d Momentum | **{latest['ttf_30d_momentum_pct']:+.1f}%** | {"🔴 Bullish" if latest['ttf_30d_momentum_pct'] > 5 else "🟢 Bearish" if latest['ttf_30d_momentum_pct'] < -5 else "🟡 Neutral"} |
| 6 | EUA Carbon Price | **€{latest['eua_price_eur']:.2f}/tonne** | {"🔴 Rising" if latest['eua_30d_momentum_pct'] > 5 else "🟢 Falling" if latest['eua_30d_momentum_pct'] < -5 else "🟡 Stable"} |
| 7 | EUA 30d Momentum | **{latest['eua_30d_momentum_pct']:+.1f}%** | {"🔴 Bullish" if latest['eua_30d_momentum_pct'] > 5 else "🟢 Bearish" if latest['eua_30d_momentum_pct'] < -5 else "🟡 Neutral"} |
| 8 | Gas–Carbon 30d Corr | **{latest['gas_carbon_30d_corr']:.2f}** | {"🔴 Strong" if abs(latest['gas_carbon_30d_corr']) > 0.6 else "🟡 Moderate" if abs(latest['gas_carbon_30d_corr']) > 0.3 else "🟢 Weak"} |

---

## Charts

### Chart 1 — EU Gas Storage vs 5yr Seasonal Average
![Gas Storage](output/charts/chart1_gas_storage.png)

### Chart 2 — EUA Carbon vs TTF Gas Price (90 Days)
![EUA vs TTF](output/charts/chart2_eua_vs_ttf.png)

---

*Generated automatically by the Cobblestone Energy Cross-Commodity Monitor.*
*Data: GIE AGSI+, Yahoo Finance (CO2.L, TTF=F) | LLM: Logged in output/logs/{TODAY}_log.json*
"""

    out_path = os.path.join("output", f"daily_brief_{TODAY}.md")
    os.makedirs("output", exist_ok=True)
    with open(out_path, "w") as f:
        f.write(brief)

    print(f"\n{'='*60}")
    print(f"  ✓ PIPELINE COMPLETE")
    print(f"  Brief saved → {out_path}")
    print(f"  Charts      → output/charts/")
    print(f"  LLM log     → output/logs/{TODAY}_log.json")
    print(f"{'='*60}\n")

    return out_path


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {e}")
        traceback.print_exc()
        sys.exit(1)
