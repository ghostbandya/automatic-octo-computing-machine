"""
charts.py
---------
Generates two publication-quality charts for the daily brief.

Chart 1: EU Gas Storage % Full vs 5yr Seasonal Average (area + band)
Chart 2: EUA Carbon Price vs TTF Gas Price — dual axis, 90 days

Outputs: output/charts/chart1_gas_storage.png
         output/charts/chart2_eua_vs_ttf.png

Install: pip install matplotlib pandas
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.dates as mdates
from datetime import datetime

# ── Style ──────────────────────────────────────────────────────────────────
COBBLESTONE_BLUE  = "#1A3557"
COBBLESTONE_RED   = "#C0392B"
COBBLESTONE_AMBER = "#E67E22"
BAND_COLOR        = "#AED6F1"
GRID_COLOR        = "#E8E8E8"
FONT_FAMILY       = "DejaVu Sans"

plt.rcParams.update({
    "font.family":        FONT_FAMILY,
    "axes.spines.top":    False,
    "axes.spines.right":  False,
    "axes.grid":          True,
    "grid.color":         GRID_COLOR,
    "grid.linewidth":     0.7,
    "figure.dpi":         150,
})

TODAY = datetime.today().strftime("%d %b %Y")
SOURCE_GIE    = "Source: GIE AGSI+ | Cobblestone Energy Research"
SOURCE_MARKET = "Source: Yahoo Finance (CO2.L, TTF=F) | Cobblestone Energy Research"


def chart1_gas_storage() -> str:
    """
    Chart 1 — EU Gas Storage % Full vs 5yr Seasonal Average.
    Shows current fill level against the historical range (±1 std) and mean.
    """
    gas_5yr = pd.read_csv("data/raw/gas_storage_5yr.csv", parse_dates=["date"])
    metrics  = pd.read_csv("data/metrics.csv", parse_dates=["date"])

    # Build 5yr seasonal band: mean ± 1 std per day-of-year
    gas_5yr["doy"] = gas_5yr["date"].dt.dayofyear
    current_year   = datetime.today().year
    hist = gas_5yr[gas_5yr["date"].dt.year < current_year]
    band = hist.groupby("doy")["storage_pct_full"].agg(["mean", "std"]).reset_index()
    band["upper"] = band["mean"] + band["std"]
    band["lower"] = band["mean"] - band["std"]

    # Current year data
    current = gas_5yr[gas_5yr["date"].dt.year == current_year].copy()
    current["doy"] = current["date"].dt.dayofyear

    # Merge band onto current dates
    plot_df = current.merge(band[["doy","mean","upper","lower"]], on="doy", how="left")

    fig, ax = plt.subplots(figsize=(11, 5.5))

    # Shaded 5yr range
    ax.fill_between(plot_df["date"], plot_df["lower"], plot_df["upper"],
                    color=BAND_COLOR, alpha=0.45, label="5yr ±1 std range")

    # 5yr mean line
    ax.plot(plot_df["date"], plot_df["mean"],
            color=COBBLESTONE_BLUE, linewidth=1.2, linestyle="--",
            alpha=0.7, label="5yr seasonal average")

    # Current storage line
    ax.plot(plot_df["date"], plot_df["storage_pct_full"],
            color=COBBLESTONE_RED, linewidth=2.2, label="EU storage 2026")

    # Latest value annotation
    latest = plot_df.dropna(subset=["storage_pct_full"]).iloc[-1]
    avg_latest = latest["mean"]
    deviation  = latest["storage_pct_full"] - avg_latest
    ax.annotate(
        f" {latest['storage_pct_full']:.1f}%\n ({deviation:+.1f} ppt vs avg)",
        xy=(latest["date"], latest["storage_pct_full"]),
        fontsize=8.5, color=COBBLESTONE_RED, fontweight="bold",
        va="center",
    )

    # Formatting
    ax.set_title("EU Gas Storage: Current Fill vs 5-Year Seasonal Average",
                 fontsize=13, fontweight="bold", color=COBBLESTONE_BLUE, pad=12)
    ax.set_ylabel("Storage Fill Level (%)", fontsize=10)
    ax.set_xlabel("")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.xticks(rotation=30, ha="right", fontsize=8.5)
    ax.set_ylim(0, 105)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.legend(loc="upper left", fontsize=8.5, framealpha=0.9)
    fig.text(0.01, -0.02, SOURCE_GIE, fontsize=7, color="grey", ha="left")
    fig.text(0.99, -0.02, TODAY, fontsize=7, color="grey", ha="right")

    plt.tight_layout()
    out = os.path.join("output", "charts", "chart1_gas_storage.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"[charts] ✓ Chart 1 saved → {out}")
    return out


def chart2_eua_vs_ttf() -> str:
    """
    Chart 2 — EUA Carbon Price vs TTF Gas Price (dual axis, 90 days).
    Highlights the cross-commodity relationship and recent divergence.
    """
    carbon  = pd.read_csv("data/raw/carbon_eua.csv",  parse_dates=["date"])
    ttf     = pd.read_csv("data/raw/ttf_prices.csv",  parse_dates=["date"])
    metrics = pd.read_csv("data/metrics.csv",          parse_dates=["date"])

    # Align on common dates
    merged = ttf.merge(carbon, on="date", how="inner").sort_values("date")

    # Pull correlation from metrics for annotation
    latest_corr = metrics.dropna(subset=["gas_carbon_30d_corr"])["gas_carbon_30d_corr"].iloc[-1]

    fig, ax1 = plt.subplots(figsize=(11, 5.5))
    ax2 = ax1.twinx()

    # TTF on left axis
    ax1.plot(merged["date"], merged["ttf_price_eur_mwh"],
             color=COBBLESTONE_BLUE, linewidth=2.2, label="TTF Gas (€/MWh)")
    ax1.set_ylabel("TTF Gas Price (€/MWh)", fontsize=10, color=COBBLESTONE_BLUE)
    ax1.tick_params(axis="y", labelcolor=COBBLESTONE_BLUE)

    # EUA on right axis
    ax2.plot(merged["date"], merged["eua_price_eur"],
             color=COBBLESTONE_AMBER, linewidth=2.2, linestyle="-", label="EUA Carbon (€/tonne)")
    ax2.set_ylabel("EUA Carbon Price (€/tonne)", fontsize=10, color=COBBLESTONE_AMBER)
    ax2.tick_params(axis="y", labelcolor=COBBLESTONE_AMBER)

    # Shade area under EUA lightly
    ax2.fill_between(merged["date"], merged["eua_price_eur"],
                     alpha=0.08, color=COBBLESTONE_AMBER)

    # Correlation annotation box
    ax1.annotate(
        f"30d Correlation: {latest_corr:.2f}",
        xy=(0.02, 0.07), xycoords="axes fraction",
        fontsize=9, color="white", fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.4", facecolor=COBBLESTONE_BLUE, alpha=0.85)
    )

    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2,
               loc="upper right", fontsize=8.5, framealpha=0.9)

    ax1.set_title("EUA Carbon vs TTF Gas — Cross-Commodity Relationship (90 Days)",
                  fontsize=13, fontweight="bold", color=COBBLESTONE_BLUE, pad=12)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax1.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=2))
    plt.xticks(rotation=30, ha="right", fontsize=8.5)
    ax1.grid(True, color=GRID_COLOR, linewidth=0.7)
    ax2.grid(False)

    fig.text(0.01, -0.02, SOURCE_MARKET, fontsize=7, color="grey", ha="left")
    fig.text(0.99, -0.02, TODAY, fontsize=7, color="grey", ha="right")

    plt.tight_layout()
    out = os.path.join("output", "charts", "chart2_eua_vs_ttf.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"[charts] ✓ Chart 2 saved → {out}")
    return out


if __name__ == "__main__":
    c1 = chart1_gas_storage()
    c2 = chart2_eua_vs_ttf()
    print(f"\n[charts] Both charts generated successfully.")
    print(f"  → {c1}")
    print(f"  → {c2}")
