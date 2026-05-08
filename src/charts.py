"""
charts.py
---------
Generates publication-quality charts for the daily brief.

Chart 1: EU Gas Storage vs 5yr Seasonal Average
Chart 2: EUA Carbon vs TTF Gas Price (dual axis, 90 days)
Chart 3: Multi-zone Day-Ahead Power + DE Clean Spark Spread [requires ENTSO-E]
Chart 4: TTF Forward Curve Signal — price vs 30d/90d MA + momentum comparison
Chart 5: Daily Injection Pace vs 30d Average + 5yr Seasonal Norm
Chart 6: Cross-Zone Power Spreads — DE vs FR, NL, ES [requires ENTSO-E]

Install: pip install matplotlib pandas
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

COBBLESTONE_BLUE  = "#1A3557"
COBBLESTONE_RED   = "#C0392B"
COBBLESTONE_AMBER = "#E67E22"
COBBLESTONE_GREEN = "#27AE60"
BAND_COLOR        = "#AED6F1"
GRID_COLOR        = "#E8E8E8"

plt.rcParams.update({
    "font.family":       "DejaVu Sans",
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "axes.grid":         True,
    "grid.color":        GRID_COLOR,
    "grid.linewidth":    0.7,
    "figure.dpi":        150,
})

TODAY         = datetime.today().strftime("%d %b %Y")
SOURCE_GIE    = "Source: GIE AGSI+"
SOURCE_MARKET = "Source: Yahoo Finance (CO2.L, TTF=F)"
SOURCE_ENTSO  = "Source: ENTSO-E Transparency Platform, Yahoo Finance"


def chart1_gas_storage() -> str:
    """EU Gas Storage % Full vs 5yr Seasonal Average with shaded band."""
    gas_5yr = pd.read_csv("data/raw/gas_storage_5yr.csv", parse_dates=["date"])

    gas_5yr["doy"] = gas_5yr["date"].dt.dayofyear
    current_year   = datetime.today().year
    hist = gas_5yr[gas_5yr["date"].dt.year < current_year]
    band = hist.groupby("doy")["storage_pct_full"].agg(["mean", "std"]).reset_index()
    band["upper"] = band["mean"] + band["std"]
    band["lower"] = band["mean"] - band["std"]

    current = gas_5yr[gas_5yr["date"].dt.year == current_year].copy()
    current["doy"] = current["date"].dt.dayofyear
    plot_df = current.merge(band[["doy","mean","upper","lower"]], on="doy", how="left")

    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.fill_between(plot_df["date"], plot_df["lower"], plot_df["upper"],
                    color=BAND_COLOR, alpha=0.45, label="5yr +/-1 std range")
    ax.plot(plot_df["date"], plot_df["mean"],
            color=COBBLESTONE_BLUE, linewidth=1.2, linestyle="--",
            alpha=0.7, label="5yr seasonal average")
    ax.plot(plot_df["date"], plot_df["storage_pct_full"],
            color=COBBLESTONE_RED, linewidth=2.2, label="EU storage 2026")

    latest    = plot_df.dropna(subset=["storage_pct_full"]).iloc[-1]
    deviation = latest["storage_pct_full"] - latest["mean"]
    ax.annotate(
        f" {latest['storage_pct_full']:.1f}%\n ({deviation:+.1f} ppt vs avg)",
        xy=(latest["date"], latest["storage_pct_full"]),
        fontsize=8.5, color=COBBLESTONE_RED, fontweight="bold", va="center",
    )

    ax.set_title("EU Gas Storage: Current Fill vs 5-Year Seasonal Average",
                 fontsize=13, fontweight="bold", color=COBBLESTONE_BLUE, pad=12)
    ax.set_ylabel("Storage Fill Level (%)", fontsize=10)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.xticks(rotation=30, ha="right", fontsize=8.5)
    ax.set_ylim(0, 105)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.legend(loc="upper left", fontsize=8.5, framealpha=0.9)
    fig.text(0.01, -0.02, SOURCE_GIE, fontsize=7, color="grey", ha="left")
    fig.text(0.99, -0.02, TODAY,      fontsize=7, color="grey", ha="right")
    plt.tight_layout()

    out = os.path.join("output", "charts", "chart1_gas_storage.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"[charts] Chart 1 saved -> {out}")
    return out


def chart2_eua_vs_ttf() -> str:
    """EUA Carbon vs TTF Gas Price, dual axis, 90 days."""
    carbon  = pd.read_csv("data/raw/carbon_eua.csv", parse_dates=["date"])
    ttf     = pd.read_csv("data/raw/ttf_prices.csv", parse_dates=["date"])
    metrics = pd.read_csv("data/metrics.csv",         parse_dates=["date"])
    merged  = ttf.merge(carbon, on="date", how="inner").sort_values("date")
    latest_corr = metrics.dropna(subset=["gas_carbon_30d_corr"])["gas_carbon_30d_corr"].iloc[-1]

    fig, ax1 = plt.subplots(figsize=(11, 5.5))
    ax2 = ax1.twinx()

    ax1.plot(merged["date"], merged["ttf_price_eur_mwh"],
             color=COBBLESTONE_BLUE, linewidth=2.2, label="TTF Gas (EUR/MWh)")
    ax1.set_ylabel("TTF Gas Price (EUR/MWh)", fontsize=10, color=COBBLESTONE_BLUE)
    ax1.tick_params(axis="y", labelcolor=COBBLESTONE_BLUE)

    ax2.plot(merged["date"], merged["eua_price_eur"],
             color=COBBLESTONE_AMBER, linewidth=2.2, label="EUA Carbon (EUR/tonne)")
    ax2.set_ylabel("EUA Carbon Price (EUR/tonne)", fontsize=10, color=COBBLESTONE_AMBER)
    ax2.tick_params(axis="y", labelcolor=COBBLESTONE_AMBER)
    ax2.fill_between(merged["date"], merged["eua_price_eur"],
                     alpha=0.08, color=COBBLESTONE_AMBER)

    ax1.annotate(
        f"30d Correlation: {latest_corr:.2f}",
        xy=(0.02, 0.07), xycoords="axes fraction",
        fontsize=9, color="white", fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.4", facecolor=COBBLESTONE_BLUE, alpha=0.85)
    )
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2,
               loc="upper right", fontsize=8.5, framealpha=0.9)
    ax1.set_title("EUA Carbon vs TTF Gas -- Cross-Commodity Relationship (90 Days)",
                  fontsize=13, fontweight="bold", color=COBBLESTONE_BLUE, pad=12)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax1.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=2))
    plt.xticks(rotation=30, ha="right", fontsize=8.5)
    ax1.grid(True, color=GRID_COLOR, linewidth=0.7)
    ax2.grid(False)
    fig.text(0.01, -0.02, SOURCE_MARKET, fontsize=7, color="grey", ha="left")
    fig.text(0.99, -0.02, TODAY,         fontsize=7, color="grey", ha="right")
    plt.tight_layout()

    out = os.path.join("output", "charts", "chart2_eua_vs_ttf.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"[charts] Chart 2 saved -> {out}")
    return out


def chart3_power_and_spark() -> str:
    """
    Chart 3 -- Multi-zone Day-Ahead Power Prices + DE Clean Spark Spread.
    Top panel  : All available DA price zones (solid = core EUR, dashed = others).
    Bottom panel: DE Clean Spark Spread shaded green (profitable) / red (loss-making).

    Zone style guide:
      Solid, thick  : DE (blue), FR (amber), NL (purple)   — continental EUR core
      Dashed, thin  : BE (teal), ES (red), GB (green)      — peripheral / non-EUR
    """
    if not os.path.exists(os.path.join("data", "raw", "power_da.csv")):
        print("[charts] Skipping Chart 3 -- power_da.csv not found")
        return ""

    metrics = pd.read_csv("data/metrics.csv", parse_dates=["date"])
    if "de_da_price_eur_mwh" not in metrics.columns:
        print("[charts] Skipping Chart 3 -- DE DA price column missing")
        return ""
    metrics = metrics.dropna(subset=["de_da_price_eur_mwh"])

    # Zone display config: (column, label, colour, linewidth, linestyle)
    # GB excluded: post-Brexit data no longer on ENTSO-E Transparency Platform
    ZONE_STYLE = [
        ("de_da_price_eur_mwh", "DE", COBBLESTONE_BLUE,  2.2, "-"),
        ("fr_da_price_eur_mwh", "FR", COBBLESTONE_AMBER, 2.0, "-"),
        ("nl_da_price_eur_mwh", "NL", "#8E44AD",         1.8, "-"),
        ("be_da_price_eur_mwh", "BE", "#2980B9",         1.5, "--"),
        ("es_da_price_eur_mwh", "ES", COBBLESTONE_RED,   1.5, "--"),
    ]

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(11, 8), sharex=True,
        gridspec_kw={"height_ratios": [2, 1], "hspace": 0.08}
    )

    # -- Top panel: all available zones --
    latest = metrics.dropna(subset=["de_da_price_eur_mwh"]).iloc[-1]
    plotted_any = False
    for col, label, colour, lw, ls in ZONE_STYLE:
        if col not in metrics.columns:
            continue
        series = metrics[col].dropna()
        if series.empty:
            continue
        ax1.plot(metrics["date"], metrics[col],
                 color=colour, linewidth=lw, linestyle=ls,
                 label=f"{label} DA (EUR/MWh)")
        if pd.notna(latest.get(col)):
            ax1.annotate(f"  {label} {latest[col]:.0f}",
                         xy=(latest["date"], latest[col]),
                         fontsize=7.5, color=colour, fontweight="bold", va="center")
        plotted_any = True

    ax1.set_ylabel("Day-Ahead Price (EUR/MWh)", fontsize=10)
    ax1.legend(loc="upper left", fontsize=7.5, framealpha=0.9, ncol=2)
    ax1.set_title("European Day-Ahead Power Prices & DE Clean Spark Spread",
                  fontsize=13, fontweight="bold", color=COBBLESTONE_BLUE, pad=12)

    # -- Bottom panel: DE Clean Spark Spread --
    if "clean_spark_spread_eur_mwh" in metrics.columns:
        css_df = metrics.dropna(subset=["clean_spark_spread_eur_mwh"])
        css    = css_df["clean_spark_spread_eur_mwh"]
        dates  = css_df["date"]

        ax2.fill_between(dates, css, 0, where=(css >= 0),
                         color=COBBLESTONE_GREEN, alpha=0.45,
                         label="Positive (gas gen. profitable)")
        ax2.fill_between(dates, css, 0, where=(css < 0),
                         color=COBBLESTONE_RED, alpha=0.45,
                         label="Negative (gas gen. loss-making)")
        ax2.plot(dates, css, color=COBBLESTONE_BLUE, linewidth=1.5)
        ax2.axhline(0, color="black", linewidth=0.8, linestyle="--")

        latest_css = css.iloc[-1]
        ax2.annotate(
            f"  {latest_css:+.1f}",
            xy=(dates.iloc[-1], latest_css),
            fontsize=8.5,
            color=COBBLESTONE_GREEN if latest_css >= 0 else COBBLESTONE_RED,
            fontweight="bold", va="center"
        )
        ax2.annotate("CSS (DE) = DA Power - (TTF / 0.49) - (0.202 x EUA)",
                     xy=(0.01, 0.05), xycoords="axes fraction",
                     fontsize=7, color="grey", style="italic")
    else:
        ax2.text(0.5, 0.5, "Clean Spark Spread not available",
                 ha="center", va="center", transform=ax2.transAxes,
                 fontsize=9, color="grey")

    ax2.set_ylabel("DE Clean Spark Spread\n(EUR/MWh)", fontsize=9)
    ax2.legend(loc="upper left", fontsize=7.5, framealpha=0.9)

    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax2.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=2))
    plt.xticks(rotation=30, ha="right", fontsize=8.5)
    fig.text(0.01, -0.01, SOURCE_ENTSO, fontsize=7, color="grey", ha="left")
    fig.text(0.99, -0.01, TODAY,        fontsize=7, color="grey", ha="right")
    plt.tight_layout()

    out = os.path.join("output", "charts", "chart3_power_spark.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"[charts] Chart 3 saved -> {out}")
    return out


def chart4_ttf_curve_signal() -> str:
    """
    Chart 4 — TTF Forward Curve Signal.
    Top panel  : TTF price with 30d and 90d rolling averages.
                 Shaded area between MAs shows curve shape (backwardation / contango).
    Bottom panel: 30d vs 90d momentum bars — when 30d > 90d the near end is
                  accelerating (backwardation), when 30d < 90d curve is flattening.
    """
    metrics = pd.read_csv("data/metrics.csv", parse_dates=["date"])
    metrics = metrics.dropna(subset=["ttf_price_eur_mwh"]).copy()

    metrics["ma30"] = metrics["ttf_price_eur_mwh"].rolling(30, min_periods=10).mean()
    metrics["ma90"] = metrics["ttf_price_eur_mwh"].rolling(90, min_periods=30).mean()

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(11, 7), sharex=True,
        gridspec_kw={"height_ratios": [2, 1], "hspace": 0.08}
    )

    # -- Top: price + MAs --
    ax1.plot(metrics["date"], metrics["ttf_price_eur_mwh"],
             color=COBBLESTONE_BLUE, linewidth=2.2, label="TTF M+1 (EUR/MWh)", zorder=3)
    ax1.plot(metrics["date"], metrics["ma30"],
             color=COBBLESTONE_AMBER, linewidth=1.5, linestyle="--", label="30d MA", zorder=2)
    ax1.plot(metrics["date"], metrics["ma90"],
             color=COBBLESTONE_RED, linewidth=1.5, linestyle=":", label="90d MA", zorder=2)

    # Shade between 30d and 90d MA: green when 30d > 90d (backwardation), red when below
    ax1.fill_between(metrics["date"], metrics["ma30"], metrics["ma90"],
                     where=(metrics["ma30"] >= metrics["ma90"]),
                     color=COBBLESTONE_GREEN, alpha=0.18, label="Near > Far (backwardation)")
    ax1.fill_between(metrics["date"], metrics["ma30"], metrics["ma90"],
                     where=(metrics["ma30"] < metrics["ma90"]),
                     color=COBBLESTONE_RED, alpha=0.18, label="Near < Far (contango)")

    latest = metrics.dropna(subset=["ma30", "ma90"]).iloc[-1]
    signal = "BACKWARDATION" if latest["ma30"] > latest["ma90"] else "CONTANGO"
    sig_col = COBBLESTONE_GREEN if signal == "BACKWARDATION" else COBBLESTONE_RED
    ax1.annotate(
        f"  EUR {latest['ttf_price_eur_mwh']:.2f}  [{signal}]",
        xy=(latest["date"], latest["ttf_price_eur_mwh"]),
        fontsize=8.5, color=sig_col, fontweight="bold", va="center"
    )

    ax1.set_ylabel("TTF Price (EUR/MWh)", fontsize=10)
    ax1.legend(loc="upper left", fontsize=7.5, framealpha=0.9, ncol=2)
    ax1.set_title("TTF Gas — Forward Curve Signal (Day-Ahead to Curve)",
                  fontsize=13, fontweight="bold", color=COBBLESTONE_BLUE, pad=12)

    # -- Bottom: 30d vs 90d momentum bars --
    # Use separate masks — 90d momentum needs 90 rows so is NaN early on;
    # don't let that mask out the 30d bars which are available much sooner.
    mom30_df = metrics.dropna(subset=["ttf_30d_momentum_pct"]).copy()
    mom90_df = metrics.dropna(subset=["ttf_90d_momentum_pct"]).copy()

    bar_w = 1.5   # days
    if not mom30_df.empty:
        colors_30d = [COBBLESTONE_AMBER if v >= 0 else COBBLESTONE_RED
                      for v in mom30_df["ttf_30d_momentum_pct"]]
        ax2.bar(mom30_df["date"], mom30_df["ttf_30d_momentum_pct"],
                width=bar_w, color=colors_30d, alpha=0.75, label="30d momentum (%)")
    if not mom90_df.empty:
        ax2.plot(mom90_df["date"], mom90_df["ttf_90d_momentum_pct"],
                 color=COBBLESTONE_BLUE, linewidth=1.8, label="90d momentum (%)")
    ax2.axhline(0, color="black", linewidth=0.8, linestyle="--")

    # Build annotation safely — handle NaN for 90d when history is short
    _latest_full = metrics.dropna(subset=["ttf_30d_momentum_pct"]).iloc[-1]
    _mom30 = _latest_full["ttf_30d_momentum_pct"]
    _mom90_val = _latest_full.get("ttf_90d_momentum_pct")
    _mom90_str = (f"{_mom90_val:+.1f}%" if pd.notna(_mom90_val)
                  else "n/a (need >90d history)")
    ax2.annotate(
        f"  30d: {_mom30:+.1f}%\n  90d: {_mom90_str}",
        xy=(0.01, 0.85), xycoords="axes fraction",
        fontsize=7.5, color=COBBLESTONE_BLUE,
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8)
    )
    ax2.set_ylabel("Momentum (%)", fontsize=9)
    ax2.legend(loc="upper right", fontsize=7.5, framealpha=0.9)
    ax2.annotate("When 30d > 90d: near end accelerating (backwardation signal)",
                 xy=(0.01, 0.04), xycoords="axes fraction",
                 fontsize=7, color="grey", style="italic")

    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax2.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=2))
    plt.xticks(rotation=30, ha="right", fontsize=8.5)
    fig.text(0.01, -0.01, SOURCE_MARKET, fontsize=7, color="grey", ha="left")
    fig.text(0.99, -0.01, TODAY,         fontsize=7, color="grey", ha="right")
    plt.tight_layout()

    out = os.path.join("output", "charts", "chart4_ttf_curve.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"[charts] Chart 4 saved -> {out}")
    return out


def chart5_injection_pace() -> str:
    """
    Chart 5 — Daily Injection Pace vs Seasonal Norm.
    Top panel  : Daily injection/withdrawal bars (green/red) + 30d rolling average.
                 5yr seasonal average line overlaid for context.
    Bottom panel: Cumulative injection deficit vs 5yr seasonal norm (running sum).
    """
    metrics = pd.read_csv("data/metrics.csv", parse_dates=["date"])
    metrics = metrics.dropna(subset=["injection_gwh"]).copy()
    metrics = metrics.tail(90)   # focus on trailing 90 days

    has_seasonal = "injection_5yr_avg_gwh" in metrics.columns

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(11, 7), sharex=True,
        gridspec_kw={"height_ratios": [2, 1], "hspace": 0.08}
    )

    # -- Top: injection bars + averages --
    colours = [COBBLESTONE_GREEN if v >= 0 else COBBLESTONE_RED
               for v in metrics["injection_gwh"]]
    ax1.bar(metrics["date"], metrics["injection_gwh"],
            color=colours, alpha=0.6, width=0.9, label="Daily injection/withdrawal (GWh)")

    if "injection_30d_avg_gwh" in metrics.columns:
        ax1.plot(metrics["date"],
                 metrics["injection_gwh"].rolling(30, min_periods=5).mean(),
                 color=COBBLESTONE_AMBER, linewidth=2.0, label="30d rolling average")

    if has_seasonal:
        ax1.plot(metrics["date"], metrics["injection_5yr_avg_gwh"],
                 color=COBBLESTONE_BLUE, linewidth=1.8, linestyle="--",
                 label="5yr seasonal average")

    ax1.axhline(0, color="black", linewidth=0.7, linestyle="-")

    latest = metrics.iloc[-1]
    ax1.annotate(f"  Latest: {latest['injection_gwh']:+.0f} GWh",
                 xy=(latest["date"], latest["injection_gwh"]),
                 fontsize=8.5, color=COBBLESTONE_BLUE, fontweight="bold", va="bottom")

    ax1.set_ylabel("Injection / Withdrawal (GWh/day)", fontsize=10)
    ax1.legend(loc="upper left", fontsize=8, framealpha=0.9)
    ax1.set_title("EU Gas Storage — Daily Injection Pace vs Seasonal Norm",
                  fontsize=13, fontweight="bold", color=COBBLESTONE_BLUE, pad=12)

    # -- Bottom: cumulative deficit vs 5yr seasonal --
    if has_seasonal:
        metrics["cum_deficit"] = (
            (metrics["injection_gwh"] - metrics["injection_5yr_avg_gwh"])
            .fillna(0).cumsum()
        )
        ax2.fill_between(metrics["date"], metrics["cum_deficit"], 0,
                         where=(metrics["cum_deficit"] < 0),
                         color=COBBLESTONE_RED, alpha=0.4, label="Cumulative injection deficit")
        ax2.fill_between(metrics["date"], metrics["cum_deficit"], 0,
                         where=(metrics["cum_deficit"] >= 0),
                         color=COBBLESTONE_GREEN, alpha=0.4, label="Cumulative injection surplus")
        ax2.plot(metrics["date"], metrics["cum_deficit"],
                 color=COBBLESTONE_BLUE, linewidth=1.5)
        ax2.axhline(0, color="black", linewidth=0.8, linestyle="--")
        ax2.set_ylabel("Cumulative Deficit\nvs 5yr Norm (GWh)", fontsize=9)
        ax2.legend(loc="lower left", fontsize=7.5, framealpha=0.9)
    else:
        # Fallback: just show vs 30d average
        ax2.bar(metrics["date"], metrics["injection_vs_30d_avg_gwh"],
                color=[COBBLESTONE_GREEN if v >= 0 else COBBLESTONE_RED
                       for v in metrics["injection_vs_30d_avg_gwh"].fillna(0)],
                alpha=0.5, width=0.9)
        ax2.axhline(0, color="black", linewidth=0.8, linestyle="--")
        ax2.set_ylabel("vs 30d Average\n(GWh/day)", fontsize=9)

    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax2.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=2))
    plt.xticks(rotation=30, ha="right", fontsize=8.5)
    fig.text(0.01, -0.01, SOURCE_GIE, fontsize=7, color="grey", ha="left")
    fig.text(0.99, -0.01, TODAY,      fontsize=7, color="grey", ha="right")
    plt.tight_layout()

    out = os.path.join("output", "charts", "chart5_injection_pace.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"[charts] Chart 5 saved -> {out}")
    return out


def chart6_power_spreads() -> str:
    """
    Chart 6 — Cross-Zone Power Spreads (DE as base).
    Shows DE minus FR, NL, ES spreads over 90 days.
    A wide DE-ES spread signals Iberian grid isolation.
    A collapsing DE-FR spread signals tight cross-border interconnection.
    """
    if not os.path.exists(os.path.join("data", "raw", "power_da.csv")):
        print("[charts] Skipping Chart 6 -- power_da.csv not found")
        return ""

    metrics = pd.read_csv("data/metrics.csv", parse_dates=["date"])
    if "de_da_price_eur_mwh" not in metrics.columns:
        print("[charts] Skipping Chart 6 -- DE DA price column missing")
        return ""

    metrics = metrics.dropna(subset=["de_da_price_eur_mwh"]).copy()

    # Spread pairs: (column to subtract, label, colour, linestyle)
    SPREAD_PAIRS = [
        ("fr_da_price_eur_mwh", "DE–FR", COBBLESTONE_AMBER, "-",  2.0),
        ("nl_da_price_eur_mwh", "DE–NL", "#8E44AD",         "-",  1.8),
        ("be_da_price_eur_mwh", "DE–BE", "#2980B9",         "--", 1.5),
        ("es_da_price_eur_mwh", "DE–ES", COBBLESTONE_RED,   "-",  2.0),
    ]

    fig, ax = plt.subplots(figsize=(11, 5.5))

    plotted = False
    latest  = metrics.iloc[-1]
    for col, label, colour, ls, lw in SPREAD_PAIRS:
        if col not in metrics.columns:
            continue
        spread = metrics["de_da_price_eur_mwh"] - metrics[col]
        ax.plot(metrics["date"], spread,
                color=colour, linewidth=lw, linestyle=ls, label=f"{label} spread (EUR/MWh)")
        if pd.notna(latest.get(col)):
            val = latest["de_da_price_eur_mwh"] - latest[col]
            ax.annotate(f"  {label}: {val:+.0f}",
                        xy=(latest["date"], val),
                        fontsize=8, color=colour, fontweight="bold", va="center")
        plotted = True

    if not plotted:
        plt.close()
        return ""

    ax.axhline(0, color="black", linewidth=1.0, linestyle="--", alpha=0.6)
    ax.fill_between(metrics["date"],
                    metrics["de_da_price_eur_mwh"] - metrics.get("es_da_price_eur_mwh",
                    pd.Series([float("nan")] * len(metrics))),
                    0, alpha=0.06, color=COBBLESTONE_RED)

    ax.set_title("Cross-Zone Power Spreads — DE as Base (EUR/MWh)",
                 fontsize=13, fontweight="bold", color=COBBLESTONE_BLUE, pad=12)
    ax.set_ylabel("Spread vs DE Day-Ahead (EUR/MWh)", fontsize=10)
    ax.annotate("Positive = DE more expensive | Negative = DE cheaper",
                xy=(0.01, 0.04), xycoords="axes fraction",
                fontsize=7.5, color="grey", style="italic")
    ax.legend(loc="upper left", fontsize=8.5, framealpha=0.9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=2))
    plt.xticks(rotation=30, ha="right", fontsize=8.5)
    fig.text(0.01, -0.02, SOURCE_ENTSO, fontsize=7, color="grey", ha="left")
    fig.text(0.99, -0.02, TODAY,        fontsize=7, color="grey", ha="right")
    plt.tight_layout()

    out = os.path.join("output", "charts", "chart6_power_spreads.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    plt.savefig(out, bbox_inches="tight")
    plt.close()
    print(f"[charts] Chart 6 saved -> {out}")
    return out


if __name__ == "__main__":
    chart1_gas_storage()
    chart2_eua_vs_ttf()
    chart3_power_and_spark()
    chart4_ttf_curve_signal()
    chart5_injection_pace()
    chart6_power_spreads()
