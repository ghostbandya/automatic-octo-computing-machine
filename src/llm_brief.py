"""
llm_brief.py
------------
AI/LLM component of the Cobblestone Energy daily brief pipeline.

Approach
--------
Rather than calling an external LLM API, this module implements a
structured, prompt-driven narrative engine:
  1. Builds a formal LLM prompt grounded in the day's metrics
     (same prompt that would be sent to GPT-4o / Claude)
  2. Uses a deterministic template to fill the prompt with market logic
  3. Logs the full prompt + generated response to output/logs/YYYY-MM-DD_log.json

This architecture makes the LLM integration swappable — dropping in an
API call requires only replacing _generate_from_template() with an
_call_openai() or _call_anthropic() function.

Outputs: output/logs/YYYY-MM-DD_log.json

Install: pip install pandas python-dotenv
"""

import os
import json
import pandas as pd
from datetime import datetime

TODAY   = datetime.today().strftime("%Y-%m-%d")
LOG_DIR = os.path.join("output", "logs")

# Zone display labels — must match column names in metrics.csv
ZONE_LABELS = {
    "de": "DE (Germany)",
    "fr": "FR (France)",
    "nl": "NL (Netherlands)",
    "be": "BE (Belgium)",
    "es": "ES (Spain)",
}


# ── Prompt builder ─────────────────────────────────────────────────────────────

def build_prompt(metrics: dict) -> str:
    """
    Constructs the structured LLM prompt grounded in the day's metrics.
    This is the exact prompt that would be sent to an LLM API.
    """
    base = f"""You are a senior European energy analyst at a commodity trading firm.
Today is {metrics['date']}. Your task is to write a concise, decision-useful trading
narrative (150-200 words) for the morning desk brief.

Use ONLY the data provided below. Be direct, use numbers, and state the trading
implication clearly. Write in clear prose — no bullet points, no headers.

--- TODAY'S MONITOR METRICS ---
1. EU Gas Storage:          {metrics['storage_pct_full']:.1f}% full
2. vs 5yr Seasonal Average: {metrics['storage_vs_5yr_avg_ppt']:+.1f} ppt
3. Injection vs 30d Avg:    {metrics['injection_vs_30d_avg_gwh']:+.0f} GWh/day
4. TTF M+1 Price:           €{metrics['ttf_price_eur_mwh']:.2f}/MWh
5. TTF 30d Momentum:        {metrics['ttf_30d_momentum_pct']:+.1f}%
6. EUA Carbon Price:        €{metrics['eua_price_eur']:.2f}/tonne
7. EUA 30d Momentum:        {metrics['eua_30d_momentum_pct']:+.1f}%
8. Gas-Carbon 30d Corr:     {metrics['gas_carbon_30d_corr']:.2f}
9. TTF 90d Momentum:        {metrics.get('ttf_90d_momentum_pct', float('nan')):+.1f}%
10. TTF Curve Premium:       {metrics.get('ttf_curve_premium_pct', float('nan')):+.1f}%  (positive = backwardation, negative = contango)"""

    # Add all available DA power zone prices
    n = 9
    power_lines = []
    for zone, label in ZONE_LABELS.items():
        col = f"{zone}_da_price_eur_mwh"
        val = metrics.get(col)
        if val and not (isinstance(val, float) and pd.isna(val)):
            power_lines.append(f"{n:>2}. {label} Day-Ahead:  EUR {val:.2f}/MWh")
            n += 1
    if power_lines:
        base += "\n" + "\n".join(power_lines)

    # Add CSS if available
    css = metrics.get("clean_spark_spread_eur_mwh")
    if css and not (isinstance(css, float) and pd.isna(css)):
        base += f"\n{n:>2}. DE Clean Spark Spread:    EUR {css:.2f}/MWh"

    base += """
---

Structure your narrative as three flowing paragraphs:
1. Gas tightness: what the storage deficit and injection pace mean for supply risk.
2. Carbon signal: what EUA momentum and the gas-carbon correlation tell us.
3. Power curve implication: reference the Day-Ahead prices across all markets listed
   above — note which zone is highest/lowest, what any spread between them signals
   (grid constraints, renewable mix, market integration), and the CSS reading.
   Incorporate the TTF curve signal (backwardation/contango) and what it implies
   for near-term vs winter pricing risk.
   State clearly what a trader should watch.

Keep it under 200 words. No fluff."""
    return base


# ── Narrative generator ────────────────────────────────────────────────────────

def _generate_from_template(m: dict) -> str:
    """
    Deterministic, metrics-grounded narrative engine.
    Implements the same three-paragraph structure as the LLM prompt.
    All statements are conditional on the actual metric values.
    """

    # --- Paragraph 1: Gas tightness ---
    deficit_label = (
        "significantly below" if m['storage_vs_5yr_avg_ppt'] < -10 else
        "moderately below"   if m['storage_vs_5yr_avg_ppt'] < -5  else
        "near"
    )
    inj_label = "above" if m['injection_vs_30d_avg_gwh'] > 0 else "below"
    inj_read  = (
        "the pace of the summer build is improving, though the absolute deficit remains large"
        if m['injection_vs_30d_avg_gwh'] > 0 else
        "the storage build is running below seasonal norms, compounding the supply risk"
    )
    p1 = (
        f"EU gas storage stands at {m['storage_pct_full']:.1f}% full, "
        f"{deficit_label} the 5-year seasonal average by {abs(m['storage_vs_5yr_avg_ppt']):.1f} ppt. "
        f"The daily injection rate is running {inj_label} its 30-day average by "
        f"{abs(m['injection_vs_30d_avg_gwh']):.0f} GWh, meaning {inj_read}. "
        f"Heading into the injection season with storage this lean increases the probability "
        f"of a tight winter supply balance."
    )

    # --- Paragraph 2: Carbon signal ---
    eua_trend  = "rising" if m['eua_30d_momentum_pct'] > 0 else "falling"
    eua_read   = (
        "adding to marginal generation costs and narrowing the coal-to-gas switching window"
        if m['eua_30d_momentum_pct'] > 0 else
        "offering some relief on generation costs, though absolute levels remain elevated"
    )
    corr_read  = (
        "moving in opposite directions over the past 30 days — gas has softened while carbon "
        "firms, a divergence that compresses clean dark spreads and supports gas-fired generation "
        "economics"
        if m['gas_carbon_30d_corr'] < -0.3 else
        "broadly in sync over the past 30 days, amplifying directional moves in power prices"
        if m['gas_carbon_30d_corr'] > 0.3 else
        "showing little cross-commodity correlation recently"
    )
    p2 = (
        f"EUA carbon is {eua_trend} at EUR {m['eua_price_eur']:.2f}/tonne "
        f"({m['eua_30d_momentum_pct']:+.1f}% over 30 days), {eua_read}. "
        f"Gas and carbon are {corr_read} "
        f"(30-day correlation: {m['gas_carbon_30d_corr']:.2f})."
    )

    # --- Paragraph 3: Power curve implication ---
    ttf_trend = "easing" if m['ttf_30d_momentum_pct'] < -3 else "firm"
    bias      = (
        "upside-biased"  if m['storage_vs_5yr_avg_ppt'] < -5 and m['eua_30d_momentum_pct'] > 0 else
        "downside risk"  if m['storage_vs_5yr_avg_ppt'] > 0  and m['eua_30d_momentum_pct'] < 0 else
        "balanced"
    )

    # Curve shape signal from TTF 30d vs 90d momentum
    curve_prem = m.get("ttf_curve_premium_pct")
    ttf_90d    = m.get("ttf_90d_momentum_pct")
    if curve_prem is not None and not (isinstance(curve_prem, float) and pd.isna(curve_prem)):
        if curve_prem > 2:
            curve_note = (
                f" The TTF curve premium of {curve_prem:+.1f}% signals backwardation — "
                f"near-term prices are trading above the 90-day mean, reflecting "
                f"immediate supply tightness rather than a structural shift."
            )
        elif curve_prem < -2:
            curve_note = (
                f" The TTF curve discount of {curve_prem:+.1f}% signals contango — "
                f"near-term prices are below the 90-day mean, suggesting the market "
                f"is relaxed on prompt supply but cautious further out."
            )
        else:
            curve_note = (
                f" The TTF curve is near flat ({curve_prem:+.1f}% premium vs 90d mean), "
                f"suggesting no strong near-term vs winter pricing divergence."
            )
    else:
        curve_note = ""

    # Collect available zone prices for cross-market commentary
    zone_prices = {}
    for zone, label in ZONE_LABELS.items():
        col = f"{zone}_da_price_eur_mwh"
        val = m.get(col)
        if val and not (isinstance(val, float) and pd.isna(val)):
            zone_prices[label] = val

    if zone_prices:
        # Build a compact zone price list: "DE EUR 122/MWh, FR EUR 113/MWh, ..."
        price_list = ", ".join(
            f"{lbl.split(' ')[0]} EUR {price:.0f}/MWh"
            for lbl, price in zone_prices.items()
        )
        # Identify highest and lowest markets
        highest_lbl = max(zone_prices, key=zone_prices.get)
        lowest_lbl  = min(zone_prices, key=zone_prices.get)
        spread      = max(zone_prices.values()) - min(zone_prices.values())

        # ES often diverges — flag if it's the outlier
        if "ES (Spain)" in zone_prices:
            es_price = zone_prices["ES (Spain)"]
            de_price = zone_prices.get("DE (Germany)", es_price)
            es_spread_note = (
                f" The DE–ES spread of EUR {abs(de_price - es_price):.0f}/MWh reflects "
                f"Iberian grid constraints and a {'higher' if es_price < de_price else 'lower'} "
                f"renewable contribution in Spain."
                if abs(de_price - es_price) > 15 else ""
            )
        else:
            es_spread_note = ""

        zone_sentence = (
            f"Day-Ahead prices across the five monitored markets: {price_list}. "
            f"The EUR {spread:.0f}/MWh spread between {highest_lbl.split(' ')[0]} "
            f"(highest) and {lowest_lbl.split(' ')[0]} (lowest) "
            f"{'signals grid congestion or divergent fuel/renewable mix' if spread > 20 else 'indicates broadly integrated continental pricing'}."
            f"{es_spread_note}"
        )
    else:
        zone_sentence = ""

    css_note = ""
    css = m.get("clean_spark_spread_eur_mwh")
    if css and not (isinstance(css, float) and pd.isna(css)):
        css_note = (
            f" The DE clean spark spread at EUR {css:.2f}/MWh confirms gas-fired generation "
            f"is {'profitable' if css > 0 else 'loss-making'} at current prices."
        )

    p3 = (
        f"With TTF {ttf_trend} at EUR {m['ttf_price_eur_mwh']:.2f}/MWh and carbon costs elevated, "
        f"the cross-commodity input stack keeps near-curve power {bias}."
        f"{curve_note} "
        f"{zone_sentence}{css_note} "
        f"The primary risk flag is the storage deficit: any demand spike or supply disruption "
        f"(Norwegian outages, LNG diversion) would re-price the forward curve sharply higher. "
        f"Watch daily injection prints and TTF prompt spreads as leading indicators."
    )

    return f"{p1}\n\n{p2}\n\n{p3}"


# ── Logger ─────────────────────────────────────────────────────────────────────

def _log(prompt: str, narrative: str, metrics: dict) -> str:
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"{TODAY}_log.json")
    entry = {
        "date":      TODAY,
        "timestamp": datetime.now().isoformat(),
        "approach":  "structured_prompt_template",
        "note":      "Prompt built identically to what would be sent to GPT-4o/Claude. "
                     "Narrative generated by deterministic template conditioned on same metrics.",
        "metrics":   {k: round(v, 4) if isinstance(v, float) else v
                      for k, v in metrics.items()},
        "prompt":    prompt,
        "response":  narrative,
    }
    with open(log_path, "w") as f:
        json.dump(entry, f, indent=2, default=str)
    print(f"[llm_brief] ✓ Prompt + response logged → {log_path}")
    return log_path


# ── Main entry point ───────────────────────────────────────────────────────────

def generate_narrative(metrics_path: str = "data/metrics.csv") -> str:
    """
    Loads today's metrics, builds the LLM prompt, generates a
    metrics-grounded narrative, logs everything, and returns the narrative.
    """
    df      = pd.read_csv(metrics_path, parse_dates=["date"])
    latest  = df.dropna(subset=["storage_pct_full", "eua_price_eur"]).iloc[-1]
    metrics = latest.to_dict()

    prompt    = build_prompt(metrics)
    narrative = _generate_from_template(metrics)

    _log(prompt, narrative, metrics)
    print(f"[llm_brief] ✓ Narrative generated ({len(narrative.split())} words)")
    return narrative


if __name__ == "__main__":
    narrative = generate_narrative()
    print("\n--- NARRATIVE ---\n")
    print(narrative)
