# Cobblestone Energy — European Cross-Commodity Risk Monitor

**Vedang Abhyankar** | vedangabhyankar1@gmail.com

An automated daily monitor that converts public gas, carbon and power fundamentals into a clear, repeatable trading narrative covering the full Gas + Carbon → Power Curve signal chain.

---

## What It Does

Running `python main.py` executes the full pipeline in one command:

1. **Pulls** EU gas storage data from GIE AGSI+ (90-day window + 5yr seasonal history)
2. **Pulls** EUA carbon prices via Yahoo Finance (CO2.L — WisdomTree Carbon ETP)
3. **Pulls** TTF gas prices via Yahoo Finance (TTF=F front-month)
4. **Pulls** DE and FR Day-Ahead power prices via ENTSO-E Transparency Platform *(optional — requires free API token)*
5. **Computes** 12 daily monitor metrics with signal classification and trading relevance
6. **Generates** 3 publication-quality charts
7. **Produces** a structured LLM-prompt-driven trading narrative
8. **Outputs** a complete Markdown daily brief to `output/`

---

## Project Structure

```
├── main.py                        # Orchestrator — run this
├── src/
│   ├── gas_data.py                # GIE AGSI+ ingestion (90d + 5yr history)
│   ├── carbon_data.py             # EUA price ingestion (CO2.L via Yahoo Finance)
│   ├── power_data.py              # ENTSO-E Day-Ahead prices (DE_LU + FR)
│   ├── metrics.py                 # 12 daily monitor metrics
│   ├── charts.py                  # 3 publication-quality charts
│   └── llm_brief.py               # Prompt builder + narrative engine + logger
├── data/
│   └── raw/                       # Auto-populated CSVs (git-ignored)
│       ├── gas_storage.csv
│       ├── gas_storage_5yr.csv
│       ├── carbon_eua.csv
│       ├── ttf_prices.csv
│       └── power_da.csv           # ENTSO-E only — requires token
├── output/
│   ├── charts/                    # chart1_gas_storage.png, chart2_eua_vs_ttf.png,
│   │                              # chart3_power_spark.png
│   ├── cobblestone_desk_note.html # Fundamentals desk note (open in browser)
│   ├── logs/                      # YYYY-MM-DD_log.json (prompt + response)
│   └── daily_brief_YYYY-MM-DD.md  # Final assembled brief
├── .env.example                   # Environment variable template
└── requirements.txt
```

---

## Setup

**1. Clone and create a virtual environment**
```bash
git clone https://github.com/your-username/automatic-octo-computing-machine.git
cd automatic-octo-computing-machine
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Configure environment**
```bash
cp .env.example .env
# Edit .env — add your API keys:
# GIE_API_KEY=your_gie_key_here
# ENTSO_E_TOKEN=your_entso_token_here   # optional — skipped if blank
```

- **GIE AGSI+ key** (required): free at [agsi.gie.eu](https://agsi.gie.eu) → My Account
- **ENTSO-E token** (optional): free at [transparency.entsoe.eu](https://transparency.entsoe.eu) → My Account → Security Tokens. Without it, Steps 4/9–12 are skipped.

**4. Run the pipeline**
```bash
python main.py
```

Output brief: `output/daily_brief_YYYY-MM-DD.md`
Desk note: open `output/cobblestone_desk_note.html` in any browser (→ Print to PDF)

---

## Data Sources

| Source | Data | Access |
|--------|------|--------|
| [GIE AGSI+](https://agsi.gie.eu) | EU gas storage % full, injection/withdrawal, 5yr history | Free API key |
| [Yahoo Finance](https://finance.yahoo.com) — CO2.L | EUA carbon price proxy (WisdomTree Carbon ETP) | No key required |
| [Yahoo Finance](https://finance.yahoo.com) — TTF=F | TTF natural gas front-month price | No key required |
| [ENTSO-E Transparency Platform](https://transparency.entsoe.eu) | DE and FR Day-Ahead electricity prices (hourly → daily avg) | Free security token |

---

## Monitor Metrics

| # | Metric | Trading Relevance |
|---|--------|-------------------|
| 1 | EU Gas Storage % Full | Core tightness signal — below seasonal average is structurally bullish for power |
| 2 | Storage vs 5yr Seasonal Avg (ppt) | Quantifies the structural deficit relative to historical norms |
| 3 | Injection vs 30d Average (GWh/day) | Pace of summer build — slow injection compounds winter supply risk |
| 4 | TTF M+1 Price (EUR/MWh) | Direct gas cost input to gas-fired power generation |
| 5 | TTF 30-day Momentum (%) | Price trend — captures whether gas tightness is accelerating |
| 6 | EUA Carbon Price (EUR/tonne) | Raises marginal cost of coal generation; drives fuel-switching threshold |
| 7 | EUA 30-day Momentum (%) | Carbon trend — accelerating EUA compresses clean dark spread |
| 8 | Gas–Carbon 30-day Correlation | Cross-commodity linkage — divergence signals structural regime shift |
| 9 | DE Day-Ahead Power (EUR/MWh) | Real-time gas+carbon cost pass-through to clearing price *(ENTSO-E)* |
| 10 | FR Day-Ahead Power (EUR/MWh) | Nuclear/hydro mix shifts DE–FR spread; cross-border arb signal *(ENTSO-E)* |
| 11 | Clean Spark Spread (EUR/MWh) | Gas-fired margin = DA Power − (TTF/0.49) − (0.202×EUA); negative = loss-making *(ENTSO-E)* |
| 12 | DE Power–Gas Spread (EUR/MWh) | Carbon + scarcity premium above pure fuel cost *(ENTSO-E)* |

---

## AI / LLM Component

`src/llm_brief.py` implements a structured, prompt-driven narrative engine:

- **Prompt construction** (`build_prompt`): builds the exact prompt that would be sent to GPT-4o or Claude, grounded in the day's metrics and specifying a three-paragraph output structure (gas tightness → carbon signal → power curve implication). Power metrics are appended when available.
- **Narrative generation** (`_generate_from_template`): a deterministic, metrics-conditional engine implementing the same three-paragraph structure — every sentence is a function of actual metric values, including the Clean Spark Spread interpretation when power data is present.
- **Logging** (`_log`): writes the full prompt and response to `output/logs/YYYY-MM-DD_log.json` on every run, providing a complete audit trail.

This architecture separates prompt design from execution, making the LLM backend swappable. To connect a live LLM, replace `_generate_from_template()` with an API call — the prompt, metrics input, and logging remain unchanged.

**Sample log entry** (`output/logs/YYYY-MM-DD_log.json`):
```json
{
  "date": "2026-05-07",
  "approach": "structured_prompt_template",
  "metrics": { "storage_pct_full": 34.3, "eua_price_eur": 71.81,
               "clean_spark_spread_eur_mwh": 18.34, ... },
  "prompt": "You are a senior European energy analyst...",
  "response": "EU gas storage stands at 34.3% full..."
}
```

---

## Charts

**Chart 1 — EU Gas Storage vs 5yr Seasonal Average**
Current fill level against the 5-year seasonal mean and ±1 standard deviation band. Annotated with the current deviation in percentage points.

**Chart 2 — EUA Carbon vs TTF Gas Price (90 Days)**
Dual-axis line chart showing the cross-commodity relationship over the trailing 90 days. Annotated with the 30-day rolling correlation coefficient.

**Chart 3 — European Power Prices & Clean Spark Spread** *(ENTSO-E)*
Two-panel chart: DE/FR Day-Ahead daily average prices (top) and the Clean Spark Spread shaded green (profitable) / red (loss-making) with the CSS formula annotated (bottom).

---

## Requirements

```
requests
pandas
python-dotenv
yfinance
matplotlib
entsoe-py       # Day-Ahead power prices — optional
```

Install: `pip install -r requirements.txt`

---

## Reproducibility

The pipeline is fully reproducible:
- All data is pulled fresh from public sources on each run
- No hardcoded dates or values — everything is computed dynamically
- The 5yr gas storage history is cached after the first run (no need to re-fetch)
- All intermediate CSVs are saved to `data/raw/` for inspection
- Re-running `python main.py` on the same day produces identical output
