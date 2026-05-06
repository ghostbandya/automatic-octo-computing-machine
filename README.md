# Cobblestone Energy — European Cross-Commodity Risk Monitor

**Vedang Abhyankar** | vedangabhyankar1@gmail.com

An automated daily monitor that converts public gas and carbon fundamentals into a clear, repeatable trading narrative for European power (Day-Ahead to curve).

---

## What It Does

Running `python main.py` executes the full pipeline in one command:

1. **Pulls** EU gas storage data from GIE AGSI+
2. **Pulls** EUA carbon prices via Yahoo Finance (CO2.L)
3. **Pulls** TTF gas prices via Yahoo Finance (TTF=F)
4. **Computes** 8 daily monitor metrics with trading relevance
5. **Generates** 2 publication-quality charts
6. **Produces** a structured trading narrative (LLM-prompt-driven)
7. **Outputs** a complete Markdown daily brief to `output/`

---

## Project Structure

```
├── main.py                        # Orchestrator — run this
├── src/
│   ├── gas_data.py                # GIE AGSI+ ingestion
│   ├── carbon_data.py             # EUA price ingestion (CO2.L)
│   ├── metrics.py                 # 8 daily monitor metrics
│   ├── charts.py                  # Chart 1 (storage) + Chart 2 (EUA vs TTF)
│   └── llm_brief.py               # Prompt builder + narrative engine + logger
├── data/
│   └── raw/                       # Auto-populated CSVs
│       ├── gas_storage.csv
│       ├── gas_storage_5yr.csv
│       ├── carbon_eua.csv
│       └── ttf_prices.csv
├── output/
│   ├── charts/                    # chart1_gas_storage.png, chart2_eua_vs_ttf.png
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
# Edit .env and add your GIE AGSI+ API key:
# GIE_API_KEY=your_key_here
```
Get a free GIE AGSI+ API key at [agsi.gie.eu](https://agsi.gie.eu) → My Account.

**4. Run the pipeline**
```bash
python main.py
```

Output brief is saved to `output/daily_brief_YYYY-MM-DD.md`.

---

## Data Sources

| Source | Data | Access |
|--------|------|--------|
| [GIE AGSI+](https://agsi.gie.eu) | EU gas storage % full, injection/withdrawal | Free API key |
| [Yahoo Finance](https://finance.yahoo.com) — CO2.L | EUA carbon price proxy (WisdomTree Carbon ETP) | No key required |
| [Yahoo Finance](https://finance.yahoo.com) — TTF=F | TTF natural gas front-month price | No key required |

---

## Monitor Metrics

| # | Metric | Trading Relevance |
|---|--------|-------------------|
| 1 | EU Gas Storage % Full | Core tightness signal — below seasonal average is structurally bullish for power |
| 2 | Storage vs 5yr Seasonal Avg (ppt) | Quantifies the structural deficit relative to historical norms |
| 3 | Injection vs 30d Average (GWh/day) | Pace of summer build — slow injection compounds winter supply risk |
| 4 | TTF M+1 Price (€/MWh) | Direct gas cost input to gas-fired power generation |
| 5 | TTF 30-day Momentum (%) | Price trend — captures whether gas tightness is accelerating |
| 6 | EUA Carbon Price (€/tonne) | Raises marginal cost of coal generation; drives fuel-switching threshold |
| 7 | EUA 30-day Momentum (%) | Carbon trend — accelerating EUA compresses clean dark spread |
| 8 | Gas–Carbon 30-day Correlation | Cross-commodity linkage — divergence signals structural regime shift |

---

## AI / LLM Component

`src/llm_brief.py` implements a structured, prompt-driven narrative engine:

- **Prompt construction** (`build_prompt`): builds the exact prompt that would be sent to GPT-4o or Claude, grounded in the day's 8 metrics and specifying a three-paragraph output structure (gas tightness → carbon signal → power curve implication).
- **Narrative generation** (`_generate_from_template`): a deterministic, metrics-conditional engine that implements the same structure as the prompt — every sentence is a function of the actual metric values.
- **Logging** (`_log`): writes the full prompt and response to `output/logs/YYYY-MM-DD_log.json` on every run, providing a full audit trail.

This architecture separates prompt design from execution, making the LLM backend swappable. To connect a live LLM, replace `_generate_from_template()` with an API call — the prompt, metrics input, and logging remain unchanged.

**Sample log entry** (`output/logs/YYYY-MM-DD_log.json`):
```json
{
  "date": "2026-05-06",
  "approach": "structured_prompt_template",
  "metrics": { "storage_pct_full": 34.5, "eua_price_eur": 71.46, ... },
  "prompt": "You are a senior European energy analyst...",
  "response": "EU gas storage stands at 34.5% full..."
}
```

---

## Charts

**Chart 1 — EU Gas Storage vs 5yr Seasonal Average**
Shows current fill level against the 5-year seasonal mean and ±1 standard deviation band. Annotated with the current deviation in percentage points.

**Chart 2 — EUA Carbon vs TTF Gas Price (90 Days)**
Dual-axis line chart showing the cross-commodity relationship over the trailing 90 days. Annotated with the 30-day rolling correlation.

---

## Requirements

```
requests
pandas
python-dotenv
yfinance
matplotlib
```

Install: `pip install -r requirements.txt`

---

## Reproducibility

The pipeline is fully reproducible:
- All data is pulled fresh from public sources on each run
- No hardcoded dates or values — everything is computed dynamically
- Re-running `python main.py` on the same day produces identical output
- All intermediate CSVs are saved to `data/raw/` for inspection
