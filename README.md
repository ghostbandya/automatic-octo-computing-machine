# Cobblestone Energy — European Cross-Commodity Risk Monitor

**Vedang Abhyankar** | vedang.abhyankar25@imperial.ac.uk

An automated daily monitor that converts public gas, carbon and power fundamentals into a clear, repeatable trading narrative covering the full Gas + Carbon → Power Curve signal chain.

---

## What It Does

Running `python main.py` executes the full pipeline in one command:

1. **Pulls** EU gas storage data from GIE AGSI+ (90-day window + 5yr seasonal history)
2. **Pulls** EUA carbon prices via Yahoo Finance (CO2.L — WisdomTree Carbon ETP)
3. **Pulls** TTF gas prices via Yahoo Finance (TTF=F front-month)
4. **Pulls** DE and FR Day-Ahead power prices via ENTSO-E Transparency Platform *(optional — requires free API token)*
5. **Computes**  daily monitor metrics with signal classification and trading relevance
6. **Generates**  publication-quality charts
7. **Produces** a structured LLM-prompt-driven trading narrative
8. **Outputs** a complete Markdown daily brief to `output/`

---

## Project Structure

```
├── main.py                        # Orchestrator — run this
├── src/
│   ├── gas_data.py                # GIE AGSI+ ingestion 
│   ├── carbon_data.py             # EUA price ingestion (CO2.L via Yahoo Finance)
│   ├── power_data.py              # ENTSO-E Day-Ahead prices
│   ├── metrics.py                 # daily monitor metrics
│   ├── charts.py                  # charts
│   └── llm_brief.py               # Prompt builder + narrative engine + logger
├── data/
│   └── raw/                       # Auto-populated CSVs 
│       ├── gas_storage.csv
│       ├── gas_storage_5yr.csv
│       ├── carbon_eua.csv
│       ├── ttf_prices.csv
│       └── power_da.csv           # ENTSO-E only — requires token
├── output/
│   ├── charts/                    
│   ├── logs/                      # YYYY-MM-DD_log.json (prompt + response)
│   └── daily_brief_YYYY-MM-DD.md  # Final assembled brief
├── .env                           # Environment variable 
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
cp .env
# Edit .env — add your API keys:
# GIE_API_KEY=your_gie_key_here
# ENTSO_E_TOKEN=your_entso_token_here   
```

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
