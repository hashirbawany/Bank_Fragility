# U.S. Bank Fragility — Replication Pipeline

An open-source replication of **Jiang, Matvos, Piskorski & Seru (2023), "Monetary Tightening and U.S. Bank Fragility in 2023: Mark-to-Market Losses and Uninsured Depositor Runs?"** ([NBER Working Paper 31048](https://www.nber.org/papers/w31048)).

The original paper uses proprietary WRDS data. This project reproduces the full analysis — including Table 1 and all figures — using only **free, publicly available data sources**. A single `doit` command pulls the data, runs the analysis, and builds all output documents.

---

## What This Project Does

The paper asks: how exposed were U.S. banks to rising interest rates in 2022–2023? When rates rise sharply, the market value of fixed-income assets falls. If losses are large enough relative to uninsured deposits, a bank faces run risk even without a solvency crisis.

This replication:

1. Downloads every FDIC-insured bank's balance sheet from the FFIEC Call Report
2. Pulls U.S. Treasury yield curves from FRED to compute yield shocks across six maturity buckets
3. Calibrates an RMBS haircut multiplier from MBS ETF price data (Yahoo Finance)
4. Applies duration-based mark-to-market losses to each bank's asset portfolio
5. Computes a fragility measure: uninsured deposits divided by mark-to-market assets
6. Reproduces Table 1 broken out by Small banks, Large non-GSIB banks, and G-SIBs

---

## Data Sources

| Source | What it provides | Access |
|--------|-----------------|--------|
| [FFIEC CDR](https://cdr.ffiec.gov/public/) | Quarterly Call Reports — balance sheets for all FDIC-insured banks | Free, no login |
| [FRED API](https://fred.stlouisfed.org/) | U.S. Treasury constant-maturity yields | Free API key |
| [Yahoo Finance](https://finance.yahoo.com/) | MBS ETF prices (SPMB, CMBS) for RMBS multiplier calibration | Free, no login |

No WRDS, Bloomberg, or paid data subscription required.

---

## Project Structure

```
Bank_Fragility/
├── dodo.py                    # doit pipeline definition
├── .env.sample                # Configuration template
├── scripts/                   # Data and analysis scripts
│   ├── pull_ffiec.py          # Download FFIEC Call Report ZIP
│   ├── pull_gsib_banks.py     # Build G-SIB bank list
│   ├── pull_treasury_yields.py# Pull Treasury yields from FRED
│   ├── pull_mbs_etfs.py       # Pull MBS ETF prices from Yahoo Finance
│   ├── compute_yield_shocks.py# Compute yield shocks per maturity bucket
│   ├── process_ffiec.py       # Parse Call Report → bank panel + Figure A1
│   └── make_table_1.py        # Compute MTM losses and fragility → Table 1
├── document_scripts/          # Output document generators
│   ├── generate_research_paper.py  # LaTeX research paper → PDF
│   ├── generate_student_notebook.py# Teaching notebook (Jupyter)
│   ├── generate_blog_html.py       # Blog-style HTML with embedded charts
│   ├── generate_pdf.py             # Project description PDF
│   └── generate_pipeline_doc.py    # Pipeline documentation PDF
├── documents/                 # Pre-built output documents
│   ├── research_paper.pdf
│   ├── bank_fragility_blog.html
│   ├── student_notebook.ipynb
│   ├── pipeline_documentation.pdf
│   └── project_description.pdf
├── _data/                     # Downloaded raw data (gitignored)
└── _output/                   # Computed outputs: tables, figures (gitignored)
```

---

## Quickstart

### 1. Clone and install dependencies

```bash
git clone https://github.com/hashirbawany/Bank_Fragility.git
cd Bank_Fragility
pip install doit pandas numpy matplotlib openpyxl python-dotenv fredapi yfinance selenium
```

> **Note:** `pull_ffiec.py` uses Selenium with Chrome to download the FFIEC ZIP. Make sure [ChromeDriver](https://chromedriver.chromium.org/) is installed and on your PATH.

### 2. Configure

```bash
cp .env.sample .env
```

Edit `.env`:

```ini
REPORT_DATE_SLASH=12/31/2024   # Quarter-end date to analyse (MM/DD/YYYY)
RMBS_MULTIPLIER=1.25            # RMBS haircut multiplier (1.25 = Jiang et al. baseline)
FRED_API_KEY=your_key_here      # Free key from https://fred.stlouisfed.org/docs/api/api_key.html
```

### 3. Run the pipeline

```bash
doit
```

doit runs only tasks whose inputs have changed. To force a full re-run:

```bash
doit clean -a
doit
```

---

## Pipeline DAG

```
pull_ffiec ─┐
pull_gsib  ─┼─► process_ffiec ─┐
            │                   ├─► make_table_1 ─► generate_research_paper
pull_treasury ► compute_yield_shocks ─┘              generate_blog_html
pull_mbs ───────────────────────────────────────►    generate_student_notebook
                                                     generate_pdf
                                                     generate_pipeline_doc
```

---

## Output Documents

| Document | Description |
|----------|-------------|
| `research_paper.pdf` | Full replication paper with methodology, Table 1, and figures |
| `bank_fragility_blog.html` | Blog-style article with embedded interactive charts and EDA visualisations |
| `student_notebook.ipynb` | Step-by-step teaching notebook — run cell-by-cell to follow the methodology |
| `pipeline_documentation.pdf` | Technical reference for the doit pipeline |
| `project_description.pdf` | Non-technical project overview |

Pre-built versions of all documents are in the `documents/` folder.

---

## Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `REPORT_DATE_SLASH` | `12/31/2024` | Call Report quarter-end date |
| `RMBS_MULTIPLIER` | `1.25` | Scales MTM losses on residential MBS. Calibrated from the gap between SPMB's actual price path and duration-implied path during the rate shock. |
| `FRED_API_KEY` | — | Required for Treasury yield data |

Changing any `.env` value causes doit to re-run only the affected downstream tasks.

---

## Reference

Jiang, E., Matvos, G., Piskorski, T., & Seru, A. (2023). *Monetary Tightening and U.S. Bank Fragility in 2023: Mark-to-Market Losses and Uninsured Depositor Runs?* NBER Working Paper 31048.
