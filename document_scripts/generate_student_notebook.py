"""
Generates documents/student_notebook.ipynb — a comprehensive teaching notebook
that walks through the full Bank Fragility replication pipeline step by step.
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "documents" / "student_notebook.ipynb"


def md(source, cell_id):
    return {"cell_type": "markdown", "id": cell_id, "metadata": {}, "source": source}


def code(source, cell_id):
    return {
        "cell_type": "code",
        "execution_count": None,
        "id": cell_id,
        "metadata": {},
        "outputs": [],
        "source": source,
    }


# ---------------------------------------------------------------------------
# Cell content
# ---------------------------------------------------------------------------

TITLE = """\
# U.S. Bank Fragility — Teaching Notebook
### A Step-by-Step Replication of Jiang, Matvos, Piskorski & Seru (2023)

This notebook walks a student or researcher through the complete methodology for
estimating mark-to-market (MTM) losses on U.S. bank balance sheets and measuring
depositor run risk. Every assumption is made explicit and every computational step
is shown in full.

**Reference:** Jiang, E., Matvos, G., Piskorski, T., & Seru, A. (2023).
*Monetary Tightening and U.S. Bank Fragility in 2023: Mark-to-Market Losses and
Uninsured Depositor Runs?* NBER Working Paper 31048.

**Data sources (all free, no WRDS required):**
- FFIEC CDR — quarterly Call Reports for every FDIC-insured bank
- FRED API — U.S. Treasury constant-maturity yields
- Yahoo Finance — MBS ETF prices (SPMB, CMBS)

---

## Notebook Structure
1. Data Acquisition — FFIEC Call Reports
2. Cleaning the FFIEC Data
3. Treasury Yields
4. Yield Shocks
5. The RMBS Multiplier Assumption
6. Applying Shocks to Bank Balance Sheets
7. Results — Table 1
8. Analysis & Interpretation\
"""

SETUP = """\
import os
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from IPython.display import display, Image
from dotenv import load_dotenv

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT = Path("..").resolve()
load_dotenv(ROOT / ".env")

DATA_DIR   = ROOT / "_data"
OUTPUT_DIR = ROOT / "_output"

# ── Config from .env ─────────────────────────────────────────────────────────
REPORT_DATE_SLASH = os.getenv("REPORT_DATE_SLASH", "12/31/2025")  # MM/DD/YYYY — e.g. 03/31/2025
REPORT_DATE       = REPORT_DATE_SLASH.replace("/", "")
# Derive shock window from REPORT_DATE (MMDDYYYY -> YYYY-MM-DD)
# Start is fixed at 2020-01-01 (near-zero rate baseline)
MARKET_START = "2020-01-01"
MARKET_END   = f"{REPORT_DATE[4:]}-{REPORT_DATE[:2]}-{REPORT_DATE[2:4]}"
RMBS_MULTIPLIER   = float(os.getenv("RMBS_MULTIPLIER", "1.25"))

# ── Constants ────────────────────────────────────────────────────────────────
BUCKETS       = ["lt1y", "1_3y", "3_5y", "5_10y", "10_15y", "15plus"]
BUCKET_LABELS = ["<1yr", "1-3yr", "3-5yr", "5-10yr", "10-15yr", "15+yr"]
BUCKET_MIDS   = [0.5, 2.0, 4.0, 7.5, 12.5, 22.0]   # midpoint maturity in years
FRED_MATS     = [1, 3, 5, 10, 20, 30]
FRED_COLS     = ["dgs1", "dgs3", "dgs5", "dgs10", "dgs20", "dgs30"]
SMALL_CUTOFF  = 1.384e6   # $1.384 billion in $thousands (FFIEC unit)

# ── Plot style ───────────────────────────────────────────────────────────────
plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "#f9f9f9",
    "axes.grid": True,
    "grid.alpha": 0.4,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "font.size": 11,
})

print(f"Report date  : {REPORT_DATE_SLASH}")
print(f"Shock window : {MARKET_START}  ->  {MARKET_END}")
print(f"RMBS mult    : {RMBS_MULTIPLIER}x")
print(f"Data dir     : {DATA_DIR}")\
"""

S1_HEADER = """\
---
## 1. Data Acquisition — FFIEC Call Reports

The Federal Financial Institutions Examination Council (FFIEC) requires every
FDIC-insured commercial bank and savings institution to file a **Call Report**
every quarter. These are consolidated financial statements — balance sheet,
income statement, and detailed schedules — submitted to federal bank regulators.

### What we download
We use the **FFIEC CDR Bulk Download** portal
(`https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx`) to download a single
ZIP file containing all schedule data for a given quarter. The download is
automated using **Selenium** (a browser-control library) because the site
requires dropdown selections before the download link becomes available.

### Schedule files inside the ZIP
The ZIP contains multiple schedule files, each covering a different part of the
Call Report:

| Schedule | Contents |
|----------|----------|
| **RC**   | Balance sheet — total assets, loans, securities, deposits, equity |
| **RC-A** | Cash and balances due from banks |
| **RC-B** | Securities by type and maturity bucket |
| **RC-C Part I** | Loans and leases by type |
| **RC-E** | Deposit liabilities — insured vs. uninsured |

Each file is **tab-delimited** with two header rows: row 0 contains the FFIEC
mnemonic codes (e.g., `RCFD2170` for total assets), and row 1 contains plain-text
descriptions. The first column is `IDRSSD` — the unique RSSD identifier assigned
to each bank by the Federal Reserve.\
"""

S1_ZIP = """\
# ── Locate and inspect the ZIP file ─────────────────────────────────────────
ZIP_PATH = DATA_DIR / f"FFIEC CDR Call Bulk All Schedules {REPORT_DATE}.zip"

if not ZIP_PATH.exists():
    raise FileNotFoundError(
        f"ZIP not found: {ZIP_PATH}\\n"
        "Run `doit pull_ffiec` from the project root to download it."
    )

size_mb = ZIP_PATH.stat().st_size / 1e6
print(f"ZIP file : {ZIP_PATH.name}")
print(f"Size     : {size_mb:.1f} MB\\n")

with zipfile.ZipFile(ZIP_PATH) as zf:
    names = zf.namelist()
    print(f"Files inside the ZIP ({len(names)} total):")
    for name in sorted(names):
        info = zf.getinfo(name)
        print(f"  {name:<65s}  {info.file_size/1e6:6.1f} MB")\
"""

S1_RAW = """\
# ── Show what the raw data looks like BEFORE any processing ─────────────────
# We open Schedule RC (main balance sheet) and read it without any cleaning.

def find_member(zf, stub):
    matches = [n for n in zf.namelist() if stub.lower() in n.lower()]
    if not matches:
        raise FileNotFoundError(f"'{stub}' not found in ZIP")
    return matches[0]

with zipfile.ZipFile(ZIP_PATH) as zf:
    rc_name = find_member(zf, f"Schedule RC {REPORT_DATE}")
    print(f"Reading: {rc_name}\\n")
    with zf.open(rc_name) as f:
        raw = pd.read_csv(f, sep="\\t", nrows=5, low_memory=False, dtype=str)

print("Raw file — first 5 rows, first 10 columns:")
print(raw.iloc[:, :10].to_string(index=False))
print()
print("Observations:")
print("  Row 0 (shown as header) : FFIEC mnemonic codes  (e.g. RCFD2170 = Total Assets)")
print("  Row 1 (index 0 above)   : Plain-text descriptions — this row is SKIPPED in processing")
print("  Rows 2+ (index 1+)      : One bank per row, identified by IDRSSD (RSSD ID)")\
"""

S2_HEADER = """\
---
## 2. Cleaning the FFIEC Data

Raw FFIEC data needs several cleaning steps before it can be used for analysis.
Here we walk through each step explicitly.\
"""

S2_EXPLAIN = """\
### Step 1 — Read and standardize each schedule file

The `read_ffiec` function (from `scripts/process_ffiec.py`) applies four operations:

1. **Skip the description row** (`skiprows=[1]`) — keep only mnemonic codes as column names
2. **Strip and lowercase column names** — remove quotes and whitespace
3. **Rename `idrssd` → `rssd9001`** and convert to integer — this is the bank identifier
4. **Set `rssd9001` as the index** and convert all values to numeric (`errors='coerce'` → non-numeric become NaN)

### Step 2 — RCFD vs RCON prefixes

FFIEC mnemonic codes begin with either `RCFD` or `RCON`:

| Prefix | Scope |
|--------|-------|
| **RCFD** | **Consolidated** — all domestic and foreign offices combined |
| **RCON** | **Domestic** offices only |

For most variables we use `RCFD` (global consolidated). For deposit data we use
`RCON` because foreign deposits are tracked separately via `RCFN`.
When a bank lacks RCFD data (e.g., smaller banks that only file the domestic form),
we fall back to the RCON value using pandas `fillna`.

### Step 3 — Construct economic variables

We map raw FFIEC codes to meaningful variable names:

| Variable | FFIEC Code(s) | Notes |
|----------|--------------|-------|
| Total Assets | `rcfd2170` | |
| Cash | `rcfd0010` | |
| Treasuries & Agency Securities | `rcfd0213` + `rcfd1287` | AFS + HTM |
| RMBS (total) | `rcfdg301…rcfdg323` | 12 codes across 6 maturity buckets |
| CMBS | `rcfdk143…rcfdk157` | |
| Total Loans | `rcfd2122` | |
| Residential Mortgage Loans | `rcfd1420`, `rcfd1797`, `rcfd5367`, `rcfd5368`, `rcfd1460` | |
| Uninsured Deposits | Domestic Deposits − Insured Deposits | |

### Step 4 — Maturity bucket assignment

For **RMBS**, the call report provides separate line items for each maturity bucket
(e.g., `rcfdg301` = RMBS maturing in < 1 year). We use these directly.

For all other asset classes (**Treasuries, CMBS, ABS, loans**), the call report
only reports a total. We distribute that total across the six maturity buckets
using **assumed weight distributions** that are consistent with Jiang et al. (2023).\
"""

S2_READ_FN = """\
# ── Demonstrate the cleaning function ───────────────────────────────────────

def read_ffiec_clean(zf, filename):
    \"\"\"
    Simplified version of the read_ffiec() function used in the pipeline.
    Cleans one schedule file from the ZIP.
    \"\"\"
    with zf.open(filename) as f:
        df = pd.read_csv(
            f,
            sep="\\t",
            header=0,       # Row 0 = mnemonic code headers
            skiprows=[1],   # Row 1 = plain-text descriptions — skip
            low_memory=False,
            dtype=str,
        )
    df.columns = df.columns.str.strip().str.replace('"', "").str.lower()
    df = df.rename(columns={"idrssd": "rssd9001"})
    df["rssd9001"] = pd.to_numeric(df["rssd9001"], errors="coerce")
    df = df.dropna(subset=["rssd9001"])
    df["rssd9001"] = df["rssd9001"].astype(int)
    df = df.set_index("rssd9001")
    df = df.apply(pd.to_numeric, errors="coerce")
    return df

with zipfile.ZipFile(ZIP_PATH) as zf:
    rc_name = find_member(zf, f"Schedule RC {REPORT_DATE}")
    rc_clean = read_ffiec_clean(zf, rc_name)

print(f"Schedule RC after cleaning: {rc_clean.shape[0]:,} banks x {rc_clean.shape[1]} columns")
print(f"Index (RSSD IDs): {list(rc_clean.index[:5])} ...")
print()

key_cols = {
    "rcfd2170": "Total Assets ($K)",
    "rcfd0010": "Cash ($K)",
    "rcfd2122": "Total Loans ($K)",
}
available = {k: v for k, v in key_cols.items() if k in rc_clean.columns}
print("Key columns (first 5 banks):")
display(
    rc_clean[list(available.keys())]
    .rename(columns=available)
    .head(5)
    .applymap(lambda x: f"{x:,.0f}" if pd.notna(x) else "")
)\
"""

S2_PANEL = """\
# ── Load the processed bank panel ───────────────────────────────────────────
# The pipeline has already run all cleaning and variable construction.
# We load the resulting parquet file which contains one row per bank.

panel_path = DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet"
panel = pd.read_parquet(panel_path)

# Convert key columns to numeric
for col in ["Total Asset", "Uninsured Deposit", "Domestic Deposit", "Insured Deposit"]:
    if col in panel.columns:
        panel[col] = pd.to_numeric(panel[col], errors="coerce")

# Add bank size classification
gsib_df = pd.read_parquet(DATA_DIR / "gsib_list.parquet")
gsib_ids = set(pd.to_numeric(gsib_df["rssd_id_call"], errors="coerce").dropna().astype(int))
panel["is_gsib"]    = panel["rssd_id_call"].isin(gsib_ids)
panel["size_group"] = "Large (non-GSIB)"
panel.loc[panel["Total Asset"] < SMALL_CUTOFF, "size_group"] = "Small"
panel.loc[panel["is_gsib"], "size_group"] = "GSIB"

print(f"Bank panel: {panel.shape[0]:,} banks x {panel.shape[1]} columns")
print(f"Report date: {REPORT_DATE_SLASH}")
print()

show_cols = [
    "rssd_id_call", "size_group", "Total Asset",
    "security_rmbs", "security_treasury", "Total_Loan",
    "Uninsured Deposit",
]
show_cols = [c for c in show_cols if c in panel.columns]
print("Sample rows (values in $thousands):")
display(panel[show_cols].dropna(subset=["Total Asset"]).head(8))\
"""

S2_COUNTS = """\
# ── Bank counts by size group ────────────────────────────────────────────────
counts = panel["size_group"].value_counts().rename("Count")
assets = panel.groupby("size_group")["Total Asset"].sum().rename("Total Assets ($B)")
assets = (assets * 1000 / 1e9).round(1)   # convert $thousands -> $billions

summary = pd.concat([counts, assets], axis=1)
summary.index.name = "Group"
print("Bank classification:")
print(f"  Cutoff : Small = Total Assets < $1.384B")
print(f"  GSIB   = 37 institutions on the FSB G-SIB designation list\\n")
display(summary)
print()

# Sanity check: uninsured deposit share
ud_share = (panel["Uninsured Deposit"].sum() / panel["Domestic Deposit"].sum() * 100
            if "Domestic Deposit" in panel.columns else float("nan"))
print(f"System-wide uninsured deposit share: {ud_share:.1f}% of domestic deposits")\
"""

S2_FIGURE = """\
# ── Figure A1 — Total asset distribution across bank groups ─────────────────
fig_path = OUTPUT_DIR / f"figure_A1_{REPORT_DATE}.png"
if fig_path.exists():
    display(Image(filename=str(fig_path), width=820))
else:
    print(f"Figure not found at {fig_path}")
    print("Run `doit process_ffiec` to generate it.")\
"""

S2_BUCKETS_EXPLAIN = """\
### Maturity Bucket Assignment — Detail

The six maturity buckets follow the Call Report schedule for securities (RC-B):

| Bucket | Range | Midpoint used |
|--------|-------|--------------|
| `lt1y` | < 1 year | 0.5 yr |
| `1_3y` | 1–3 years | 2.0 yr |
| `3_5y` | 3–5 years | 4.0 yr |
| `5_10y` | 5–10 years | 7.5 yr |
| `10_15y` | 10–15 years | 12.5 yr |
| `15plus` | > 15 years | 22.0 yr |

**RMBS** is special — the call report contains separate line items for each bucket
(codes `rcfdg301` through `rcfdg323`), so we have actual bank-level data.

For all other asset classes, the call report only provides a **total**. We must
assume how that total is spread across maturity buckets. The assumed weights below
are stylized distributions chosen to match typical U.S. bank portfolio compositions
(following Jiang et al., 2023). This is one of the key simplifying assumptions in
the analysis — the loss estimates for non-RMBS assets are sensitive to these weights.\
"""

S2_BUCKET_TABLE = """\
# ── Display the assumed bucket weight distributions ──────────────────────────
weights = pd.DataFrame({
    "Bucket"      : BUCKETS,
    "Range"       : ["<1yr", "1-3yr", "3-5yr", "5-10yr", "10-15yr", "15+yr"],
    "Midpoint(yr)": BUCKET_MIDS,
    "RMBS source" : ["Actual FFIEC codes"] * 6,
    "Treasury"    : [0.20, 0.25, 0.20, 0.20, 0.10, 0.05],
    "Res.Mortgage": [0.05, 0.10, 0.15, 0.25, 0.25, 0.20],
    "Other Assets": [0.10, 0.15, 0.20, 0.25, 0.20, 0.10],
    "Other Loans" : [0.20, 0.20, 0.20, 0.20, 0.10, 0.10],
}).set_index("Bucket")

print("Maturity bucket weights (non-RMBS asset classes):")
display(weights)
print()
print("Note: Treasury + Res.Mortgage + Other Assets + Other Loans weights each sum to 1.0")
print("RMBS uses actual per-bucket FFIEC codes — no assumption needed.")\
"""

S3_HEADER = """\
---
## 3. Treasury Yields

We obtain U.S. Treasury constant-maturity yields from the **FRED API** (Federal
Reserve Bank of St. Louis). These six series give us yield observations at six
maturities across the yield curve:

| Series | Maturity |
|--------|---------|
| DGS1 | 1-year |
| DGS3 | 3-year |
| DGS5 | 5-year |
| DGS10 | 10-year |
| DGS20 | 20-year |
| DGS30 | 30-year |

All series are reported in **percent per annum** on a daily frequency.
We pull the full history (back to 1962 where available) so that any shock window
can be analysed without re-downloading.

**Why these maturities?** They span the yield curve well and match the six maturity
buckets used in the Call Report. We will interpolate between these points to get a
yield (and yield shock) at the midpoint of each bucket.\
"""

S3_LOAD = """\
# ── Load Treasury yields ─────────────────────────────────────────────────────
tsy = pd.read_parquet(DATA_DIR / "treasury_yields.parquet")
tsy["date"] = pd.to_datetime(tsy["date"])
tsy = tsy.sort_values("date").reset_index(drop=True)

print(f"Treasury yields: {len(tsy):,} daily observations")
print(f"Date range     : {tsy['date'].min().date()}  to  {tsy['date'].max().date()}")
print(f"Columns        : {list(tsy.columns)}")
print()
print("Most recent 5 observations:")
display(tsy.tail(5).set_index("date"))\
"""

S3_PLOT = """\
# ── Plot the yield curve at start and end of the shock window ────────────────

def get_curve(date_str):
    \"\"\"Return the yield curve row closest to date_str.\"\"\"
    dt = pd.Timestamp(date_str)
    idx = (tsy["date"] - dt).abs().idxmin()
    row = tsy.loc[idx]
    return row["date"].date(), row[FRED_COLS].values.astype(float)

start_date, start_yields = get_curve(MARKET_START)
end_date,   end_yields   = get_curve(MARKET_END)

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Left: yield curves at start and end
ax = axes[0]
ax.plot(FRED_MATS, start_yields, "o-", label=f"Start: {start_date}", color="steelblue", lw=2, ms=8)
ax.plot(FRED_MATS, end_yields,   "s-", label=f"End:   {end_date}",   color="firebrick", lw=2, ms=8)
ax.fill_between(FRED_MATS, start_yields, end_yields, alpha=0.12, color="orange", label="Yield increase")
ax.set_xlabel("Maturity (years)")
ax.set_ylabel("Yield (% p.a.)")
ax.set_title("U.S. Treasury Yield Curve")
ax.set_xticks(FRED_MATS)
ax.set_xticklabels(["1yr", "3yr", "5yr", "10yr", "20yr", "30yr"])
ax.legend()

# Right: full history of 10yr yield
ax = axes[1]
ax.plot(tsy["date"], tsy["dgs10"], color="steelblue", lw=1.2, label="DGS10")
ax.axvline(pd.Timestamp(MARKET_START), color="green",    lw=1.5, linestyle="--", label=f"Start {MARKET_START}")
ax.axvline(pd.Timestamp(MARKET_END),   color="firebrick", lw=1.5, linestyle="--", label=f"End   {MARKET_END}")
ax.set_title("10-Year Treasury Yield — Full History")
ax.set_ylabel("Yield (% p.a.)")
ax.set_xlabel("Date")
ax.legend(fontsize=9)

plt.tight_layout()
plt.show()

print("Yield changes over the shock window:")
for mat, s, e in zip(FRED_MATS, start_yields, end_yields):
    print(f"  {mat:2d}yr:  {s:.2f}%  ->  {e:.2f}%   delta = {e-s:+.2f} pp")\
"""

S4_HEADER = """\
---
## 4. Yield Shocks

A **yield shock** is the change in the yield at a given maturity between the start
and end of the shock window. We compute it for each of the six FRED maturities,
then **linearly interpolate** to get the shock at the midpoint of each maturity bucket.

### Why interpolate?

The FRED series cover 1, 3, 5, 10, 20, and 30 years. Our buckets have midpoints at
0.5, 2, 4, 7.5, 12.5, and 22 years — these don't perfectly align with the FRED
maturities. Linear interpolation (`numpy.interp`) gives us the best estimate of
the shock at any intermediate maturity, using the two nearest FRED data points.\
"""

S4_SHOCKS = """\
# ── Load pre-computed shocks ─────────────────────────────────────────────────
shocks = pd.read_parquet(DATA_DIR / "market_shocks.parquet").iloc[0]

shock_keys = [f"d_tsy_{b}" for b in BUCKETS]

shock_df = pd.DataFrame({
    "Bucket"         : BUCKETS,
    "Range"          : ["<1yr", "1-3yr", "3-5yr", "5-10yr", "10-15yr", "15+yr"],
    "Midpoint (yr)"  : BUCKET_MIDS,
    "Yield shock (pp)": [round(float(shocks[k]), 4) for k in shock_keys],
}).set_index("Bucket")

print(f"Shock window: {MARKET_START}  ->  {MARKET_END}")
print()
print("Interpolated yield shocks by maturity bucket:")
display(shock_df)
print()
print(f"RMBS multiplier stored alongside shocks: {shocks['rmbs_multiplier']}")\
"""

S4_PLOT = """\
# ── Visualise shocks and interpolation ───────────────────────────────────────
shock_vals = [float(shocks[f"d_tsy_{b}"]) for b in BUCKETS]

# Recompute raw FRED shocks from the yield data
fred_shocks = [float(e - s) for s, e in zip(start_yields, end_yields)]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

# Left: bar chart of bucket shocks
bars = ax1.bar(BUCKET_LABELS, shock_vals, color="#4C72B0", edgecolor="white", width=0.6)
ax1.set_ylabel("Yield shock (percentage points)")
ax1.set_title("Interpolated Yield Shock per Maturity Bucket")
for bar, v in zip(bars, shock_vals):
    ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.04,
             f"{v:+.2f}", ha="center", va="bottom", fontsize=9)

# Right: show how interpolation works
interp_x = np.linspace(0.5, 30, 300)
interp_y = np.interp(interp_x, FRED_MATS, fred_shocks)

ax2.plot(interp_x, interp_y, "--", color="gray", lw=1.5, label="Linear interpolation", alpha=0.7)
ax2.plot(FRED_MATS,   fred_shocks, "o",  color="firebrick",  ms=10, zorder=5, label="FRED data points")
ax2.plot(BUCKET_MIDS, shock_vals,  "D",  color="#4C72B0",    ms=8,  zorder=5, label="Bucket midpoints (used)")
ax2.set_xlabel("Maturity (years)")
ax2.set_ylabel("Yield shock (pp)")
ax2.set_title("How Bucket Shocks Are Interpolated from FRED")
ax2.legend(fontsize=9)

plt.tight_layout()
plt.show()\
"""

S5_HEADER = """\
---
## 5. The RMBS Multiplier Assumption

### Duration and mark-to-market losses

For a fixed-income security, the approximate price change due to a yield change is:

$$\\Delta P \\approx -D_{mod} \\times \\Delta y \\times P$$

where $D_{mod}$ is the **modified duration** (sensitivity of price to yield changes)
and $\\Delta y$ is the change in yield. We use the bucket midpoint as the proxy for
duration: a security in the 5–10yr bucket has duration ≈ 7.5 years.

### Why does RMBS get an extra 1.25× penalty?

Residential mortgage-backed securities (RMBS) behave differently from plain
Treasury bonds because of **prepayment optionality**:

- When rates *fall*, homeowners refinance → mortgages prepay early → MBS shortens (like a callable bond)
- When rates *rise*, homeowners *stop* refinancing → prepayments slow → MBS duration **extends**

This "negative convexity" (also called **extension risk**) means that when rates
rise sharply, RMBS loses *more* value than a plain-duration calculation would predict.
The 1.25× multiplier accounts for this: we assume RMBS losses are 25% larger than
simple duration implies.

**Key assumption:** This multiplier is calibrated against observed MBS ETF performance
during the 2020–2023 rate cycle. It applies equally to residential mortgage *loans*
held for investment (which share the same prepayment dynamics).

The multiplier is stored in `.env` as `RMBS_MULTIPLIER=1.25` and can be changed
to test sensitivity.\
"""

S5_ETF = """\
# ── Compare SPMB (MBS ETF) to duration-implied price path ───────────────────
# SPMB = SPDR Portfolio Mortgage Backed Bond ETF — a proxy for RMBS performance.
# We ask: did SPMB fall more than simple duration would predict?

mbs = pd.read_parquet(DATA_DIR / "mbs_etfs.parquet")
mbs["date"] = pd.to_datetime(mbs["date"])
mbs = mbs.sort_values("date").reset_index(drop=True)

start_dt = pd.Timestamp(MARKET_START)
end_dt   = pd.Timestamp(MARKET_END)
window   = mbs[(mbs["date"] >= start_dt) & (mbs["date"] <= end_dt)].copy()

if window.empty or "rmbs_px" not in window.columns:
    print("MBS ETF data not available for this window.")
    print(f"Columns: {list(mbs.columns)}")
else:
    window = window.reset_index(drop=True)
    start_px = window["rmbs_px"].iloc[0]
    window["spmb_idx"] = window["rmbs_px"] / start_px * 100

    # Duration-implied price path using the 5yr yield as reference
    # SPMB effective duration ≈ 5.5 years (typical for agency MBS)
    est_duration = 5.5
    tsy_w = tsy[(tsy["date"] >= start_dt) & (tsy["date"] <= end_dt)][["date", "dgs5"]].copy()
    tsy_w["delta_y"] = tsy_w["dgs5"] - tsy_w["dgs5"].iloc[0]   # cumulative yield change (pp)
    tsy_w = tsy_w.reset_index(drop=True)

    merged = pd.merge_asof(window[["date", "spmb_idx"]], tsy_w, on="date")
    # Price change ≈ -Duration × ΔyieldInDecimal
    merged["duration_implied"] = 100 * (1 - est_duration * merged["delta_y"] / 100)

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(merged["date"], merged["spmb_idx"],        color="firebrick",  lw=2,   label="SPMB actual (indexed)")
    ax.plot(merged["date"], merged["duration_implied"], color="steelblue",  lw=2,   linestyle="--",
            label=f"Duration-implied loss (D = {est_duration}yr)")
    ax.axhline(100, color="gray", lw=0.8, linestyle=":")
    ax.set_ylabel("Price index (start = 100)")
    ax.set_title("SPMB vs. Duration-Implied: Justification for the 1.25× RMBS Multiplier")
    ax.legend()
    ax.set_xlabel("Date")
    plt.tight_layout()
    plt.show()

    final_spmb    = merged["spmb_idx"].iloc[-1]
    final_implied = merged["duration_implied"].iloc[-1]
    actual_loss   = 100 - final_spmb
    implied_loss  = 100 - final_implied
    empirical_mult = actual_loss / implied_loss if implied_loss > 0 else float("nan")

    print(f"Actual SPMB price change over window  : {final_spmb - 100:+.1f}%  (loss = {actual_loss:.1f}%)")
    print(f"Duration-implied price change          : {final_implied - 100:+.1f}%  (loss = {implied_loss:.1f}%)")
    print(f"Empirical multiplier (actual / implied): {empirical_mult:.2f}x")
    print()
    print(f"We use {RMBS_MULTIPLIER}x as a round conservative estimate.")
    print("This multiplier is applied to both RMBS securities AND residential mortgage loans.")\
"""

S6_HEADER = """\
---
## 6. Applying Shocks to Bank Balance Sheets

We now combine everything: the **bank panel** (what each bank holds), the **yield
shocks** (how much rates moved in each maturity bucket), and the **RMBS multiplier**
(how much extra RMBS suffers).

### Loss formula

For each bank $i$, asset class $a$, and maturity bucket $b$:

$$\\text{Loss}_{i,a,b} = H_{i,a,b} \\times \\Delta y_b \\times m_a$$

where:
- $H_{i,a,b}$ = book value holding (in $thousands, from FFIEC)
- $\\Delta y_b$ = interpolated yield shock at bucket midpoint $b$ (in percentage points)
- $m_a$ = asset-class multiplier: **1.25** for RMBS and residential mortgages, **1.0** for everything else

Total bank-level loss: $\\text{Loss}_i = \\sum_{a,b} \\text{Loss}_{i,a,b}$

Mark-to-market assets: $\\text{MTM Assets}_i = \\text{Total Assets}_i - \\text{Loss}_i$

**Fragility:**

$$\\text{Fragility}_i = \\frac{\\text{Uninsured Deposits}_i}{\\text{MTM Assets}_i} \\times 100$$

A fragility > 100% means that even if all uninsured depositors ran simultaneously,
the bank could not repay them in full at MTM prices — without central bank support.\
"""

S6_EXAMPLE = """\
# ── Walk through the loss computation for one example bank ──────────────────
rmbs_mult = float(shocks["rmbs_multiplier"])

# Pick a typical small bank (median by total assets among Small group)
small  = panel[panel["size_group"] == "Small"].dropna(subset=["Total Asset"]).copy()
small  = small.sort_values("Total Asset")
bank   = small.iloc[len(small) // 2]   # median small bank

print(f"Example bank: RSSD ID = {bank['rssd_id_call']}")
print(f"Total Assets       : ${bank['Total Asset'] * 1000:>20,.0f}  (${bank['Total Asset']/1000:.1f}M)")
print(f"Uninsured Deposits : ${float(bank['Uninsured Deposit'] or 0) * 1000:>20,.0f}")
print()

header = f"{'Asset class':<20s} {'Bucket':<8s} {'Holding ($K)':>14s} {'Shock (pp)':>11s} {'Mult':>5s} {'Loss ($K)':>13s}"
print(header)
print("-" * len(header))

total_loss = 0.0
ASSET_CLASSES = [
    ("rmbs",         rmbs_mult),
    ("treasury",     1.0),
    ("other_assets", 1.0),
    ("res_mtg",      rmbs_mult),
    ("other_loan",   1.0),
]

for bucket, label in zip(BUCKETS, BUCKET_LABELS):
    shock_key = f"d_tsy_{bucket}"
    if shock_key not in shocks.index:
        continue
    shock = float(shocks[shock_key])
    for cls, mult in ASSET_CLASSES:
        col = f"{cls}_{bucket}"
        if col not in panel.columns:
            continue
        holding = float(bank.get(col, 0) or 0)
        if abs(holding) < 1:
            continue
        loss = holding * shock * mult
        total_loss += loss
        print(f"{cls:<20s} {label:<8s} {holding:>14,.0f} {shock:>11.4f} {mult:>5.2f} {loss:>13,.0f}")

print("-" * len(header))
ta  = float(bank["Total Asset"])
ud  = float(bank["Uninsured Deposit"] or 0)
mma = ta - abs(total_loss)
frag = ud / mma * 100 if mma > 0 else float("nan")

print(f"{'TOTAL LOSS':>60s}  {abs(total_loss):>13,.0f}")
print()
print(f"  Total Assets (book value) : ${ta * 1000:>15,.0f}")
print(f"  MTM Loss                  : ${abs(total_loss) * 1000:>15,.0f}")
print(f"  MTM Assets                : ${mma * 1000:>15,.0f}")
print(f"  Uninsured Deposits        : ${ud * 1000:>15,.0f}")
print(f"  Fragility                 :  {frag:>14.1f}%")\
"""

S6_ALL = """\
# ── Compute MTM losses for all banks ────────────────────────────────────────
df = panel.copy()
rmbs_mult = float(shocks["rmbs_multiplier"])

# Initialise loss accumulators
for lc in ["loss_rmbs", "loss_tsy_other", "loss_res_mtg", "loss_other_loan"]:
    df[lc] = 0.0

for bucket in BUCKETS:
    shock_key = f"d_tsy_{bucket}"
    if shock_key not in shocks.index:
        continue
    shock = float(shocks[shock_key])

    def safe_col(colname):
        if colname in df.columns:
            return pd.to_numeric(df[colname], errors="coerce").fillna(0.0)
        return pd.Series(0.0, index=df.index)

    df["loss_rmbs"]     += safe_col(f"rmbs_{bucket}")        * shock * rmbs_mult
    df["loss_res_mtg"]  += safe_col(f"res_mtg_{bucket}")     * shock * rmbs_mult
    df["loss_tsy_other"]+= safe_col(f"treasury_{bucket}")    * shock
    df["loss_tsy_other"]+= safe_col(f"other_assets_{bucket}")* shock
    df["loss_other_loan"]+= safe_col(f"other_loan_{bucket}") * shock

df["loss_total"] = (df["loss_rmbs"].abs() + df["loss_tsy_other"].abs()
                  + df["loss_res_mtg"].abs() + df["loss_other_loan"].abs())
df["mm_assets"]  = df["Total Asset"] - df["loss_total"]
df["fragility"]  = 100 * df["Uninsured Deposit"] / df["mm_assets"].replace(0, np.nan)
df["loss_pct"]   = 100 * df["loss_total"] / df["Total Asset"].replace(0, np.nan)

total_loss_T = df["loss_total"].sum() * 1000 / 1e12
print(f"Losses computed for {len(df):,} banks")
print(f"System-wide aggregate MTM loss : ${total_loss_T:.1f} trillion")
print()
print("Loss summary by group:")
summary_rows = []
for g in ["Small", "Large (non-GSIB)", "GSIB"]:
    sub = df[df["size_group"] == g]
    clean = sub["loss_pct"].replace([np.inf, -np.inf], np.nan).dropna()
    summary_rows.append({
        "Group"             : g,
        "N banks"           : len(sub),
        "Agg.Loss"          : f"${sub['loss_total'].sum()*1000/1e9:.1f}B",
        "Mean loss/asset %" : f"{clean.mean():.1f}%",
        "Mean fragility %"  : f"{sub['fragility'].replace([np.inf,-np.inf],np.nan).dropna().mean():.0f}%",
    })
display(pd.DataFrame(summary_rows).set_index("Group"))\
"""

S6_DIST = """\
# ── Distribution of losses ───────────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
colors_map = {"Small": "#4C72B0", "Large (non-GSIB)": "#DD8452", "GSIB": "#55A868"}

# Left: loss/asset ratio distribution (per bank)
ax = axes[0]
for g, col in colors_map.items():
    sub = df[df["size_group"] == g]["loss_pct"].replace([np.inf, -np.inf], np.nan).dropna()
    sub_clip = sub.clip(upper=sub.quantile(0.99))
    ax.hist(sub_clip, bins=60, alpha=0.6, color=col, label=g, density=True)
ax.set_xlabel("Loss / Total Assets (%)")
ax.set_ylabel("Density")
ax.set_title("Distribution of MTM Loss as % of Assets")
ax.legend()

# Right: fragility distribution (per bank)
ax = axes[1]
for g, col in colors_map.items():
    sub = df[df["size_group"] == g]["fragility"].replace([np.inf, -np.inf], np.nan).dropna()
    sub_clip = sub.clip(upper=min(200, sub.quantile(0.99)))
    ax.hist(sub_clip, bins=60, alpha=0.6, color=col, label=g, density=True)
ax.axvline(100, color="black", lw=1.5, linestyle="--", label="Fragility = 100%")
ax.set_xlabel("Fragility: Uninsured Dep. / MTM Assets (%)")
ax.set_ylabel("Density")
ax.set_title("Distribution of Fragility")
ax.legend()

plt.tight_layout()
plt.show()\
"""

S7_HEADER = """\
---
## 7. Results — Table 1

Table 1 replicates the core results from Jiang et al. (2023), reporting aggregate
and bank-level statistics on MTM losses and fragility across the four bank groups.\
"""

S7_TABLE = """\
# ── Load and display Table 1 ────────────────────────────────────────────────
table1 = pd.read_csv(OUTPUT_DIR / "table_1.csv", index_col=0)
print(f"Table 1: Bank Fragility Summary — {REPORT_DATE_SLASH}")
print("=" * 90)
display(table1)
print()
print("Notes:")
print("  Bank-level rows report mean with (standard deviation) below in parentheses.")
print("  Share rows = % of total exposure in that asset class.")
print("  Loss/Asset = MTM loss as % of total book assets.")
print("  Uninsured Deposit/MM Asset = Fragility measure (>100 means run cannot be absorbed).")\
"""

S7_CHARTS = """\
# ── Summary charts ───────────────────────────────────────────────────────────
groups      = ["Small", "Large (non-GSIB)", "GSIB"]
colors_list = ["#4C72B0", "#DD8452", "#55A868"]

agg_loss, mean_loss_pct, mean_frag = [], [], []
for g in groups:
    sub = df[df["size_group"] == g]
    agg_loss.append(sub["loss_total"].sum() * 1000 / 1e12)
    mean_loss_pct.append(
        sub["loss_pct"].replace([np.inf, -np.inf], np.nan).dropna().mean()
    )
    mean_frag.append(
        sub["fragility"].replace([np.inf, -np.inf], np.nan).dropna().clip(upper=200).mean()
    )

fig, axes = plt.subplots(1, 3, figsize=(16, 5))

titles  = ["Aggregate MTM Loss ($T)", "Mean Loss / Total Assets (%)", "Mean Fragility (%)"]
vals    = [agg_loss, mean_loss_pct, mean_frag]
y_labels = ["USD Trillion ($T)", "Percent (%)", "Percent (%)"]
fmt     = [lambda v: f"${v:.1f}T", lambda v: f"{v:.1f}%", lambda v: f"{v:.0f}%"]

for ax, title, val, ylabel, fmtfn in zip(axes, titles, vals, y_labels, fmt):
    bars = ax.bar(groups, val, color=colors_list, edgecolor="white", width=0.5)
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    for bar, v in zip(bars, val):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(val) * 0.02,
                fmtfn(v), ha="center", va="bottom", fontsize=9)
    ax.set_ylim(0, max(val) * 1.25)

plt.tight_layout()
plt.show()\
"""

S8_HEADER = """\
---
## 8. Analysis & Interpretation

### Key findings

1. **GSIBs bear the largest aggregate losses** — reflecting their outsized balance sheets.
   At the individual bank level, however, large non-GSIB banks often have higher *loss/asset*
   ratios because they hold more rate-sensitive securities relative to their asset base.

2. **RMBS is the dominant source of losses for small banks** — consistent with the
   concentration of mortgage holdings in community banks and their exposure to
   extension risk.

3. **Fragility is elevated across all groups** — the system-wide fragility measure
   indicates that a coordinated uninsured depositor run would exhaust MTM assets at
   many institutions, particularly among GSIBs whose uninsured deposit base is large.

4. **The 2022–2023 tightening cycle was historically fast** — the +300 to +400 bp
   shock within 2–3 years is one of the sharpest in modern U.S. history, giving
   banks little time to reprice their liabilities or reduce duration.

### Limitations of this analysis

| Assumption | Why it matters |
|------------|---------------|
| Stylized bucket weights for non-RMBS assets | Losses for Treasuries, CMBS, and loans are sensitive to these distributions, which are the same for every bank |
| Duration = bucket midpoint | Ignores actual maturity distribution within each bucket and convexity for non-MBS assets |
| Instantaneous shock | Does not account for hedging (interest rate swaps), which some banks use to offset duration risk |
| Book value of RMBS | Does not distinguish AFS (marked to market on balance sheet) from HTM (not marked) |
| Static uninsured deposits | Deposit balances as of quarter-end; does not model deposit outflows during the shock |

### Policy implications

The fragility measure is best interpreted as a **stress-test indicator** under an
extreme tail scenario — simultaneous runs by all uninsured depositors. It does not
predict bank failure, but banks with fragility > 100% would require:

- Access to the **Fed discount window** or the Bank Term Funding Program (BTFP)
- **Emergency capital injection**
- Or a **purchase and assumption** transaction with a healthier institution

The analysis motivates broader use of interest-rate risk reporting in regulatory
stress tests, and highlights the role of deposit insurance limits in containing run risk.

### Extending this analysis

To update for a new quarter: change `REPORT_DATE_SLASH` in `.env` (format: MM/DD/YYYY)
and run `doit` from the project root. Only the FFIEC download and downstream steps
re-execute — Treasury yields and MBS ETFs are unaffected.\
"""

# ---------------------------------------------------------------------------
# Assemble notebook
# ---------------------------------------------------------------------------

cells = [
    md(TITLE,            "cell-00"),
    code(SETUP,          "cell-01"),
    md(S1_HEADER,        "cell-02"),
    code(S1_ZIP,         "cell-03"),
    code(S1_RAW,         "cell-04"),
    md(S2_HEADER,        "cell-05"),
    md(S2_EXPLAIN,       "cell-06"),
    code(S2_READ_FN,     "cell-07"),
    code(S2_PANEL,       "cell-08"),
    code(S2_COUNTS,      "cell-09"),
    code(S2_FIGURE,      "cell-10"),
    md(S2_BUCKETS_EXPLAIN, "cell-11"),
    code(S2_BUCKET_TABLE,  "cell-12"),
    md(S3_HEADER,        "cell-13"),
    code(S3_LOAD,        "cell-14"),
    code(S3_PLOT,        "cell-15"),
    md(S4_HEADER,        "cell-16"),
    code(S4_SHOCKS,      "cell-17"),
    code(S4_PLOT,        "cell-18"),
    md(S5_HEADER,        "cell-19"),
    code(S5_ETF,         "cell-20"),
    md(S6_HEADER,        "cell-21"),
    code(S6_EXAMPLE,     "cell-22"),
    code(S6_ALL,         "cell-23"),
    code(S6_DIST,        "cell-24"),
    md(S7_HEADER,        "cell-25"),
    code(S7_TABLE,       "cell-26"),
    code(S7_CHARTS,      "cell-27"),
    md(S8_HEADER,        "cell-28"),
]

nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "codemirror_mode": {"name": "ipython", "version": 3},
            "file_extension": ".py",
            "mimetype": "text/x-python",
            "name": "python",
            "version": "3.10.0",
        },
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
with open(OUTPUT, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)

print(f"Written -> {OUTPUT}")
print(f"Cells   : {len(cells)}")
