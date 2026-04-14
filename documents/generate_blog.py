"""
Generates documents/bank_fragility_blog.html by combining three notebooks:
  - eda.ipynb                   (raw data exploration)
  - methodology_notebook.ipynb  (processing steps)
  - analysis_notebook.ipynb     (final results)

All code cells are stripped. Outputs (charts, tables) are kept.
Code cells are replaced with plain-English prose from the PROSE dictionaries.
"""

import json
import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Markdown renderers
# ---------------------------------------------------------------------------

def md_inline(text):
    """Convert inline markdown to HTML — protects code spans and math first."""
    codes = []
    def save_code(m):
        codes.append(m.group(1))
        return f"\x00CODE{len(codes)-1}\x00"
    text = re.sub(r"`(.+?)`", save_code, text)

    maths = []
    def save_math(m):
        maths.append(m.group(0))
        return f"\x00MATH{len(maths)-1}\x00"
    text = re.sub(r"\$\$.+?\$\$", save_math, text, flags=re.DOTALL)
    text = re.sub(r"\$.+?\$",     save_math, text)

    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"\*(.+?)\*",     r"<em>\1</em>", text)
    text = re.sub(r"\[(.+?)\]\((.+?)\)", r'<a href="\2">\1</a>', text)

    for j, raw in enumerate(codes):
        escaped = raw.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        text = text.replace(f"\x00CODE{j}\x00", f"<code>{escaped}</code>")
    for j, raw in enumerate(maths):
        text = text.replace(f"\x00MATH{j}\x00", raw)
    return text


def md_to_html(text):
    """Convert a markdown cell to HTML."""
    lines = text.split("\n")
    out = []
    i = 0
    in_ul = False
    in_ol = False

    def close_lists():
        nonlocal in_ul, in_ol
        if in_ul:
            out.append("</ul>")
            in_ul = False
        if in_ol:
            out.append("</ol>")
            in_ol = False

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if stripped in ("---", "***", "___"):
            close_lists()
            out.append("<hr>")
            i += 1
            continue

        m = re.match(r"^(#{1,4})\s+(.*)", stripped)
        if m:
            close_lists()
            level = len(m.group(1))
            out.append(f"<h{level}>{md_inline(m.group(2))}</h{level}>")
            i += 1
            continue

        if "|" in stripped and stripped.startswith("|"):
            close_lists()
            tbl_lines = []
            while i < len(lines) and "|" in lines[i] and lines[i].strip().startswith("|"):
                tbl_lines.append(lines[i].strip())
                i += 1
            rows = []
            sep_seen = False
            for tl in tbl_lines:
                cells = [c.strip() for c in tl.strip("|").split("|")]
                if all(re.match(r"^[-: ]+$", c) for c in cells if c):
                    sep_seen = True
                    continue
                rows.append((not sep_seen, cells))
            out.append('<div class="table-wrap"><table>')
            header_done = False
            for is_hdr, cells in rows:
                if not header_done:
                    out.append("<thead><tr>")
                    for c in cells:
                        out.append(f"<th>{md_inline(c)}</th>")
                    out.append("</tr></thead><tbody>")
                    header_done = True
                else:
                    out.append("<tr>")
                    for c in cells:
                        out.append(f"<td>{md_inline(c)}</td>")
                    out.append("</tr>")
            out.append("</tbody></table></div>")
            continue

        m = re.match(r"^[-*+]\s+(.*)", stripped)
        if m:
            if not in_ul:
                close_lists()
                out.append("<ul>")
                in_ul = True
            out.append(f"<li>{md_inline(m.group(1))}</li>")
            i += 1
            continue

        m = re.match(r"^\d+\.\s+(.*)", stripped)
        if m:
            if not in_ol:
                close_lists()
                out.append("<ol>")
                in_ol = True
            out.append(f"<li>{md_inline(m.group(1))}</li>")
            i += 1
            continue

        if stripped == "":
            close_lists()
            out.append("")
            i += 1
            continue

        close_lists()
        para_lines = []
        while (
            i < len(lines)
            and lines[i].strip() != ""
            and not lines[i].strip().startswith("#")
            and not lines[i].strip().startswith("|")
            and not re.match(r"^[-*+]\s", lines[i].strip())
            and not re.match(r"^\d+\.\s", lines[i].strip())
            and lines[i].strip() not in ("---", "***", "___")
        ):
            para_lines.append(lines[i].strip())
            i += 1
        para_text = " ".join(para_lines)
        if para_text:
            out.append(f"<p>{md_inline(para_text)}</p>")
        continue

    close_lists()
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def clean_pandas_html(raw_html):
    raw_html = re.sub(r"<style[^>]*>.*?</style>", "", raw_html, flags=re.DOTALL)
    raw_html = re.sub(r' style="[^"]*"', "", raw_html)
    raw_html = re.sub(r' class="[^"]*"', "", raw_html)
    raw_html = re.sub(r' border="\d+"', "", raw_html)
    raw_html = re.sub(r"<th></th>", "<th>#</th>", raw_html)
    return raw_html


def extract_outputs(cell):
    """Return (images_html_list, tables_html_list) from a cell's outputs."""
    images, tables = [], []
    for out in cell.get("outputs", []):
        data = out.get("data", {})
        if "image/png" in data:
            b64 = data["image/png"]
            if isinstance(b64, list):
                b64 = "".join(b64)
            images.append(f'<img src="data:image/png;base64,{b64}" alt="chart">')
        elif "text/html" in data:
            html_src = data["text/html"]
            if isinstance(html_src, list):
                html_src = "".join(html_src)
            tables.append('<div class="table-wrap">' + clean_pandas_html(html_src) + "</div>")
    return images, tables


def render_notebook_section(cells, prose_dict):
    """
    Convert notebook cells to HTML.
    Code cells are replaced by prose from prose_dict (keyed by cell index).
    Outputs (charts, tables) from code cells are always included.
    """
    parts = []
    for i, cell in enumerate(cells):
        ctype = cell["cell_type"]
        source = "".join(cell["source"])

        if ctype == "markdown":
            parts.append(md_to_html(source))

        elif ctype == "code":
            prose = prose_dict.get(i, "")
            images, tables = extract_outputs(cell)

            if not prose and not images and not tables:
                continue

            if prose:
                parts.append(prose)
            for tbl in tables:
                parts.append(tbl)
            for img in images:
                parts.append(img)

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT          = Path(__file__).resolve().parent.parent
DOCUMENTS     = ROOT / "documents"
EDA_PATH      = DOCUMENTS / "eda.ipynb"
METHOD_PATH   = DOCUMENTS / "methodology_notebook.ipynb"
ANALYSIS_PATH = DOCUMENTS / "analysis_notebook.ipynb"
OUT           = DOCUMENTS / "bank_fragility_blog.html"

# ---------------------------------------------------------------------------
# Load notebooks
# ---------------------------------------------------------------------------

with open(EDA_PATH, encoding="utf-8") as f:
    eda_nb = json.load(f)

with open(METHOD_PATH, encoding="utf-8") as f:
    method_nb = json.load(f)

with open(ANALYSIS_PATH, encoding="utf-8") as f:
    analysis_nb = json.load(f)

eda_cells      = eda_nb["cells"]
method_cells   = method_nb["cells"]
analysis_cells = analysis_nb["cells"]

# ---------------------------------------------------------------------------
# Prose dictionaries — plain English captions for each code cell
# Keys are cell indices within each notebook
# ---------------------------------------------------------------------------

EDA_PROSE = {
    # Cell 1 — imports/setup
    1: "",

    # Cell 3 — GSIB shape/dtypes
    3: """
<p>The GSIB list contains 37 institutions identified by the Financial Stability Board as
Globally Systemically Important Banks. Each is keyed by its RSSD ID —
the unique identifier assigned by the Federal Reserve to every bank holding company
and subsidiary in the United States.</p>
""",

    # Cell 4 — GSIB missing values
    4: "",

    # Cell 6 — MBS ETF shape
    6: """
<p>MBS ETF price data is sourced from Yahoo Finance. We use MBB (iShares MBS ETF)
as the primary MBS benchmark alongside six Treasury ETFs spanning the maturity spectrum.
The dataset covers daily prices from the start of 2020 through the report date.</p>
""",

    # Cell 7 — date range / stats
    7: "",

    # Cell 8 — MBS ETF chart
    8: """
<p>The chart below shows MBB's price path alongside the long-end Treasury ETF (TLT)
since the start of 2020. The steep decline mirrors the Federal Reserve's rate-hiking
cycle that began in March 2022. When yields rise, fixed-income prices fall — and
mortgage-backed securities fell further than simple duration math would suggest,
due to extension risk.</p>
""",

    # Cell 10 — treasury shape
    10: """
<p>Treasury yield data is pulled from FRED using six constant-maturity series:
1-year, 3-year, 5-year, 10-year, 20-year, and 30-year yields.
Daily observations span from January 2020 through the report date.</p>
""",

    # Cell 11 — date range / missing
    11: "",

    # Cell 12 — yield time series
    12: """
<p>The chart below shows how all six Treasury yields moved from January 2020 onward.
The near-zero rates of 2020–2021 gave way to a rapid rise starting in early 2022,
with shorter maturities increasing fastest in the early stages of the hiking cycle.</p>
""",

    # Cell 13 — latest yield curve
    13: """
<p>The snapshot below shows the current yield curve — the yield at each maturity as of
the most recent available date. This is the end point of the shock window;
the start point is the near-zero curve of January 2020.</p>
""",

    # Cell 15 — bank panel shape/columns
    15: """
<p>The FFIEC bank panel is the core dataset: one row per FDIC-insured institution,
with balance sheet items drawn from five Call Report schedules.
Assets are denominated in thousands of dollars, as FFIEC requires.</p>
""",

    # Cell 17 — bank counts
    17: """
<p>Banks are split into three groups. The Small / Large cutoff is $1.384 billion
in total assets — approximately the median of the full sample.
The GSIB group contains 37 institutions designated by the Financial Stability Board
as globally systemically important.</p>
""",

    # Cell 19 — total assets
    19: """
<p>Despite being tiny in count, G-SIBs hold a disproportionate share of system-wide assets.
The table below shows the aggregate and per-bank asset totals for each group.</p>
""",

    # Cell 21 — asset composition
    21: """
<p>The stacked bar chart shows asset composition for each bank group.
Loans dominate for small banks; securities (including MBS and Treasuries) make up
a larger share for larger institutions, which is why rate sensitivity differs across groups.</p>
""",

    # Cell 23 — securities breakdown
    23: """
<p>Within the securities bucket, RMBS and Treasuries are the main components.
RMBS exposure is concentrated in large and GSIB banks.</p>
""",

    # Cell 25 — by-category composition
    25: """
<p>The table below shows the percentage breakdown of each major asset class by bank group,
confirming the structural differences in portfolio composition across size tiers.</p>
""",

    # Cell 27 — missing values
    27: """
<p>Missing values are uncommon in the aggregated line items used in the analysis.
Columns with the most missingness tend to be granular sub-items not required from all bank types.</p>
""",
}

METHOD_PROSE = {
    # Cell 1 — setup
    1: "",

    # Cell 2 — read_ffiec + schedule load
    2: """
<p>Each of the five schedule files is read through the same cleaning function.
The two-row header is collapsed to just the FFIEC codes; the description row is discarded.
All values are coerced to numeric — any footnote markers or blank strings become missing values.
The table below shows how many banks appear in each schedule after cleaning.</p>
""",

    # Cell 3 — key codes
    3: """
<p>The FFIEC uses internal mnemonic codes for every line item.
The <code>RCFD</code> prefix refers to consolidated data (the bank plus all its subsidiaries);
<code>RCON</code> refers to domestic offices only.
The analysis uses consolidated figures for securities and loans, but domestic-only data
for deposits — consistent with the original paper.</p>
""",

    # Cell 6 — weight table
    6: """
<p>For most asset types, the FFIEC reports a single total per bank with no maturity breakdown.
We distribute each total across the six buckets using fixed weight distributions
drawn from industry averages. RMBS is the exception — it has actual maturity bucket data
in the RC-B schedule, so no assumption is needed there.</p>
""",

    # Cell 7 — allocation example
    7: """
<p>To see the allocation in practice, consider a typical mid-sized bank.
Its total Treasury holdings are spread across the six maturity windows using the
weights above. For RMBS, the actual reported sub-totals are used directly.</p>
""",

    # Cell 9 — shocks table
    9: """
<p>Below are the yield changes at each FRED maturity for the full shock window.
The rate increase was steep and broad-based, with shorter maturities rising fastest
in percentage-point terms.</p>
""",

    # Cell 10 — interpolation
    10: """
<p>Linear interpolation maps the six FRED data points to the midpoint of each
maturity bucket, giving a single yield shock value per bucket to apply uniformly
to all bank holdings in that window.</p>
""",

    # Cell 11 — shock chart
    11: """
<p>The left panel shows the shock magnitude for each bucket. The right panel illustrates
the interpolation: red dots are FRED data points, blue diamonds are the bucket midpoints,
and the dashed line is the straight-line fit between them.</p>
""",

    # Cell 13 — MBS comparison
    13: """
<p>MBB fell further than simple duration arithmetic predicted — that gap is extension risk.
When rates rise and homeowners stop refinancing, the mortgages underlying MBS securities
take much longer to repay than expected, stretching effective duration and amplifying losses.
The RMBS multiplier is calibrated to that observed gap.</p>
""",

    # Cell 14 — MBS chart
    14: "",

    # Cell 16 — example bank walkthrough
    16: """
<p>The table below shows the full loss calculation for one example bank,
broken down by asset type and maturity bucket. Only cells with non-zero holdings are shown.</p>
""",
}

ANALYSIS_PROSE = {
    # Cell 1 — setup
    1: """
<p>All processed outputs — bank panel, market shocks, and Table 1 — are loaded
from the pipeline output directories. MTM losses are recomputed here to allow
disaggregated charts beyond what Table 1 provides.</p>
""",

    # Cell 3 — loss histogram
    3: """
<p>The charts below show the distribution of loss-to-asset ratios across banks,
separately for each size group. Most banks cluster near the system median,
but each group has a right tail of more severely exposed institutions.</p>
""",

    # Cell 5 — top 10 table
    5: """
<p>The ten most exposed banks — measured by MTM loss as a share of total assets —
are shown below. Note that high loss ratios do not automatically imply insolvency:
equity cushion and hedging positions matter too, and neither is fully observable
in Call Report data.</p>
""",

    # Cell 7 — Table 1
    7: """
<p>Table 1 replicates the core result from Jiang et al. (2023).
Values in parentheses are standard deviations across banks within each group.
<em>Share</em> rows give each asset class as a percentage of total exposure.
<em>Loss/Asset</em> is the estimated MTM loss as a fraction of book assets.
<em>Uninsured Deposit/MM Asset</em> is the fragility measure — values above 100
indicate a bank could not cover a full uninsured-depositor run at market prices.</p>
""",

    # Cell 8 — aggregate loss charts
    8: """
<p>The two panels translate Table 1 into a visual comparison.
The left shows absolute dollar losses by group; the right normalises by total assets
to compare severity on a level playing field.</p>
""",

    # Cell 10 — fragility histogram
    10: """
<p>The charts show the distribution of fragility scores across individual banks.
The vertical red line marks the 100% threshold — above which a bank's uninsured deposits
exceed its estimated mark-to-market assets. The panel title reports the share of banks
in each group that cross this threshold.</p>
""",

    # Cell 11 — fragility summary table
    11: """
<p>The table below summarises key fragility statistics by group.
The median is the more robust measure — the mean is pulled up by a small number
of extremely high-fragility institutions.</p>
""",
}

# ---------------------------------------------------------------------------
# Introduction block
# ---------------------------------------------------------------------------

INTRO_HTML = """
<h1>U.S. Bank Fragility &mdash; Analysis Through 2025</h1>

<div class="ref-box">
<strong>Based on:</strong> Jiang, E., Matvos, G., Piskorski, T., &amp; Seru, A. (2023).
<em>Monetary Tightening and U.S. Bank Fragility in 2023: Mark-to-Market Losses and
Uninsured Depositor Runs?</em> NBER Working Paper 31048.<br>
<strong>This project</strong> extends the original analysis through 2025 using only
free, publicly available data &mdash; no WRDS or proprietary sources required.
</div>

<p>When interest rates rise sharply, the market value of fixed-income assets falls.
If those losses are large enough relative to uninsured deposits, a bank faces
run risk &mdash; even without a formal insolvency. This dynamic drove the collapse
of Silicon Valley Bank in March 2023 and has remained relevant as the Federal Reserve
navigates the highest rate environment in decades.</p>

<p>This analysis estimates mark-to-market (MTM) losses on the balance sheets of every
FDIC-insured bank using three free data sources:</p>

<ul>
<li><strong>FFIEC CDR</strong> &mdash; quarterly Call Reports for every bank</li>
<li><strong>FRED API</strong> &mdash; U.S. Treasury constant-maturity yields</li>
<li><strong>Yahoo Finance</strong> &mdash; MBS ETF prices to calibrate the RMBS duration multiplier</li>
</ul>

<p>The report is divided into three sections. <strong>Part I</strong> examines the raw inputs.
<strong>Part II</strong> walks through the processing steps. <strong>Part III</strong> presents the results.</p>
"""

# ---------------------------------------------------------------------------
# Build page body
# ---------------------------------------------------------------------------

parts = [INTRO_HTML]

parts.append('<hr><h2>Part I &mdash; Exploratory Data Analysis</h2>')
parts.append(render_notebook_section(eda_cells, EDA_PROSE))

parts.append('<hr><h2>Part II &mdash; Methodology</h2>')
parts.append(render_notebook_section(method_cells, METHOD_PROSE))

parts.append('<hr><h2>Part III &mdash; Analysis &amp; Results</h2>')
parts.append(render_notebook_section(analysis_cells, ANALYSIS_PROSE))

body_html = "\n".join(parts)

# ---------------------------------------------------------------------------
# CSS + page template
# ---------------------------------------------------------------------------

CSS = """
*, *::before, *::after { box-sizing: border-box; }
body {
    font-family: 'Georgia', 'Times New Roman', serif;
    font-size: 19px;
    line-height: 1.82;
    color: #222;
    background: #fff;
    max-width: 860px;
    margin: 0 auto;
    padding: 50px 28px 100px;
}
h1 {
    font-size: 2.1em;
    color: #12203a;
    border-bottom: 4px solid #4C72B0;
    padding-bottom: 14px;
    margin-bottom: 0.3em;
    line-height: 1.25;
}
h2 {
    font-size: 1.55em;
    color: #16213e;
    margin-top: 2.8em;
    margin-bottom: 0.6em;
    border-left: 6px solid #4C72B0;
    padding-left: 16px;
}
h3 {
    font-size: 1.18em;
    color: #2c3e50;
    margin-top: 1.8em;
    margin-bottom: 0.4em;
}
h4 {
    font-size: 1.02em;
    color: #2c3e50;
    margin-top: 1.4em;
    margin-bottom: 0.3em;
}
p { margin: 0.85em 0; }
ul, ol { padding-left: 1.6em; }
li { margin: 0.35em 0; }
strong { color: #111; }
em { color: #444; }
code {
    background: #f0f0f0;
    padding: 2px 7px;
    border-radius: 4px;
    font-size: 0.87em;
    font-family: 'Consolas', 'Menlo', monospace;
}
hr {
    border: none;
    border-top: 2px solid #e4e8ef;
    margin: 2.8em 0;
}
img {
    max-width: 100%;
    height: auto;
    display: block;
    margin: 22px auto;
    border-radius: 8px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.10);
}
.table-wrap { overflow-x: auto; margin: 18px 0; }
table {
    border-collapse: collapse;
    width: 100%;
    font-size: 0.86em;
    font-family: 'Consolas', 'Menlo', monospace;
}
thead th {
    background: #4C72B0;
    color: #fff;
    padding: 9px 14px;
    text-align: left;
    white-space: nowrap;
}
tbody td {
    padding: 7px 14px;
    border-bottom: 1px solid #e4e8ef;
    vertical-align: top;
}
tbody tr:nth-child(even) { background: #f7f9fc; }
tbody tr:hover { background: #eef2fb; }
.ref-box {
    background: #f4f7ff;
    border: 1px solid #c5d3ee;
    border-radius: 8px;
    padding: 14px 20px;
    font-size: 0.9em;
    margin: 1.5em 0;
    color: #333;
}
"""

PAGE_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>U.S. Bank Fragility &mdash; Analysis Through 2025</title>
<style>
{css}
</style>
<script>
MathJax = {{
  tex: {{ inlineMath: [['$', '$'], ['\\\\(', '\\\\)']] }},
  options: {{ skipHtmlTags: ['script','noscript','style','textarea','pre'] }}
}};
</script>
<script src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-chtml.js" async></script>
</head>
<body>
{body}
</body>
</html>
"""

html = PAGE_TEMPLATE.format(css=CSS, body=body_html)

OUT.parent.mkdir(parents=True, exist_ok=True)
with open(OUT, "w", encoding="utf-8") as f:
    f.write(html)

print(f"Written -> {OUT}")
print(f"Size    -> {OUT.stat().st_size / 1024:.0f} KB")
