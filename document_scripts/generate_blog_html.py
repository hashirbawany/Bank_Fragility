"""
Converts student_notebook.ipynb -> documents/bank_fragility_blog.html
Strips all code, replaces each code cell with plain-English prose,
keeps matplotlib charts and pandas tables from the notebook outputs.
"""

import base64
import json
import re
from pathlib import Path

def md_inline(text):
    """Convert inline markdown to HTML — protects code spans and math first."""
    # Step 1: pull out backtick code spans so they aren't touched by other rules
    codes = []
    def save_code(m):
        codes.append(m.group(1))
        return f"\x00CODE{len(codes)-1}\x00"
    text = re.sub(r"`(.+?)`", save_code, text)

    # Step 2: pull out inline math $...$ so underscores inside aren't italicised
    maths = []
    def save_math(m):
        maths.append(m.group(0))
        return f"\x00MATH{len(maths)-1}\x00"
    text = re.sub(r"\$\$.+?\$\$", save_math, text, flags=re.DOTALL)
    text = re.sub(r"\$.+?\$",     save_math, text)

    # Step 3: HTML-escape the remaining text
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # Step 4: bold (** only — avoid __ to prevent false matches with underscores)
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)

    # Step 5: italic (* only — never _ to avoid mangling variable names)
    text = re.sub(r"\*(.+?)\*", r"<em>\1</em>", text)

    # Step 6: links
    text = re.sub(r"\[(.+?)\]\((.+?)\)", r'<a href="\2">\1</a>', text)

    # Step 7: restore code spans (escape their content separately)
    for j, raw in enumerate(codes):
        escaped = raw.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        text = text.replace(f"\x00CODE{j}\x00", f"<code>{escaped}</code>")

    # Step 8: restore math spans as-is (MathJax will handle them)
    for j, raw in enumerate(maths):
        text = text.replace(f"\x00MATH{j}\x00", raw)

    return text


def md_to_html(text):
    """Convert a markdown cell to HTML — handles headings, lists, tables, paragraphs."""
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

        # Horizontal rule
        if stripped in ("---", "***", "___"):
            close_lists()
            out.append("<hr>")
            i += 1
            continue

        # Headings
        m = re.match(r"^(#{1,4})\s+(.*)", stripped)
        if m:
            close_lists()
            level = len(m.group(1))
            tag = f"h{level}"
            out.append(f"<{tag}>{md_inline(m.group(2))}</{tag}>")
            i += 1
            continue

        # Markdown table  (line contains | and next line is separator)
        if "|" in stripped and stripped.startswith("|"):
            close_lists()
            # Collect all table lines
            tbl_lines = []
            while i < len(lines) and "|" in lines[i] and lines[i].strip().startswith("|"):
                tbl_lines.append(lines[i].strip())
                i += 1
            # Parse table
            rows = []
            is_header = True
            sep_seen = False
            for tl in tbl_lines:
                cells = [c.strip() for c in tl.strip("|").split("|")]
                # separator row?
                if all(re.match(r"^[-: ]+$", c) for c in cells if c):
                    sep_seen = True
                    continue
                rows.append((is_header and not sep_seen, cells))
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

        # Unordered list
        m = re.match(r"^[-*+]\s+(.*)", stripped)
        if m:
            if not in_ul:
                close_lists()
                out.append("<ul>")
                in_ul = True
            out.append(f"<li>{md_inline(m.group(1))}</li>")
            i += 1
            continue

        # Ordered list
        m = re.match(r"^\d+\.\s+(.*)", stripped)
        if m:
            if not in_ol:
                close_lists()
                out.append("<ol>")
                in_ol = True
            out.append(f"<li>{md_inline(m.group(1))}</li>")
            i += 1
            continue

        # Blank line
        if stripped == "":
            close_lists()
            out.append("")
            i += 1
            continue

        # Regular paragraph line — accumulate until blank
        close_lists()
        para_lines = []
        while i < len(lines) and lines[i].strip() != "" and not lines[i].strip().startswith("#") and not lines[i].strip().startswith("|") and not re.match(r"^[-*+]\s", lines[i].strip()) and not re.match(r"^\d+\.\s", lines[i].strip()) and lines[i].strip() not in ("---","***","___"):
            para_lines.append(lines[i].strip())
            i += 1
        para_text = " ".join(para_lines)
        if para_text:
            out.append(f"<p>{md_inline(para_text)}</p>")
        continue

    close_lists()
    return "\n".join(out)

ROOT     = Path(__file__).resolve().parent.parent
NB_PATH  = ROOT / "documents" / "student_notebook.ipynb"
EDA_PATH = ROOT / "documents" / "eda.ipynb"
OUT      = ROOT / "documents" / "bank_fragility_blog.html"

with open(NB_PATH, encoding="utf-8") as f:
    nb = json.load(f)
cells = nb["cells"]

# Load EDA notebook and pre-extract images keyed by cell index
eda_images: dict[int, list[str]] = {}
if EDA_PATH.exists():
    with open(EDA_PATH, encoding="utf-8") as f:
        eda_nb = json.load(f)
    for idx, cell in enumerate(eda_nb["cells"]):
        imgs = []
        for out in cell.get("outputs", []):
            data = out.get("data", {})
            if "image/png" in data:
                b64 = data["image/png"]
                if isinstance(b64, list):
                    b64 = "".join(b64)
                imgs.append(f'<img src="data:image/png;base64,{b64}" alt="chart">')
        if imgs:
            eda_images[idx] = imgs

# ---------------------------------------------------------------------------
# EDA chart injection points:
# Keys are student_notebook cell indices; after that cell's content is rendered,
# the listed EDA cell images are inserted into the blog.
# ---------------------------------------------------------------------------
EDA_INSERTS: dict[int, list[int]] = {
    9:  [17, 19, 25],   # bank counts section  → count bars, asset dist, by-category
    14: [12, 13],        # treasury yields      → yield time series, latest curve
    20: [8],             # RMBS multiplier      → MBS ETF price paths
    23: [21, 23],        # all-bank losses      → asset composition, securities breakdown
    24: [36],            # loss distribution    → EDA loss histograms by group
    27: [45],            # summary charts       → EDA visual comparison
}

# ---------------------------------------------------------------------------
# Plain-English replacements for every code cell (keyed by cell index)
# Blog-style: no jargon, no math, conversational tone
# ---------------------------------------------------------------------------

PROSE = {
    # Cell 1 — setup/imports (skip, no user-facing content)
    1: "",

    # Cell 3 — ZIP inspection (all commented out, no output)
    3: "",

    # Cell 4 — raw FFIEC data
    4: """
<p>Below is a snapshot of what the raw FFIEC file looks like straight out of the download.
Each row is one bank, identified by its RSSD ID — a unique number assigned by the Federal Reserve.
The column headers are internal FFIEC codes (things like <code>RCFD2170</code>, which means total assets).
A second row in the original file contains plain-English descriptions of those codes;
it gets dropped during processing so the software doesn't mistake it for actual bank data.</p>
""",

    # Cell 7 — cleaned data demo
    7: """
<p>After standardising, the column codes are mapped to readable variable names and every value is
converted to a number. Non-numeric entries — missing data, footnote markers, and so on — become blank.
Below are a handful of banks with their total assets, cash on hand, and loan portfolios,
all reported in thousands of dollars as FFIEC requires.</p>
""",

    # Cell 8 — bank panel load
    8: """
<p>All five schedule files are merged into a single flat table — one row per bank,
with every balance sheet item the analysis needs. The dataset covers every
FDIC-insured commercial bank and savings institution that filed a Call Report for the quarter.</p>
""",

    # Cell 9 — bank counts
    9: """
<p>Banks are divided into three groups based on size and regulatory designation.
The line between <em>Small</em> and <em>Large</em> sits at <strong>$1.384 billion</strong>
in total assets — roughly the median of the full sample.
The 37 G-SIBs (Globally Systemically Important Banks) are flagged separately using a fixed list
of RSSD IDs corresponding to institutions designated by the Financial Stability Board.</p>
""",

    # Cell 10 — Figure A1
    10: """
<p>The chart below shows just how unequal the U.S. banking system is by size.
A handful of giant institutions hold an enormous share of total system assets,
while thousands of community banks each hold comparatively small amounts.</p>
""",

    # Cell 12 — bucket weights table
    12: """
<p>For most asset types — Treasuries, commercial loans, consumer loans — the FFIEC only reports
a single total per bank, not a breakdown by how long until the assets mature.
To estimate losses, we need to know how much of each asset sits in each maturity window.
The table below shows the assumed split. These percentages are the same for every bank,
which is one of the key simplifying assumptions of this approach.
Mortgage-backed securities are the exception: the call report actually provides separate
line items for each maturity bucket, so no assumption is needed there.</p>
""",

    # Cell 14 — treasury yields table
    14: """
<p>Below are the most recent yield observations pulled from FRED,
showing the daily rate for each maturity from 1 year out to 30 years.
All values are in percent per annum.</p>
""",

    # Cell 15 — yield curve chart
    15: """
<p>The left panel compares the yield curve at the beginning and end of the shock window —
the orange shading shows how much rates rose across every maturity.
The right panel puts that move in historical context, showing the 10-year yield all the way
back through the dataset with the shock window marked.</p>
""",

    # Cell 17 — shocks table
    17: """
<p>After reading off the yield change at each bucket's midpoint maturity,
here are the shocks that get applied to bank holdings in each time window.
Short-maturity assets experienced the largest rate increases in percentage-point terms.</p>
""",

    # Cell 18 — shocks chart
    18: """
<p>The bar chart on the left shows the shock for each bucket at a glance.
The diagram on the right shows exactly how those numbers were derived:
the red dots are the six FRED data points, the blue diamonds are the bucket midpoints
we need, and the dashed line is the straight-line interpolation between them.</p>
""",

    # Cell 20 — SPMB chart
    20: """
<p>The chart compares SPMB's actual price path (red) to what simple duration arithmetic
would have predicted (blue dashed). SPMB fell noticeably further — that gap is extension risk.
When rates rise and homeowners stop refinancing, the mortgages underlying these securities
take much longer to repay than originally expected, which stretches the effective duration
and amplifies the price drop.
The 1.25× multiplier is calibrated to that observed gap.</p>
""",

    # Cell 22 — example bank walkthrough
    22: """
<p>To see how the numbers play out in practice, take a typical small community bank
sitting right at the middle of the small-bank size distribution.
It holds mortgage-backed securities, Treasuries, and loans spread across the six maturity windows.
For each position the formula is straightforward: multiply the dollar amount held
by the yield shock for that window, then apply the 1.25× penalty if the asset is an MBS or
residential mortgage loan.
Adding up all those individual losses gives the bank's total mark-to-market loss.
Subtract that from total assets to get the mark-to-market asset value,
then divide uninsured deposits by that figure to get the fragility score.</p>
""",

    # Cell 23 — all banks
    23: """
<p>Running that same calculation across all banks in the dataset gives the system-wide picture.
The table below summarises losses and fragility by bank group.</p>
""",

    # Cell 24 — distribution charts
    24: """
<p>The histograms above show the full distribution of loss ratios and fragility scores
across individual banks, broken out by group.
The distributions overlap — there is no clean dividing line between small and large banks —
but the right-hand tails differ. A subset of banks in every group shows fragility well above 100%,
meaning their uninsured deposit base exceeds their estimated mark-to-market asset value.</p>
""",

    # Cell 26 — Table 1
    26: """
<p>The table below is the main output of the analysis, replicating Table 1 from Jiang et al. (2023).
Numbers in parentheses beneath each mean are standard deviations across banks within that group.
<em>Share</em> rows express each asset class as a percentage of total estimated exposure.
<em>Loss/Asset</em> is the estimated mark-to-market loss as a share of total book assets.
<em>Uninsured Deposit/MM Asset</em> is the fragility measure — values above 100
indicate the bank could not cover a full uninsured-depositor run at mark-to-market prices.</p>
""",

    # Cell 27 — summary charts
    27: """
<p>The three charts summarise the headline results by bank group.
G-SIBs dominate in absolute dollar terms — their size means they account for the bulk of
system-wide losses. On a relative basis the picture is more nuanced: large non-GSIB banks
often carry the highest loss-to-asset ratios, and elevated fragility is a feature of all three groups,
not just the biggest institutions.</p>
""",
}

# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

def clean_pandas_html(raw_html):
    """Strip pandas-generated styles/classes so blog CSS takes over."""
    # Remove the <style scoped> block pandas injects
    raw_html = re.sub(r"<style[^>]*>.*?</style>", "", raw_html, flags=re.DOTALL)
    # Remove inline styles, class attributes, and border attribute
    raw_html = re.sub(r' style="[^"]*"', "", raw_html)
    raw_html = re.sub(r' class="[^"]*"', "", raw_html)
    raw_html = re.sub(r' border="\d+"', "", raw_html)
    # Empty top-left header cell
    raw_html = re.sub(r"<th></th>", "<th>#</th>", raw_html)
    return raw_html


def extract_outputs(cell):
    """Return (images_html, tables_html) from a cell's outputs."""
    images, tables = [], []
    for out in cell.get("outputs", []):
        data = out.get("data", {})
        if "image/png" in data:
            b64 = data["image/png"]
            if isinstance(b64, list):
                b64 = "".join(b64)
            images.append(
                f'<img src="data:image/png;base64,{b64}" alt="chart">'
            )
        elif "text/html" in data:
            html_src = data["text/html"]
            if isinstance(html_src, list):
                html_src = "".join(html_src)
            tables.append(
                '<div class="table-wrap">'
                + clean_pandas_html(html_src)
                + "</div>"
            )
    return images, tables


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
blockquote {
    border-left: 5px solid #4C72B0;
    margin: 1.4em 0;
    padding: 0.6em 1.2em;
    background: #f4f7ff;
    color: #444;
    border-radius: 0 6px 6px 0;
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
/* Tables */
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
/* Section number badge */
.section-num {
    display: inline-block;
    background: #4C72B0;
    color: #fff;
    border-radius: 50%;
    width: 32px; height: 32px;
    line-height: 32px;
    text-align: center;
    font-size: 0.85em;
    font-weight: bold;
    margin-right: 8px;
    vertical-align: middle;
}
/* Reference box */
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
<title>U.S. Bank Fragility — Analysis</title>
<style>
{css}
</style>
<!-- MathJax for equations -->
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

# ---------------------------------------------------------------------------
# Build the page body
# ---------------------------------------------------------------------------

parts = []

for i, cell in enumerate(cells):
    ctype = cell["cell_type"]
    source = "".join(cell["source"])

    if ctype == "markdown":
        # Remove leading "---" horizontal rules — we handle section breaks via headings
        parts.append(md_to_html(source))

    elif ctype == "code":
        prose = PROSE.get(i, "")
        images, tables = extract_outputs(cell)

        if not prose and not images and not tables:
            # Still inject EDA charts even if the code cell itself has nothing to show
            for eda_idx in EDA_INSERTS.get(i, []):
                for img in eda_images.get(eda_idx, []):
                    parts.append(img)
            continue

        if prose:
            parts.append(prose)

        # Show tables before charts (they give context)
        for tbl in tables:
            parts.append(tbl)

        for img in images:
            parts.append(img)

    # After each cell, inject any EDA charts mapped to this position
    for eda_idx in EDA_INSERTS.get(i, []):
        for img in eda_images.get(eda_idx, []):
            parts.append(img)

body_html = "\n".join(parts)

html = PAGE_TEMPLATE.format(css=CSS, body=body_html)

OUT.parent.mkdir(parents=True, exist_ok=True)
with open(OUT, "w", encoding="utf-8") as f:
    f.write(html)

print(f"Written -> {OUT}")
print(f"Size    -> {OUT.stat().st_size / 1024:.0f} KB")
