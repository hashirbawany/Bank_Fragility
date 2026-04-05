"""
doit pipeline for Bank Fragility project.
Run with: doit
Re-runs only tasks whose targets are missing or out of date.
"""

import sys
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).resolve().parent / ".env")

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parent
SCRIPTS     = ROOT / "scripts"
DOC_SCRIPTS = ROOT / "document_scripts"
DATA_DIR    = ROOT / "_data"
OUTPUT_DIR  = ROOT / "_output"
DOCUMENTS   = ROOT / "documents"
ENV_FILE    = ROOT / ".env"

REPORT_DATE_SLASH = os.getenv("REPORT_DATE_SLASH", "12/31/2025")  # MM/DD/YYYY — e.g. 03/31/2025
REPORT_DATE       = REPORT_DATE_SLASH.replace("/", "")

PYTHON = sys.executable


# ── Helper ───────────────────────────────────────────────────────────────────
def run(script: str) -> list:
    """
    Return a doit action that runs a script with the current Python.
    Uses a list (not a shell string) so paths with spaces are handled correctly.
    """
    return [[PYTHON, str(SCRIPTS / script)]]


def doc_run(script: str) -> list:
    """Same as run() but for scripts in document_scripts/."""
    return [[PYTHON, str(DOC_SCRIPTS / script)]]


# ── Tasks ────────────────────────────────────────────────────────────────────

def task_pull_ffiec():
    """Download FFIEC Call Report bulk zip via Selenium."""
    target = DATA_DIR / f"FFIEC CDR Call Bulk All Schedules {REPORT_DATE}.zip"
    return {
        "file_dep": [SCRIPTS / "pull_ffiec.py", ENV_FILE],
        "targets":  [target],
        "actions":  run("pull_ffiec.py"),
        "verbosity": 2,
    }


def task_pull_gsib():
    """Generate GSIB bank list parquet."""
    return {
        "file_dep": [SCRIPTS / "pull_gsib_banks.py", ENV_FILE],
        "targets":  [DATA_DIR / "gsib_list.parquet"],
        "actions":  run("pull_gsib_banks.py"),
        "verbosity": 2,
    }


def task_pull_treasury():
    """Download Treasury yield history from FRED API."""
    return {
        "file_dep": [SCRIPTS / "pull_treasury_yields.py", ENV_FILE],
        "targets":  [DATA_DIR / "treasury_yields.parquet"],
        "actions":  run("pull_treasury_yields.py"),
        "verbosity": 2,
    }


def task_pull_mbs():
    """Download MBS ETF price history from Yahoo Finance."""
    return {
        "file_dep": [SCRIPTS / "pull_mbs_etfs.py", ENV_FILE],
        "targets":  [DATA_DIR / "mbs_etfs.parquet"],
        "actions":  run("pull_mbs_etfs.py"),
        "verbosity": 2,
    }


def task_compute_yield_shocks():
    """Compute market shock parameters from Treasury yield changes."""
    return {
        "file_dep": [
            SCRIPTS / "compute_yield_shocks.py",
            DATA_DIR / "treasury_yields.parquet",
            ENV_FILE,
        ],
        "targets":  [DATA_DIR / "market_shocks.parquet"],
        "actions":  run("compute_yield_shocks.py"),
        "verbosity": 2,
    }


def task_process_ffiec():
    """Process FFIEC zip into bank panel and summary stats."""
    ffiec_zip = DATA_DIR / f"FFIEC CDR Call Bulk All Schedules {REPORT_DATE}.zip"
    return {
        "file_dep": [
            SCRIPTS / "process_ffiec.py",
            SCRIPTS / "pull_gsib_banks.py",
            ffiec_zip,
            DATA_DIR / "gsib_list.parquet",
            ENV_FILE,
        ],
        "targets": [
            DATA_DIR   / f"bank_panel_{REPORT_DATE}.parquet",
            OUTPUT_DIR / f"summary_stats_{REPORT_DATE}.xlsx",
            OUTPUT_DIR / f"figure_A1_{REPORT_DATE}.png",
            OUTPUT_DIR / "summary_assets.tex",
            OUTPUT_DIR / "summary_liabilities.tex",
        ],
        "actions":  run("process_ffiec.py"),
        "verbosity": 2,
    }


def task_make_table_1():
    """Compute Table 1 — bank fragility summary."""
    return {
        "file_dep": [
            SCRIPTS / "make_table_1.py",
            SCRIPTS / "pull_gsib_banks.py",
            DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet",
            DATA_DIR / "market_shocks.parquet",
            ENV_FILE,
        ],
        "targets": [
            OUTPUT_DIR / "table_1.csv",
            OUTPUT_DIR / "table_1.tex",
        ],
        "actions":  run("make_table_1.py"),
        "verbosity": 2,
    }



def task_generate_pdf():
    """Generate LaTeX project description and compile to PDF."""
    return {
        "file_dep": [
            DOC_SCRIPTS / "generate_pdf.py",
            ENV_FILE,
        ],
        "targets":  [DOCUMENTS / "project_description.pdf"],
        "actions":  doc_run("generate_pdf.py"),
        "verbosity": 2,
    }


def task_generate_pipeline_doc():
    """Generate pipeline technical documentation PDF."""
    return {
        "file_dep": [
            DOC_SCRIPTS / "generate_pipeline_doc.py",
            ENV_FILE,
        ],
        "targets":  [DOCUMENTS / "pipeline_documentation.pdf"],
        "actions":  doc_run("generate_pipeline_doc.py"),
        "verbosity": 2,
    }


def task_generate_research_paper():
    """Generate research paper PDF with embedded analysis tables."""
    return {
        "file_dep": [
            DOC_SCRIPTS / "generate_research_paper.py",
            OUTPUT_DIR / "table_1.tex",
            OUTPUT_DIR / "table_1.csv",
            OUTPUT_DIR / f"summary_stats_{REPORT_DATE}.xlsx",
            OUTPUT_DIR / f"figure_A1_{REPORT_DATE}.png",
            ENV_FILE,
        ],
        "targets":  [DOCUMENTS / "research_paper.pdf"],
        "actions":  doc_run("generate_research_paper.py"),
        "verbosity": 2,
    }


def task_generate_student_notebook():
    """Generate the student teaching notebook (student_notebook.ipynb)."""
    return {
        "file_dep": [
            DOC_SCRIPTS / "generate_student_notebook.py",
            ENV_FILE,
        ],
        "targets":  [DOCUMENTS / "student_notebook.ipynb"],
        "actions":  doc_run("generate_student_notebook.py"),
        "verbosity": 2,
    }


def task_generate_blog_html():
    """Convert student notebook + EDA charts to blog-style HTML (bank_fragility_blog.html)."""
    return {
        "file_dep": [
            DOC_SCRIPTS / "generate_blog_html.py",
            DOCUMENTS   / "student_notebook.ipynb",
            DOCUMENTS   / "eda.ipynb",
        ],
        "targets":  [DOCUMENTS / "bank_fragility_blog.html"],
        "actions":  doc_run("generate_blog_html.py"),
        "verbosity": 2,
    }
