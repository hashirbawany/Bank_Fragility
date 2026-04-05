from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DATA_DIR = Path(__file__).resolve().parent.parent / "_data"

# Reporting-bank RSSD IDs used for GSIB classification in the call report panel
GSIB_REPORTING_BANK_IDS = [
    934329, 488318, 212465, 449038, 476810, 3382547, 852218, 651448,
    480228, 1443266, 413208, 3357620, 1015560, 2980209, 214807, 304913,
    670560, 2325882, 2182786, 3066025, 398668, 541101, 229913, 1456501,
    2489805, 722777, 35301, 93619, 352745, 812164, 925411, 3212149,
    451965, 688079, 1225761, 2362458, 2531991,
]


def pull_gsib_list() -> pd.DataFrame:
    """
    Return GSIB reporting-bank RSSD IDs as a DataFrame.
    """
    df = pd.DataFrame({"rssd_id_call": GSIB_REPORTING_BANK_IDS})

    # Ensure numeric and remove duplicates
    df["rssd_id_call"] = pd.to_numeric(df["rssd_id_call"], errors="coerce")
    df = df.dropna().drop_duplicates()

    df["rssd_id_call"] = df["rssd_id_call"].astype(int)
    df["is_gsib"] = 1

    return df


def save_gsib_list(df: pd.DataFrame, filename: str = "gsib_list.parquet") -> None:
    """
    Save GSIB list to the data directory.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    outpath = DATA_DIR / filename
    df.to_parquet(outpath, index=False)

    print(f"Wrote {outpath} | rows={len(df):,} cols={df.shape[1]}")


if __name__ == "__main__":
    df = pull_gsib_list()
    save_gsib_list(df)