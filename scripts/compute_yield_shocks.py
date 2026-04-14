"""
Compute mark-to-market price changes per maturity bucket using actual Treasury
ETF total returns (iShares SHV / SHY / IEI / IEF / TLH / TLT).

Methodology: same as Jiang et al. (2023) — price change = (P_end / P_start) - 1
for each bucket ETF over the configured shock window.

RMBS multiplier = MBB total return / GOVT total return over the same window,
matching the Peizhe Huang replication approach.

Outputs market_shocks.parquet with columns:
    d_tsy_{bucket}   — price change fraction (e.g. -0.14 = -14 %)
    rmbs_multiplier  — MBS / treasury-benchmark price-change ratio
"""
import os
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DATA_DIR = Path(__file__).resolve().parent.parent / "_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

REPORT_DATE_SLASH = os.getenv("REPORT_DATE_SLASH", "12/31/2025")  # MM/DD/YYYY
REPORT_DATE       = REPORT_DATE_SLASH.replace("/", "")

MARKET_START_DATE = pd.to_datetime("2020-01-01")
MARKET_END_DATE   = pd.to_datetime(
    f"{REPORT_DATE[4:]}-{REPORT_DATE[:2]}-{REPORT_DATE[2:4]}"
)

# Mapping from bucket name -> column in mbs_etfs.parquet
BUCKET_PRICE_COLS = {
    "lt1y":    "tsy_lt1y",    # SHV  — iShares Short Treasury Bond ETF
    "1_3y":    "tsy_1_3y",    # SHY  — iShares 1-3 Year Treasury Bond ETF
    "3_5y":    "tsy_3_5y",    # IEI  — iShares 3-7 Year Treasury Bond ETF
    "5_10y":   "tsy_5_10y",   # IEF  — iShares 7-10 Year Treasury Bond ETF
    "10_15y":  "tsy_10_15y",  # TLH  — iShares 10-20 Year Treasury Bond ETF
    "15plus":  "tsy_15plus",  # TLT  — iShares 20+ Year Treasury Bond ETF
}

MBS_COL      = "mbs_px"    # MBB — iShares MBS ETF
TSY_BMARK    = "tsy_bmark" # GOVT — iShares US Treasury Bond ETF (blended benchmark)


def _price_on_date(df: pd.DataFrame, col: str, target: pd.Timestamp) -> float:
    """Return Adj-Close price on the trading day nearest to target."""
    df = df.sort_values("date")
    idx = (df["date"] - target).abs().idxmin()
    row = df.loc[idx]
    print(f"    {col}: using {row['date'].date()}  (requested {target.date()})")
    return float(row[col])


def _price_change(df: pd.DataFrame, col: str,
                  start: pd.Timestamp, end: pd.Timestamp) -> float:
    p0 = _price_on_date(df, col, start)
    p1 = _price_on_date(df, col, end)
    return (p1 / p0) - 1.0


def main() -> None:
    etfs_path = DATA_DIR / "mbs_etfs.parquet"
    if not etfs_path.exists():
        raise FileNotFoundError(
            f"Missing ETF price file: {etfs_path}\n"
            "Run: doit pull_mbs"
        )

    etfs = pd.read_parquet(etfs_path)
    etfs["date"] = pd.to_datetime(etfs["date"])

    print(f"Shock window: {MARKET_START_DATE.date()} -> {MARKET_END_DATE.date()}")
    print(f"ETF data covers: {etfs['date'].min().date()} to {etfs['date'].max().date()}")
    print()

    # ── Treasury bucket price changes ─────────────────────────────────────────
    shocks: dict[str, float] = {}
    print("Treasury ETF price changes (start -> end):")
    for bucket, col in BUCKET_PRICE_COLS.items():
        chg = _price_change(etfs, col, MARKET_START_DATE, MARKET_END_DATE)
        shocks[f"d_tsy_{bucket}"] = round(chg, 6)
        print(f"  {bucket} ({col}): {chg:+.4f}  ({chg*100:+.2f}%)")

    # ── RMBS multiplier: MBS price change / Treasury benchmark price change ───
    print("\nRMBS multiplier calculation:")
    print("  MBS (MBB):")
    mbs_chg = _price_change(etfs, MBS_COL, MARKET_START_DATE, MARKET_END_DATE)
    print(f"    price change: {mbs_chg:+.4f}  ({mbs_chg*100:+.2f}%)")

    print("  Treasury benchmark (GOVT):")
    tsy_chg = _price_change(etfs, TSY_BMARK, MARKET_START_DATE, MARKET_END_DATE)
    print(f"    price change: {tsy_chg:+.4f}  ({tsy_chg*100:+.2f}%)")

    if abs(tsy_chg) < 1e-8:
        raise ValueError("Treasury benchmark price change is ~0; cannot compute multiplier.")

    rmbs_multiplier = mbs_chg / tsy_chg   # both negative → positive ratio
    print(f"  RMBS multiplier = {mbs_chg:.4f} / {tsy_chg:.4f} = {rmbs_multiplier:.4f}")

    shocks["rmbs_multiplier"] = round(rmbs_multiplier, 6)

    # ── Save ──────────────────────────────────────────────────────────────────
    out = pd.DataFrame([shocks])
    out_path = DATA_DIR / "market_shocks.parquet"
    out.to_parquet(out_path, index=False)

    print(f"\nSaved -> {out_path}")
    print(out.T.rename(columns={0: "value"}).to_string())


if __name__ == "__main__":
    main()
