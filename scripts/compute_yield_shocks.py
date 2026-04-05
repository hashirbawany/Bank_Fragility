import os
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DATA_DIR = Path(__file__).resolve().parent.parent / "_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

REPORT_DATE_SLASH = os.getenv("REPORT_DATE_SLASH", "12/31/2025")  # MM/DD/YYYY — e.g. 03/31/2025
REPORT_DATE       = REPORT_DATE_SLASH.replace("/", "")
# Derive shock window automatically:
#   start = 2020-01-01 (rates were near-zero throughout 2020 — the baseline)
#   end   = the report date itself
MARKET_START_DATE = pd.to_datetime("2020-01-01")
MARKET_END_DATE   = pd.to_datetime(f"{REPORT_DATE[4:]}-{REPORT_DATE[:2]}-{REPORT_DATE[2:4]}")
RMBS_MULTIPLIER   = float(os.getenv("RMBS_MULTIPLIER", "1.25"))

# FRED maturities (years) and their column names
FRED_MATURITIES = [1, 3, 5, 10, 20, 30]
FRED_COLS       = ["dgs1", "dgs3", "dgs5", "dgs10", "dgs20", "dgs30"]

# Each bucket is represented by its midpoint (years)
# Used to interpolate a yield from the FRED curve
BUCKETS = {
    "lt1y":   0.5,
    "1_3y":   2.0,
    "3_5y":   4.0,
    "5_10y":  7.5,
    "10_15y": 12.5,
    "15plus": 22.0,
}


def interpolate_yield(yields: pd.Series, maturity: float) -> float:
    """
    Linearly interpolate a yield at a given maturity (years)
    from the available FRED maturity points.
    """
    known_maturities = np.array(FRED_MATURITIES, dtype=float)
    known_yields     = yields[FRED_COLS].values.astype(float)

    # Drop NaN points before interpolating
    mask = ~np.isnan(known_yields)
    if mask.sum() < 2:
        raise ValueError(f"Not enough yield data to interpolate. Got: {known_yields}")

    return float(np.interp(maturity, known_maturities[mask], known_yields[mask]))


def get_yield_on_date(treasury: pd.DataFrame, date: pd.Timestamp) -> pd.Series:
    """
    Return the yield curve row closest to the given date.
    """
    treasury = treasury.sort_values("date")
    idx = (treasury["date"] - date).abs().idxmin()
    row = treasury.loc[idx]
    print(f"  Using date: {row['date'].date()} (requested: {date.date()})")
    return row


def main() -> None:
    treasury = pd.read_parquet(DATA_DIR / "treasury_yields.parquet")
    treasury["date"] = pd.to_datetime(treasury["date"])

    print(f"Shock window: {MARKET_START_DATE.date()} -> {MARKET_END_DATE.date()}")
    print()

    print("Start date yield curve:")
    start_yields = get_yield_on_date(treasury, MARKET_START_DATE)

    print("End date yield curve:")
    end_yields = get_yield_on_date(treasury, MARKET_END_DATE)

    print()
    print("Yield changes by FRED maturity (end - start):")
    for col, mat in zip(FRED_COLS, FRED_MATURITIES):
        chg = end_yields[col] - start_yields[col]
        print(f"  {col} ({mat}yr): {chg:+.4f}%")

    # Compute shock for each bucket via interpolation
    shocks = {}
    print()
    print("Interpolated shocks by bucket:")
    for bucket, midpoint in BUCKETS.items():
        start_y = interpolate_yield(start_yields, midpoint)
        end_y   = interpolate_yield(end_yields,   midpoint)
        shock   = end_y - start_y
        shocks[f"d_tsy_{bucket}"] = round(shock, 6)
        print(f"  {bucket} (midpoint {midpoint}yr): {shock:+.4f}%")

    shocks["rmbs_multiplier"] = RMBS_MULTIPLIER

    # Saving
    out = pd.DataFrame([shocks])
    out_path = DATA_DIR / "market_shocks.parquet"
    out.to_parquet(out_path, index=False)

    print()
    print(f"Saved -> {out_path}")
    print(out.T.rename(columns={0: "value"}).to_string())


if __name__ == "__main__":
    main()
