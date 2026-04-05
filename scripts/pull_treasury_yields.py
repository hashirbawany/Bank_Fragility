from __future__ import annotations
import os
import time
from pathlib import Path
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DATA_DIR = Path(__file__).resolve().parent.parent / "_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = DATA_DIR / "treasury_yields.parquet"

SERIES = {
    "dgs1": "DGS1",
    "dgs3": "DGS3",
    "dgs5": "DGS5",
    "dgs10": "DGS10",
    "dgs20": "DGS20",
    "dgs30": "DGS30",
}

FRED_API_KEY = os.getenv("FRED_API_KEY")
BASE_URL = "https://api.stlouisfed.org/fred/series/observations?series_id={series}&api_key={api_key}&file_type=json&observation_start=1900-01-01"


def pull(series_code: str, max_retries: int = 3, timeout: int = 30) -> pd.DataFrame:
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY is not set in your .env file.")

    url = BASE_URL.format(series=series_code, api_key=FRED_API_KEY)

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    last_err = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()

            observations = resp.json()["observations"]
            df = pd.DataFrame(observations)[["date", "value"]]
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna().rename(columns={"value": series_code.lower()})
            df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)
            return df

        except Exception as e:
            last_err = e
            print(f"Attempt {attempt}/{max_retries} failed for {series_code}: {e}")
            if attempt < max_retries:
                time.sleep(2)

    raise RuntimeError(f"Failed to pull {series_code}") from last_err



def _normalize_dataframe(df: pd.DataFrame, series_code: str) -> pd.DataFrame:
    """
    Normalize a single series dataframe to:
    - date (datetime)
    - one value column named after the series code in lowercase
    """
    df = df.copy()
    df.columns = [col.lower().strip() for col in df.columns]

    target_col = series_code.lower()

    # Find / standardize date column
    if "date" not in df.columns:
        possible_date_cols = [c for c in df.columns if "date" in c or "time" in c]
        if possible_date_cols:
            df = df.rename(columns={possible_date_cols[0]: "date"})
        else:
            raise ValueError(f"Could not locate a date column for {series_code}")

    # Find / standardize value column
    if target_col in df.columns:
        value_col = target_col
    elif "value" in df.columns:
        value_col = "value"
    else:
        # Prefer the first non-date column
        candidate_cols = [c for c in df.columns if c != "date"]
        if not candidate_cols:
            raise ValueError(f"Could not locate a value column for {series_code}")
        value_col = candidate_cols[-1]

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")

    df = df.dropna(subset=["date", value_col])
    df = df[["date", value_col]].rename(columns={value_col: target_col})
    df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)

    return df



def write_placeholder() -> None:
    placeholder = pd.DataFrame(
        {
            "date": pd.Series(dtype="datetime64[ns]"),
            "dgs1": pd.Series(dtype="float64"),
            "dgs3": pd.Series(dtype="float64"),
            "dgs5": pd.Series(dtype="float64"),
            "dgs10": pd.Series(dtype="float64"),
            "dgs20": pd.Series(dtype="float64"),
            "dgs30": pd.Series(dtype="float64"),
        }
    )
    placeholder.to_parquet(OUTPUT_PATH, index=False)
    print(f"Wrote placeholder file to {OUTPUT_PATH}")



def main() -> None:
    pulled = []

    for out_name, fred_code in SERIES.items():
        try:
            df = pull(fred_code)

            # Make sure the final column name matches the desired output name
            expected_col = fred_code.lower()
            if expected_col != out_name and expected_col in df.columns:
                df = df.rename(columns={expected_col: out_name})

            pulled.append(df)
            print(f"Pulled {fred_code} -> shape {df.shape}")

        except Exception as e:
            print(f"Skipping {fred_code}: {e}")

    if not pulled:
        write_placeholder()
        return

    out = pulled[0]
    for df in pulled[1:]:
        out = out.merge(df, on="date", how="outer")

    out = out.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)

    value_cols = [c for c in out.columns if c != "date"]
    out = out.dropna(subset=value_cols, how="all")

    out.to_parquet(OUTPUT_PATH, index=False)

    print(f"Wrote {OUTPUT_PATH} | rows={len(out)} cols={out.shape[1]}")
    if not out.empty:
        print(f"Date range: {out['date'].min().date()} to {out['date'].max().date()}")



if __name__ == "__main__":
    main()