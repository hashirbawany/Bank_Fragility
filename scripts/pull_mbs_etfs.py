from __future__ import annotations
import os
from pathlib import Path
from typing import Dict
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DATA_DIR = Path(__file__).resolve().parent.parent / "_data"
MARKET_START_DATE = pd.to_datetime(os.getenv("MARKET_START_DATE", "2020-01-01"))
MARKET_END_DATE = pd.to_datetime(os.getenv("MARKET_END_DATE", "2023-12-31"))

TICKERS: Dict[str, str] = {
    # MBS — iShares MBS ETF (data back to 2007; replaces SPMB which launched Mar 2020)
    "mbs_px":     "MBB",
    # Treasury benchmark — iShares US Treasury Bond ETF (blended; RMBS-mult denominator)
    "tsy_bmark":  "GOVT",
    # Treasury ETFs by maturity bucket (all data back to ≥2007)
    "tsy_lt1y":   "SHV",   # iShares Short Treasury Bond ETF    (<1 yr)
    "tsy_1_3y":   "SHY",   # iShares 1-3 Year Treasury Bond ETF
    "tsy_3_5y":   "IEI",   # iShares 3-7 Year Treasury Bond ETF  (proxy for 3-5 yr)
    "tsy_5_10y":  "IEF",   # iShares 7-10 Year Treasury Bond ETF (proxy for 5-10 yr)
    "tsy_10_15y": "TLH",   # iShares 10-20 Year Treasury Bond ETF(proxy for 10-15 yr)
    "tsy_15plus": "TLT",   # iShares 20+ Year Treasury Bond ETF  (proxy for 15+ yr)
}


def _get_price_series(px: pd.DataFrame, ticker: str) -> pd.Series:
    """
    Extract adjusted close if available, otherwise close.
    Handles both single-index and multi-index yfinance outputs.
    """
    if px is None or px.empty:
        raise ValueError(f"No price data returned for {ticker}.")

    # Use Close (not Adj Close) so we capture pure price changes (capital gains/losses)
    # without dividend income. Treasury / MBS ETFs don't have splits, so Close is reliable.
    if isinstance(px.columns, pd.MultiIndex):
        for field in ("Close", "Adj Close"):
            if (field, ticker) in px.columns:
                s = px[(field, ticker)]
                s.name = ticker
                return s

        for field in ("Close", "Adj Close"):
            if field in px.columns.get_level_values(0):
                sub = px[field]
                if isinstance(sub, pd.DataFrame):
                    s = sub.iloc[:, 0]
                else:
                    s = sub
                s.name = ticker
                return s

        raise KeyError(
            f"No Close/Adj Close found for {ticker}. Columns: {list(px.columns)}"
        )

    for field in ("Close", "Adj Close"):
        if field in px.columns:
            s = px[field].copy()
            s.name = ticker
            return s

    raise KeyError(
        f"No Close/Adj Close found for {ticker}. Columns: {px.columns.tolist()}"
    )


def pull_etf_prices(
    start: str = "2000-01-01",
    end: str | None = None,
    tickers: Dict[str, str] = TICKERS,
) -> pd.DataFrame:
    """
    Pull ETF prices and return a daily dataframe with:
    - date
    - rmbs_px
    - cmbs_px
    - rmbs_ret
    - cmbs_ret
    """
    series = []

    for out_col, ticker in tickers.items():
        px = yf.download(
            ticker,
            start=start,
            end=end,
            auto_adjust=False,
            progress=False,
            actions=False,
            threads=False,
        )

        s = _get_price_series(px, ticker).rename(out_col)
        s.index = pd.to_datetime(s.index, errors="coerce")
        s = s[~s.index.isna()]
        series.append(s)

    df = pd.concat(series, axis=1, join="outer").sort_index()

    # Keep dates where all series are available (all tickers have data from ≥2007)
    required_cols = [c for c in tickers.keys() if c not in ("mbs_ret",)]
    df = df.dropna(subset=required_cols, how="any")

    df = df.reset_index()
    date_col = "Date" if "Date" in df.columns else df.columns[0]
    df = df.rename(columns={date_col: "date"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    # Daily returns for diagnostics
    if "mbs_px" in df.columns:
        df["mbs_ret"] = df["mbs_px"].pct_change()

    return df


def save_mbs_etfs(df: pd.DataFrame, filename: str = "mbs_etfs.parquet") -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    outpath = DATA_DIR / filename
    df.to_parquet(outpath, index=False)

    print(f"Wrote {outpath} | rows={len(df):,} cols={df.shape[1]}")
    if not df.empty:
        print(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")


def main() -> None:
    # Pull long history so configured market window is covered
    df = pull_etf_prices(start="2000-01-01", end=None)

    if df.empty:
        raise ValueError("ETF price pull returned an empty dataframe.")

    # Diagnostic check that the configured window is covered
    min_date = df["date"].min()
    max_date = df["date"].max()

    if MARKET_START_DATE < min_date or MARKET_END_DATE > max_date:
        raise ValueError(
            "Configured market window is not fully covered by pulled ETF data.\n"
            f"Window: {MARKET_START_DATE.date()} to {MARKET_END_DATE.date()}\n"
            f"Available: {min_date.date()} to {max_date.date()}"
        )

    save_mbs_etfs(df)

    print("\nSample rows near configured window:")
    window_df = df[
        (df["date"] >= MARKET_START_DATE - pd.Timedelta(days=5))
        & (df["date"] <= MARKET_END_DATE + pd.Timedelta(days=5))
    ]
    print(window_df.tail(10).to_string(index=False))


if __name__ == "__main__":
    main()