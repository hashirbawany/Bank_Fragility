import os
import sys
from pathlib import Path
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pull_gsib_banks import pull_gsib_list

DATA_DIR   = Path(__file__).resolve().parent.parent / "_data"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "_output"
REPORT_DATE_SLASH = os.getenv("REPORT_DATE_SLASH", "12/31/2025")  # MM/DD/YYYY — e.g. 03/31/2025
REPORT_DATE       = REPORT_DATE_SLASH.replace("/", "")

# Call report values are in thousands of dollars
SMALL_CUTOFF = 1.384e6  # $1.384B in thousands

BUCKETS = [
    ("lt1y", ("d_tsy_lt1y", "d_tsy_1Y")),
    ("1_3y", ("d_tsy_1_3y", "d_tsy_3Y")),
    ("3_5y", ("d_tsy_3_5y", "d_tsy_5Y")),
    ("5_10y", ("d_tsy_5_10y", "d_tsy_10Y")),
    ("10_15y", ("d_tsy_10_15y", "d_tsy_20Y")),
    ("15plus", ("d_tsy_15plus", "d_tsy_30Y")),
]


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    out = a / b.replace({0: np.nan})
    return out.replace([np.inf, -np.inf], np.nan)


def _fmt_mean(
    x: pd.Series,
    scale: float = 1.0,
    digits: int = 1,
    suffix: str = "",
) -> str:
    v = (x.dropna() * scale).astype(float)
    if len(v) == 0:
        return ""
    return f"{np.nanmean(v):.{digits}f}{suffix}"


def _fmt_median(
    x: pd.Series,
    scale: float = 1.0,
    digits: int = 1,
    suffix: str = "",
) -> str:
    v = (x.dropna() * scale).astype(float)
    if len(v) == 0:
        return ""
    return f"{np.nanmedian(v):.{digits}f}{suffix}"


def _fmt_sd(
    x: pd.Series,
    scale: float = 1.0,
    digits: int = 1,
) -> str:
    v = (x.dropna() * scale).astype(float)
    if len(v) <= 1:
        return ""
    return f"({np.nanstd(v, ddof=1):,.{digits}f})"


def _fmt_agg_loss_thousands(x: pd.Series) -> str:
    total_dollars = abs(float(np.nansum(x.values)) * 1000)

    if total_dollars >= 1e12:
        return f"{total_dollars / 1e12:.1f}T"
    if total_dollars >= 1e9:
        return f"{total_dollars / 1e9:.1f}B"
    if total_dollars >= 1e6:
        return f"{total_dollars / 1e6:.1f}M"
    return f"{total_dollars:.0f}"


def _format_table_latex(table: pd.DataFrame) -> str:
    latex = table.to_latex(
        escape=False,
        column_format="lcccc",
    )
    latex = latex.replace("\\toprule", "\\hline")
    latex = latex.replace("\\midrule", "\\hline")
    latex = latex.replace("\\bottomrule", "\\hline")
    return latex


def _resolve_shock_col(shocks: pd.Series, candidates: tuple[str, ...]) -> str:
    for c in candidates:
        if c in shocks.index:
            return c
    raise KeyError(f"Could not find any of these shock columns: {candidates}")


def main() -> None:
    bank_panel_path = DATA_DIR / f"bank_panel_{REPORT_DATE}.parquet"
    shocks_path = DATA_DIR / "market_shocks.parquet"

    if not bank_panel_path.exists():
        raise FileNotFoundError(f"Missing bank panel: {bank_panel_path}")
    if not shocks_path.exists():
        raise FileNotFoundError(f"Missing market shocks: {shocks_path}")

    banks = pd.read_parquet(bank_panel_path)
    shocks = pd.read_parquet(shocks_path).iloc[0]

    required_base_cols = [
        "rssd_id_call",
        "Total Asset",
        "Uninsured Deposit",
    ]

    required_bucket_cols: list[str] = []
    for suffix, _ in BUCKETS:
        required_bucket_cols.extend(
            [
                f"rmbs_{suffix}",
                f"treasury_{suffix}",
                f"other_assets_{suffix}",
                f"res_mtg_{suffix}",
                f"other_loan_{suffix}",
            ]
        )

    required_cols = required_base_cols + required_bucket_cols
    missing = [c for c in required_cols if c not in banks.columns]
    if missing:
        raise ValueError(
            "Missing required columns for bucket-based Table 1:\n"
            + ", ".join(missing)
        )

    banks["rssd_id_call"] = pd.to_numeric(
        banks["rssd_id_call"], errors="coerce"
    ).astype("Int64")
    banks["Total Asset"] = pd.to_numeric(banks["Total Asset"], errors="coerce")
    banks["Uninsured Deposit"] = pd.to_numeric(
        banks["Uninsured Deposit"], errors="coerce"
    )

    for suffix, _ in BUCKETS:
        for prefix in ["rmbs", "treasury", "other_assets", "res_mtg", "other_loan"]:
            col = f"{prefix}_{suffix}"
            banks[col] = pd.to_numeric(banks[col], errors="coerce").fillna(0.0)

    gsib_df = pull_gsib_list()
    gsib_ids = set(
        pd.to_numeric(gsib_df["rssd_id_call"], errors="coerce")
        .dropna()
        .astype(int)
    )
    banks["is_gsib"] = banks["rssd_id_call"].isin(gsib_ids).astype(int)

    banks["size_group"] = "large_non_gsib"
    banks.loc[banks["Total Asset"] < SMALL_CUTOFF, "size_group"] = "small"
    banks.loc[banks["is_gsib"] == 1, "size_group"] = "gsib"

    if "rmbs_multiplier" not in shocks.index:
        raise KeyError("market_shocks.parquet is missing 'rmbs_multiplier'")

    rmbs_multiplier = float(shocks["rmbs_multiplier"])

    resolved_shocks: dict[str, float] = {}
    for suffix, candidates in BUCKETS:
        shock_col = _resolve_shock_col(shocks, candidates)
        resolved_shocks[suffix] = float(shocks[shock_col])

    banks["exp_rmbs"] = 0.0
    banks["exp_treasury"] = 0.0
    banks["exp_other_assets"] = 0.0
    banks["exp_res_mtg"] = 0.0
    banks["exp_other_loan"] = 0.0

    banks["loss_rmbs"] = 0.0
    banks["loss_tsy_other"] = 0.0
    banks["loss_res_mtg"] = 0.0
    banks["loss_other_loan"] = 0.0

    for suffix, _ in BUCKETS:
        shock = float(resolved_shocks[suffix])

        banks["exp_rmbs"] += banks[f"rmbs_{suffix}"]
        banks["exp_treasury"] += banks[f"treasury_{suffix}"]
        banks["exp_other_assets"] += banks[f"other_assets_{suffix}"]
        banks["exp_res_mtg"] += banks[f"res_mtg_{suffix}"]
        banks["exp_other_loan"] += banks[f"other_loan_{suffix}"]

        banks["loss_rmbs"] += banks[f"rmbs_{suffix}"] * shock * rmbs_multiplier
        banks["loss_res_mtg"] += banks[f"res_mtg_{suffix}"] * shock * rmbs_multiplier
        banks["loss_tsy_other"] += (
            banks[f"treasury_{suffix}"] * shock
            + banks[f"other_assets_{suffix}"] * shock
        )
        banks["loss_other_loan"] += banks[f"other_loan_{suffix}"] * shock

    banks["exp_tsy_other"] = banks["exp_treasury"] + banks["exp_other_assets"]

    banks["loss_total"] = (
        banks["loss_rmbs"]
        + banks["loss_tsy_other"]
        + banks["loss_res_mtg"]
        + banks["loss_other_loan"]
    )

    for col in [
        "loss_rmbs",
        "loss_tsy_other",
        "loss_res_mtg",
        "loss_other_loan",
        "loss_total",
    ]:
        banks[col] = banks[col].abs()

    banks["mm_assets"] = banks["Total Asset"] - banks["loss_total"]

    # Share metrics: fraction of total MTM LOSS from each asset class (loss shares),
    # matching Jiang et al. (2023) / the prev-author replication approach.
    banks["share_rmbs"] = 100 * _safe_div(banks["loss_rmbs"], banks["loss_total"])
    banks["share_tsy_other"] = 100 * _safe_div(
        banks["loss_tsy_other"], banks["loss_total"]
    )
    banks["share_res_mtg"] = 100 * _safe_div(
        banks["loss_res_mtg"], banks["loss_total"]
    )
    banks["share_other_loan"] = 100 * _safe_div(
        banks["loss_other_loan"], banks["loss_total"]
    )

    banks["loss_asset_pct"] = 100 * _safe_div(
        banks["loss_total"], banks["Total Asset"]
    )
    banks["unins_dep_mm_asset_pct"] = 100 * _safe_div(
        banks["Uninsured Deposit"], banks["mm_assets"]
    )

    groups = {
        "All Banks": banks,
        "Small\n(0, 1.384B)": banks[banks["size_group"] == "small"],
        "Large (non-GSIB)\n[1.384B, )": banks[banks["size_group"] == "large_non_gsib"],
        "GSIB": banks[banks["size_group"] == "gsib"],
    }

    out_rows: list[dict[str, str]] = []
    out_index: list[str] = []

    out_index.append("Aggregate Loss")
    out_rows.append(
        {k: _fmt_agg_loss_thousands(df["loss_total"]) for k, df in groups.items()}
    )

    out_index.append("Bank-Level Loss")
    out_rows.append(
        {
            k: _fmt_median(
                df["loss_total"],
                scale=1 / 1000,
                digits=1,
                suffix="M",
            )
            for k, df in groups.items()
        }
    )
    out_index.append("")
    out_rows.append(
        {
            k: _fmt_sd(
                df["loss_total"],
                scale=1 / 1000,
                digits=1,
            )
            for k, df in groups.items()
        }
    )

    share_map = {
        "Share RMBS": "share_rmbs",
        "Share Treasury and Other": "share_tsy_other",
        "Share Residential Mortgage": "share_res_mtg",
        "Share Other Loan": "share_other_loan",
    }

    for label, col in share_map.items():
        out_index.append(label)
        out_rows.append(
            {k: _fmt_median(df[col], digits=1) for k, df in groups.items()}
        )
        out_index.append("")
        out_rows.append(
            {k: _fmt_sd(df[col], digits=1) for k, df in groups.items()}
        )

    out_index.append("Loss/Asset")
    out_rows.append(
        {k: _fmt_median(df["loss_asset_pct"], digits=1) for k, df in groups.items()}
    )
    out_index.append("")
    out_rows.append(
        {k: _fmt_sd(df["loss_asset_pct"], digits=1) for k, df in groups.items()}
    )

    out_index.append("Uninsured Deposit/MM Asset")
    out_rows.append(
        {
            k: _fmt_median(df["unins_dep_mm_asset_pct"], digits=1)
            for k, df in groups.items()
        }
    )
    out_index.append("")
    out_rows.append(
        {
            k: _fmt_sd(df["unins_dep_mm_asset_pct"], digits=1)
            for k, df in groups.items()
        }
    )

    out_index.append("Number of Banks")
    out_rows.append({k: f"{df.shape[0]:,}" for k, df in groups.items()})

    table = pd.DataFrame(out_rows, index=out_index)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_DIR / "table_1.csv"
    tex_path = OUTPUT_DIR / "table_1.tex"

    table.to_csv(csv_path)

    latex_str = _format_table_latex(table)
    with open(tex_path, "w", encoding="utf-8") as f:
        f.write(latex_str)

    print("\nTable 1:")
    print(table)
    print(f"\nSaved -> {csv_path}")
    print(f"Saved -> {tex_path}")


if __name__ == "__main__":
    main()