from __future__ import annotations

import pandas as pd

from ..config import KLARNA_CSV, TICKETOFFICE_SALE_COMBINED_CSV
from ..logger import log

_K_COLS = [
    "k_dailytakings_cash",
    "k_dailytakings_credit",
    "k_dailytakings_debit",
    "k_dailytakings_voucher",
    "k_dailytakings_account",
]


def build_klarna_dailytakings_data_columns() -> None:
    """
    Merge Klarna DailyTakings MoP totals into aggregate_data.csv.

    - Base file:  aggregate_data.csv (TICKETOFFICE_SALE_COMBINED_CSV)
    - Source:     klarna_dailytakings_summary.csv (KLARNA_CSV)

    Rules:
      * If Klarna *file* is missing altogether → all 5 cols = 'File Unavailable'
      * If Klarna *row for that date* is missing → 'File Unavailable' for that date
      * If Klarna row exists but specific MoP value is missing/NaN →
            'Data Unavailable' (data missing inside that PDF)
    """
    # Load base aggregate
    try:
        base_df = pd.read_csv(TICKETOFFICE_SALE_COMBINED_CSV, dtype=str)
    except FileNotFoundError:
        log.error(
            "Klarna DailyTakings aggregate – base file %s not found; "
            "cannot add k_dailytakings_* columns.",
            TICKETOFFICE_SALE_COMBINED_CSV,
        )
        return

    # Normalise base date
    if "date" in base_df.columns:
        base_df["date"] = base_df["date"].astype(str).str.zfill(8)

    # ------------------------------------------------------------------
    # Case 1: Klarna summary file completely missing → File Unavailable
    # ------------------------------------------------------------------
    try:
        k_df = pd.read_csv(KLARNA_CSV, dtype=str)
    except FileNotFoundError:
        log.warning(
            "Klarna DailyTakings aggregate – %s not found; "
            "setting all k_dailytakings_* columns to 'File Unavailable'.",
            KLARNA_CSV,
        )
        for col in _K_COLS:
            if col not in base_df.columns:
                base_df[col] = "File Unavailable"
            else:
                base_df[col] = base_df[col].fillna("File Unavailable")

        base_df.to_csv(
            TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig"
        )
        return

    # Need 'date' column in Klarna summary
    if "date" not in k_df.columns:
        log.error(
            "Klarna DailyTakings aggregate – 'date' column missing from %s; "
            "treating all Klarna values as 'File Unavailable'.",
            KLARNA_CSV,
        )
        for col in _K_COLS:
            if col not in base_df.columns:
                base_df[col] = "File Unavailable"
            else:
                base_df[col] = base_df[col].fillna("File Unavailable")
        base_df.to_csv(
            TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig"
        )
        return

    # Normalise Klarna dates
    k_df["date"] = k_df["date"].astype(str).str.zfill(8)

    # Which dates actually have *some* Klarna row?
    dates_with_klarna = set(k_df["date"].unique())

    # Keep only the columns we care about
    cols_from_k = ["date"] + [c for c in _K_COLS if c in k_df.columns]
    k_sub = k_df[cols_from_k]

    # Left-join onto base on date
    merged = base_df.merge(k_sub, on="date", how="left")

    # Ensure all 5 columns exist
    for col in _K_COLS:
        if col not in merged.columns:
            merged[col] = pd.NA

    has_file_mask = merged["date"].isin(dates_with_klarna)
    no_file_mask = ~has_file_mask

    # ------------------------------------------------------------------
    # Rows where THERE IS NO Klarna file for that date → File Unavailable
    # ------------------------------------------------------------------
    for col in _K_COLS:
        merged.loc[no_file_mask, col] = "File Unavailable"

    # ------------------------------------------------------------------
    # Rows where there IS a Klarna file, but value is missing/NaN → Data Unavailable
    # (Note: if extractor already wrote 'Data Unavailable', that's a string,
    # not NaN, so we leave it alone.)
    # ------------------------------------------------------------------
    for col in _K_COLS:
        merged.loc[has_file_mask & merged[col].isna(), col] = "Data Unavailable"

    merged.to_csv(
        TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig"
    )

    log.info(
        "Klarna DailyTakings aggregate – columns %s ensured on %s (%d row(s)).",
        ", ".join(_K_COLS),
        TICKETOFFICE_SALE_COMBINED_CSV,
        len(merged),
    )
