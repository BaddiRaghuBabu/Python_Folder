"""Calculate Xero on account values inside ``aggregate_data.csv``.

The column is derived from Klarna DailyTakings values:

    xero_on_account = (k_dailytakings_voucher + k_dailytakings_account) * -1

Unavailable placeholders such as "File Unavailable" or "Data Unavailable" are
treated as zero before performing the calculation so that the pipeline remains
resilient.
"""

from __future__ import annotations

import pandas as pd

from ..config import TICKETOFFICE_SALE_COMBINED_CSV
from ..logger import log

_COLUMN_NAME = "xero_on_account"
_DEPENDENT_COLUMNS = {"k_dailytakings_voucher", "k_dailytakings_account"}


def _series_to_number(col: pd.Series) -> pd.Series:
    """Convert aggregate Klarna values into floats.

    Rules:
    - "File Unavailable" / "Data Unavailable" / "Data Not Available In File"
      are treated as 0
    - blanks / "nan" / "null" / "none" are treated as 0
    - commas removed, bracket negatives supported: "(123.45)" -> "-123.45"
    - anything unparsable becomes 0
    """

    s = col.astype(str).fillna("0").str.strip()

    lower = s.str.lower()
    mask_unavailable = lower.str.contains("unavailable") | lower.str.contains(
        "not available"
    )
    mask_empty = lower.isin({"", "nan", "null", "none"})

    s = s.mask(mask_unavailable | mask_empty, "0")
    s = s.str.replace(",", "", regex=False)
    s = s.str.replace(r"\(([^)]+)\)", r"-\1", regex=True)

    numeric = pd.to_numeric(s, errors="coerce").fillna(0.0)
    return numeric


def build_xero_on_account_column() -> None:
    """Add ``xero_on_account`` column into ``aggregate_data.csv``."""

    try:
        base_df = pd.read_csv(TICKETOFFICE_SALE_COMBINED_CSV, dtype=str)
    except FileNotFoundError:
        log.error(
            "Xero on account aggregate – base file %s not found; cannot add '%s' column.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            _COLUMN_NAME,
        )
        return

    missing = _DEPENDENT_COLUMNS.difference(base_df.columns)
    if missing:
        log.error(
            "Xero on account aggregate – base file %s missing required columns %s.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            sorted(missing),
        )
        return

    if "date" not in base_df.columns:
        log.error(
            "Xero on account aggregate – base file %s missing 'date' column.",
            TICKETOFFICE_SALE_COMBINED_CSV,
        )
        return

    voucher_num = _series_to_number(base_df["k_dailytakings_voucher"])
    account_num = _series_to_number(base_df["k_dailytakings_account"])

    on_account = (voucher_num + account_num) * -1

    base_df[_COLUMN_NAME] = on_account.round(2).map(lambda x: f"{x:.2f}")
    base_df.sort_values("date", inplace=True)
    base_df.reset_index(drop=True, inplace=True)
    base_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")

    log.info(
        "aggregate_data.csv updated with '%s' column derived from Klarna on account totals (%d row(s)).",
        _COLUMN_NAME,
        len(base_df),
    )


__all__ = ["build_xero_on_account_column"]