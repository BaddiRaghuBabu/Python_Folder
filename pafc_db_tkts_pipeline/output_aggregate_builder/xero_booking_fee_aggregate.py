from __future__ import annotations

"""Calculate Xero booking fee values inside aggregate_data.csv."""

import pandas as pd

from ..config import TICKETOFFICE_SALE_COMBINED_CSV
from ..logger import log

_COLUMN_NAME = "xero_booking_fee"
_DEPENDENT_COLUMNS = {"charges_total", "charges_postal"}


def _series_to_number(col: pd.Series) -> pd.Series:
    """
    Convert a string Series to float.

    Rules:
    - 'File Unavailable', 'Data Unavailable',
      'Data Not Available In File', etc. -> 0
    - blanks / 'nan' / 'null' / 'none' -> 0
    - commas removed, brackets "(123.45)" -> -123.45
    - anything unparsable -> 0
    """
    s = col.astype(str).fillna("0").str.strip()

    lower = s.str.lower()
    mask_unavail = lower.str.contains("unavailable") | lower.str.contains("not available")
    mask_empty = lower.isin({"", "nan", "null", "none"})

    # treat all unavailable / empty as 0
    s = s.mask(mask_unavail | mask_empty, "0")

    # remove commas
    s = s.str.replace(",", "", regex=False)

    # bracket negatives: "(12.50)" -> "-12.50"
    s = s.str.replace(r"\(([^)]+)\)", r"-\1", regex=True)

    numeric = pd.to_numeric(s, errors="coerce").fillna(0.0)
    return numeric


def build_xero_booking_fee_column() -> None:
    """Add ``xero_booking_fee`` column derived from charges totals.

    xero_booking_fee = charges_total - charges_postal
    """

    try:
        base_df = pd.read_csv(TICKETOFFICE_SALE_COMBINED_CSV, dtype=str)
    except FileNotFoundError:
        log.error(
            "Xero booking fee aggregate – base file %s not found; cannot add '%s' column.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            _COLUMN_NAME,
        )
        return

    missing = _DEPENDENT_COLUMNS.difference(base_df.columns)
    if missing:
        log.error(
            "Xero booking fee aggregate – base file %s missing required columns %s.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            sorted(missing),
        )
        return

    if "date" not in base_df.columns:
        log.error(
            "Xero booking fee aggregate – base file %s missing 'date' column.",
            TICKETOFFICE_SALE_COMBINED_CSV,
        )
        return

    charges_total_num = _series_to_number(base_df["charges_total"])
    charges_postal_num = _series_to_number(base_df["charges_postal"])

    booking_fee = charges_total_num - charges_postal_num

    # format as 2-decimal currency string, e.g. 753.75 / 0.00
    base_df[_COLUMN_NAME] = booking_fee.round(2).map(lambda x: f"{x:.2f}")

    base_df.sort_values("date", inplace=True)
    base_df.reset_index(drop=True, inplace=True)
    base_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")

    log.info(
        "aggregate_data.csv updated with '%s' column derived from charges totals (%d row(s)).",
        _COLUMN_NAME,
        len(base_df),
    )


__all__ = ["build_xero_booking_fee_column"]
