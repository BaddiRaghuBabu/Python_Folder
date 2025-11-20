from __future__ import annotations

"""Calculate ``actual_total`` column for aggregate_data.csv."""

import pandas as pd

from ..config import TICKETOFFICE_SALE_COMBINED_CSV
from ..logger import log

_COLUMN_NAME = "actual_total"
_DEPENDENT_COLUMNS = (
    "xero_booking_fee",
    "xero_postage",
    "xero_evergreen",
    "xero_on_account",
    "xero_ccdva_less_charges",
)


def _series_to_number(col: pd.Series) -> pd.Series:
    """Convert aggregate values to floats, treating placeholders as zero.

    Rules:
    - "File Unavailable", "Data Unavailable", "Data Not Available In File", etc. -> 0
    - blanks / "nan" / "null" / "none" -> 0
    - commas removed, bracket negatives supported: "(123.45)" -> "-123.45"
    - anything unparsable -> 0
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


def build_actual_total_column() -> None:
    """Add ``actual_total`` as the sum of relevant Xero columns.

    actual_total = xero_booking_fee + xero_postage + xero_evergreen
                   + xero_on_account + xero_ccdva_less_charges
    """

    try:
        base_df = pd.read_csv(TICKETOFFICE_SALE_COMBINED_CSV, dtype=str)
    except FileNotFoundError:
        log.error(
            "Actual total aggregate – base file %s not found; cannot add '%s' column.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            _COLUMN_NAME,
        )
        return

    missing = set(_DEPENDENT_COLUMNS).difference(base_df.columns)
    if missing:
        log.error(
            "Actual total aggregate – base file %s missing required columns %s.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            sorted(missing),
        )
        return

    if "date" not in base_df.columns:
        log.error(
            "Actual total aggregate – base file %s missing 'date' column.",
            TICKETOFFICE_SALE_COMBINED_CSV,
        )
        return

    numeric_columns = [_series_to_number(base_df[col]) for col in _DEPENDENT_COLUMNS]
    actual_total = sum(numeric_columns)

    base_df[_COLUMN_NAME] = actual_total.round(2).map(lambda x: f"{x:.2f}")
    base_df.sort_values("date", inplace=True)
    base_df.reset_index(drop=True, inplace=True)
    base_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")

    log.info(
        "aggregate_data.csv updated with '%s' column derived from actual totals (%d row(s)).",
        _COLUMN_NAME,
        len(base_df),
    )


__all__ = ["build_actual_total_column"]