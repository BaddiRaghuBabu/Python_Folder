from __future__ import annotations

"""Calculate ``Status`` column by comparing ``actual_total`` and ``expected_total``."""

import pandas as pd

from ..config import TICKETOFFICE_SALE_COMBINED_CSV
from ..logger import log

_COLUMN_NAME = "Status"
_DEPENDENT_COLUMNS = ("actual_total", "expected_total")


def _series_to_number(col: pd.Series) -> pd.Series:
    """Convert total values to floats, treating placeholders as zero.

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


def build_status_expected_and_actual_total_column() -> None:
    """Add ``Status`` column based on whether actual and expected totals match."""

    try:
        base_df = pd.read_csv(TICKETOFFICE_SALE_COMBINED_CSV, dtype=str)
    except FileNotFoundError:
        log.error(
            "Status aggregate – base file %s not found; cannot add '%s' column.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            _COLUMN_NAME,
        )
        return

    missing = set(_DEPENDENT_COLUMNS).difference(base_df.columns)
    if missing:
        log.error(
            "Status aggregate – base file %s missing required columns %s.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            sorted(missing),
        )
        return

    actual_numeric = _series_to_number(base_df["actual_total"])
    expected_numeric = _series_to_number(base_df["expected_total"])

    matches = actual_numeric.round(2).eq(expected_numeric.round(2))
    base_df[_COLUMN_NAME] = matches.map({True: "Matched", False: "Not Matched"})

    if "date" in base_df.columns:
        base_df.sort_values("date", inplace=True)
        base_df.reset_index(drop=True, inplace=True)

    base_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")

    log.info(
        "aggregate_data.csv updated with '%s' column comparing expected vs actual totals (%d row(s)).",
        _COLUMN_NAME,
        len(base_df),
    )


__all__ = ["build_status_expected_and_actual_total_column"]