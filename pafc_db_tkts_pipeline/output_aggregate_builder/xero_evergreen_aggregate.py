from __future__ import annotations

"""Compute Xero evergreen totals from membership evergreen columns."""

import pandas as pd

from ..config import TICKETOFFICE_SALE_COMBINED_CSV
from ..logger import log

_COLUMN_NAME = "xero_evergreen"
_TOTAL_COLUMN = "mddto_evergreen_total"
_OTHER_COLUMN = "mddto_evergreen_other"


# any of these words inside the cell => treat as 0
_UNAVAILABLE_KEYWORDS = ("unavailable", "not available")


def _series_to_number(col: pd.Series) -> pd.Series:
    """
    Convert a string Series to float.

    - 'File Unavailable', 'Data Unavailable',
      'Data Not Available In File', etc. -> 0
    - blanks / 'nan' / 'null' / 'none' -> 0
    - commas removed, brackets "(123.45)" -> -123.45
    """

    s = col.astype(str).fillna("0").str.strip()

    lower = s.str.lower()
    mask_unavail = lower.str.contains("unavailable") | lower.str.contains("not available")
    mask_empty = lower.isin({"", "nan", "null", "none"})

    # set all unavailable / empty to "0"
    s = s.mask(mask_unavail | mask_empty, "0")

    # remove commas
    s = s.str.replace(",", "", regex=False)

    # brackets → negative: "(12.50)" → "-12.50"
    s = s.str.replace(r"\(([^)]+)\)", r"-\1", regex=True)

    # final conversion, anything weird becomes 0
    numeric = pd.to_numeric(s, errors="coerce").fillna(0.0)

    return numeric


def _format_result_series(values: pd.Series) -> list[str]:
    """Convert floats to strings, dropping .0 when it is an integer."""
    out: list[str] = []
    for v in values:
        fv = float(v)
        if fv.is_integer():
            out.append(str(int(fv)))
        else:
            out.append(str(fv))
    return out


def build_xero_evergreen_column() -> None:
    """Add Xero evergreen totals into aggregate_data.csv.

    xero_evergreen = mddto_evergreen_total - mddto_evergreen_other
    (unavailable / null / blank values are treated as 0 first)
    """

    try:
        base_df = pd.read_csv(TICKETOFFICE_SALE_COMBINED_CSV, dtype=str)
    except FileNotFoundError:
        log.error(
            "Xero evergreen aggregate – base file %s not found; cannot add '%s' column.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            _COLUMN_NAME,
        )
        return

    required_cols = {"date", _TOTAL_COLUMN, _OTHER_COLUMN}
    if not required_cols.issubset(base_df.columns):
        log.error(
            "Xero evergreen aggregate – base file %s missing required columns %s.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            sorted(required_cols),
        )
        return

    # robust numeric conversion
    total_num = _series_to_number(base_df[_TOTAL_COLUMN])
    other_num = _series_to_number(base_df[_OTHER_COLUMN])

    result = total_num - other_num  # <-- THIS WILL DO 138 - (-135) = 273, etc.

    base_df[_COLUMN_NAME] = _format_result_series(result)

    base_df.sort_values("date", inplace=True)
    base_df.reset_index(drop=True, inplace=True)
    base_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")

    log.info(
        "aggregate_data.csv updated with '%s' column derived from membership evergreen totals (%d row(s)).",
        _COLUMN_NAME,
        len(base_df),
    )


__all__ = ["build_xero_evergreen_column"]
