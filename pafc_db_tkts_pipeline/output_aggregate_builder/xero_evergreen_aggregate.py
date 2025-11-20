from __future__ import annotations

"""Compute Xero evergreen totals from membership evergreen columns."""

import pandas as pd

from ..config import TICKETOFFICE_SALE_COMBINED_CSV
from ..logger import log

_COLUMN_NAME = "xero_evergreen"
_TOTAL_COLUMN = "mddto_evergreen_total"
_OTHER_COLUMN = "mddto_evergreen_other"


_UNAVAILABLE_MARKERS = {"file unavailable", "data unavailable"}


def _coerce_to_number(value: object) -> float:
    """Convert numeric-looking values to float; treat unavailable as 0."""

    if value is None:
        return 0.0

    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned or cleaned.lower() == "nan":
            return 0.0
        if cleaned.lower() in _UNAVAILABLE_MARKERS:
            return 0.0
        cleaned = cleaned.replace(",", "")
    else:
        cleaned = value

    try:
        numeric_value = float(cleaned)
    except Exception:  # noqa: BLE001
        return 0.0

    if pd.isna(numeric_value):
        return 0.0

    return float(numeric_value)


def _format_result(value: float) -> str:
    return str(int(value)) if value.is_integer() else str(value)


def build_xero_evergreen_column() -> None:
    """Add Xero evergreen totals into aggregate_data.csv.

    Calculates ``mddto_evergreen_total - mddto_evergreen_other`` per row.
    Missing/unavailable inputs are treated as 0 before subtraction.
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

    totals = base_df[_TOTAL_COLUMN].astype(str)
    others = base_df[_OTHER_COLUMN].astype(str)

    evergreen_values: list[str] = []
    for total_raw, other_raw in zip(totals, others):
        total_val = _coerce_to_number(total_raw)
        other_val = _coerce_to_number(other_raw)
        evergreen_values.append(_format_result(total_val - other_val))

    base_df[_COLUMN_NAME] = evergreen_values
    base_df.sort_values("date", inplace=True)
    base_df.reset_index(drop=True, inplace=True)
    base_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")

    log.info(
        "aggregate_data.csv updated with '%s' column derived from membership evergreen totals (%d row(s)).",
        _COLUMN_NAME,
        len(base_df),
    )


__all__ = ["build_xero_evergreen_column"]