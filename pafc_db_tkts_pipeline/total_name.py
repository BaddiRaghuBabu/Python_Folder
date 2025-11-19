from __future__ import annotations

"""Logic for extracting Charges total names from the totals workbook."""

import pandas as pd

from .config import CHARGES_TOTALS_OUTPUT_DIR
from .logger import log
from .event_ import clean_series, unique_preserve_order

_TOTALS_WORKBOOK = "charges_totals_all_dates.xlsx"
_SKIP_TOTAL_NAME_VALUES = {"total income"}


def collect_total_names() -> list[str]:
    """Gather distinct total_name entries from the totals Excel workbook."""

    totals_workbook = CHARGES_TOTALS_OUTPUT_DIR / _TOTALS_WORKBOOK
    if not totals_workbook.exists():
        log.warning(
            "Charges/Klarna report – totals workbook %s not found.",
            totals_workbook,
        )
        return []

    try:
        df = pd.read_excel(totals_workbook)
    except Exception as exc:  # noqa: BLE001
        log.error(
            "Charges/Klarna report – failed to read totals workbook %s: %s",
            totals_workbook,
            exc,
        )
        return []

    if "total_name" not in df.columns:
        log.warning(
            "Charges/Klarna report – totals workbook %s missing 'total_name'; skipping.",
            totals_workbook,
        )
        return []

    total_names: list[str] = []
    for value in clean_series(df["total_name"]):
        if value.casefold() in _SKIP_TOTAL_NAME_VALUES:
            continue
        total_names.append(value)

    return unique_preserve_order(total_names)


__all__ = ["collect_total_names"]