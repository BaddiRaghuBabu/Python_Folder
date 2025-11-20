from __future__ import annotations

"""Calculate Xero postage values inside aggregate_data.csv."""

import pandas as pd

from ..config import TICKETOFFICE_SALE_COMBINED_CSV
from ..logger import log

_COLUMN_NAME = "xero_postage"
_DEPENDENT_COLUMNS = {"charges_postal", "saleitemsmop_total"}


def _normalise_value(value: object) -> float:
    """Convert aggregate string values into floats.

    The dependent columns may contain placeholders like "File Unavailable" or
    "Data Unavailable"; per requirements these are treated as zero. Numeric
    strings that include commas or parentheses are also supported. Any
    unparsable value is logged and treated as zero to keep the pipeline
    resilient.
    """

    if value is None:
        return 0.0

    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() == "nan":
            return 0.0

        placeholder = text.casefold()
        if placeholder in {"file unavailable", "data unavailable"}:
            return 0.0

        negative = text.startswith("(") and text.endswith(")")
        cleaned = text.strip("()").replace(",", "")
    else:
        if pd.isna(value):
            return 0.0
        negative = False
        cleaned = str(value)

    try:
        number = float(cleaned)
    except ValueError:
        log.warning(
            "Xero postage aggregate – could not parse value '%s'; using 0.",
            value,
        )
        return 0.0

    if negative:
        number = -number
    return number


def build_xero_postage_column() -> None:
    """Add ``xero_postage`` column derived from postal charges and MoP totals."""

    try:
        base_df = pd.read_csv(TICKETOFFICE_SALE_COMBINED_CSV, dtype=str)
    except FileNotFoundError:
        log.error(
            "Xero postage aggregate – base file %s not found; cannot add '%s' column.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            _COLUMN_NAME,
        )
        return

    missing = _DEPENDENT_COLUMNS.difference(base_df.columns)
    if missing:
        log.error(
            "Xero postage aggregate – base file %s missing required columns %s.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            sorted(missing),
        )
        return

    column_values: list[str] = []
    for _, row in base_df.iterrows():
        charges_postal = _normalise_value(row.get("charges_postal"))
        mop_total = _normalise_value(row.get("saleitemsmop_total"))
        postage_total = charges_postal + mop_total
        column_values.append(f"{postage_total:.2f}")

    base_df[_COLUMN_NAME] = column_values
    base_df.sort_values("date", inplace=True)
    base_df.reset_index(drop=True, inplace=True)
    base_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")

    log.info(
        "aggregate_data.csv updated with '%s' column derived from postal charges (%d row(s)).",
        _COLUMN_NAME,
        len(base_df),
    )


__all__ = ["build_xero_postage_column"]