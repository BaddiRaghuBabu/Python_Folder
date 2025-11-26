from __future__ import annotations

"""Generate per-date Xero TKTS CSV exports from ``aggregate_data.csv``."""

from pathlib import Path

import pandas as pd

from .config import TICKETOFFICE_SALE_COMBINED_CSV, XERO_TKTS_OUTPUT_BASE_DIR
from .logger import log


_REQUIRED_COLUMNS = {
    "date",
    "xero_booking_fee",
    "xero_postage",
    "xero_on_account",
    "xero_evergreen",
    "mddto_miles_gross",
}


def _format_date(value: str) -> str:
    """Ensure dates are 8-digit strings (YYYYMMDD)."""

    return str(value).strip().zfill(8)


def _build_rows(date: str, values: dict[str, str]) -> list[dict[str, str]]:
    """Return the output rows for a single date."""

    zero_value = "0"

    return [
        {"Date": date, "Heading": "BOOKING FEE", "Value": values["xero_booking_fee"]},
        {"Date": date, "Heading": "POSTAGE", "Value": values["xero_postage"]},
        {"Date": date, "Heading": "ON ACCOUNT", "Value": values["xero_on_account"]},
        {"Date": date, "Heading": "EVERGREEN", "Value": values["xero_evergreen"]},
        {
            "Date": date,
            "Heading": "MILES AWAY TRAVEL CLUB",
            "Value": values["mddto_miles_gross"],
        },
        {"Date": date, "Heading": "UNALLOCATED", "Value": zero_value},
        {"Date": date, "Heading": "REFUND", "Value": zero_value},
        {"Date": date, "Heading": "GIFT CARD", "Value": zero_value},
        {"Date": date, "Heading": "VOUCHER", "Value": zero_value},
    ]


def build_xero_ticket_outputs() -> None:
    """Create Xero TKTS CSV files for each date found in ``aggregate_data.csv``."""

    try:
        base_df = pd.read_csv(TICKETOFFICE_SALE_COMBINED_CSV, dtype=str)
    except FileNotFoundError:
        log.error(
            "Xero TKTS output – base file %s not found; cannot create per-date exports.",
            TICKETOFFICE_SALE_COMBINED_CSV,
        )
        return

    missing = _REQUIRED_COLUMNS.difference(base_df.columns)
    if missing:
        log.error(
            "Xero TKTS output – base file %s missing required columns %s.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            sorted(missing),
        )
        return

    if base_df.empty:
        log.warning(
            "Xero TKTS output – base file %s is empty; no per-date exports created.",
            TICKETOFFICE_SALE_COMBINED_CSV,
        )
        return

    # Ensure destination root exists up front.
    XERO_TKTS_OUTPUT_BASE_DIR.mkdir(parents=True, exist_ok=True)

    # Normalise date column for consistency.
    base_df["date"] = base_df["date"].map(_format_date)

    for _, row in base_df.iterrows():
        date = row["date"]
        date_folder = XERO_TKTS_OUTPUT_BASE_DIR / f"output_xero_tkts_{date}"
        date_folder.mkdir(parents=True, exist_ok=True)

        out_path: Path =  f"output_xero_tkts_{date}.csv"
        values: dict[str, str] = {}
        for col in _REQUIRED_COLUMNS:
            if col == "date":
                continue

            raw_value = str(row[col]).strip()
            values[col] = "0" if raw_value.lower() in {"", "nan", "none"} else raw_value

        rows = _build_rows(date, values)
        pd.DataFrame(rows, columns=["Date", "Heading", "Value"]).to_csv(
            out_path, index=False, encoding="utf-8-sig"
        )

        log.info(
            "Xero TKTS output – created %s with %d row(s).",
            out_path,
            len(rows),
        )


__all__ = ["build_xero_ticket_outputs"]