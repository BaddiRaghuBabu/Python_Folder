from __future__ import annotations

"""Generate per-date Xero TKTS CSV exports from ``aggregate_data.csv``."""

from pathlib import Path

import pandas as pd
import re
from .config import (
    KLARNA_SEMOP_TABLE_OUTPUT_DIR,
    TICKETOFFICE_SALE_COMBINED_CSV,
    XERO_TKTS_OUTPUT_BASE_DIR,
) 
from .logger import log


_REQUIRED_COLUMNS = {
    "date",
    "xero_booking_fee",
    "xero_postage",
    "xero_on_account",
    "xero_evergreen",
    "mddto_miles_gross",
}

_RECONCILIATION_COLUMNS = {
    "actual_total",
    "expected_total",
    "ticketoffice_notes",
}


_EVENT_COLUMN = "Event"
_CCDVA_LESS_CHARGES_COLUMN = "ccdva_less_charges"


def _clean_value(raw_value: object) -> str | None:
    """Return cleaned string value, or ``None`` if empty/invalid."""

    text = str(raw_value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return None

    return text

def _format_date(value: str) -> str:
    """Ensure dates are 8-digit strings (YYYYMMDD)."""

    return str(value).strip().zfill(8)

def _parse_number(raw_value: object) -> float:
    """Convert numeric strings to floats, treating placeholders as zero."""

    text = str(raw_value).strip()
    lower = text.lower()
    if (
        not text
        or lower in {"nan", "none", "null"}
        or "unavailable" in lower
        or "not available" in lower
    ):
        return 0.0

    cleaned = text.replace(",", "")
    cleaned = re.sub(r"\(([^)]+)\)", r"-\1", cleaned)

    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _format_number(value: float) -> str:
    return f"{value:.2f}"


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

def _build_event_ccdva_rows(date: str) -> list[dict[str, str]]:
    """Return CCDVA less charges rows derived from Season/Event CSV exports."""

def _build_reconciliation_rows(
    date: str,
    actual_total: object,
    expected_total: object,
    ticketoffice_notes: object,
) -> list[dict[str, str]]:
    """Return reconciliation rows derived from actual/expected totals and notes."""

    actual_numeric = _parse_number(actual_total)
    expected_numeric = _parse_number(expected_total)
    difference_numeric = expected_numeric - actual_numeric

    actual_value = _clean_value(actual_total) or _format_number(actual_numeric)
    expected_value = _clean_value(expected_total) or _format_number(expected_numeric)
    difference_value = _format_number(difference_numeric)

    status = "Matched" if round(difference_numeric, 2) == 0 else "Not Matched"
    notes_value = _clean_value(ticketoffice_notes) or ""

    return [
        {
            "Date": date,
            "Heading": ">>>>>>>>>>>>>>>>>>Reconciliation Notes>>>>>>>>>>>>>>>>",
            "Value": "",
        },
        {"Date": date, "Heading": "Actual Total", "Value": actual_value},
        {"Date": date, "Heading": "Expected Total", "Value": expected_value},
        {"Date": date, "Heading": "Difference", "Value": difference_value},
        {"Date": date, "Heading": "Reconciliation Status", "Value": status},
        {"Date": date, "Heading": "Ticket Office Notes", "Value": notes_value},
    ]

    rows: list[dict[str, str]] = []

    csv_matches = sorted(KLARNA_SEMOP_TABLE_OUTPUT_DIR.glob(f"*{date}*.csv"))
    if not csv_matches:
        log.warning(
            "Xero TKTS output – no Season/Event CSV found for date %s; skipping CCDVA rows.",
            date,
        )
        return rows

    csv_path = csv_matches[0]
    try:
        df = pd.read_csv(csv_path, dtype=str)
    except Exception as exc:  # noqa: BLE001
        log.error(
            "Xero TKTS output – failed to read %s for date %s: %s.",
            csv_path.name,
            date,
            exc,
        )
        return rows

    required_cols = {_EVENT_COLUMN, _CCDVA_LESS_CHARGES_COLUMN}
    if not required_cols.issubset(df.columns):
        log.warning(
            "Xero TKTS output – %s missing required columns %s; skipping CCDVA rows for %s.",
            csv_path.name,
            sorted(required_cols),
            date,
        )
        return rows

    skip_events = {"total income", "xero_ccdva_less_charges-->"}
    for _, event_row in df.iterrows():
        event_name = _clean_value(event_row[_EVENT_COLUMN])
        if not event_name or event_name.casefold() in skip_events:
            continue

        value = _clean_value(event_row[_CCDVA_LESS_CHARGES_COLUMN])
        if value is None:
            continue

        rows.append(
            {
                "Date": date,
                "Heading": f"{event_name} /ccdva_less_charges",
                "Value": value,
            }
        )

    if not rows:
        log.info(
            "Xero TKTS output – no CCDVA rows emitted for %s from %s.",
            date,
            csv_path.name,
        )

    return rows




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

    required_columns = _REQUIRED_COLUMNS.union(_RECONCILIATION_COLUMNS)
    missing = required_columns.difference(base_df.columns)
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

        out_path: Path = date_folder / f"output_xero_tkts_{date}.csv"
        values: dict[str, str] = {}
        for col in _REQUIRED_COLUMNS:
            if col == "date":
                continue

            raw_value = str(row[col]).strip()
            values[col] = "0" if raw_value.lower() in {"", "nan", "none"} else raw_value

        rows = _build_rows(date, values)
        rows.extend(_build_event_ccdva_rows(date))
        pd.DataFrame(rows, columns=["Date", "Heading", "Value"]).to_csv(
            out_path, index=False, encoding="utf-8-sig"
        )
        rows.extend(
            _build_reconciliation_rows(
                date,
                row["actual_total"],
                row["expected_total"],
                row["ticketoffice_notes"],
            )
        )
        log.info(
            "Xero TKTS output – created %s with %d row(s).",
            out_path,
            len(rows),
        )



__all__ = ["build_xero_ticket_outputs"]