from __future__ import annotations

"""Generate per-date Xero TKTS CSV exports from ``aggregate_data.csv``."""

from pathlib import Path
import re

import pandas as pd

from .config import (
    KLARNA_SEMOP_TABLE_OUTPUT_DIR,
    TICKETOFFICE_SALE_COMBINED_CSV,
    XERO_TKTS_OUTPUT_BASE_DIR,
)
from .logger import log


_REQUIRED_COLUMNS: set[str] = {
    "date",
    "xero_booking_fee",
    "xero_postage",
    "xero_on_account",
    "xero_evergreen",
    "mddto_miles_gross",
    "actual_total",
    "expected_total",
    "ticketoffice_notes",
}

_EVENT_COLUMN = "Event"
_CCDVA_LESS_CHARGES_COLUMN = "ccdva_less_charges"

_SKIP_EVENTS = {"total income", "xero_ccdva_less_charges-->"}


def _clean_value(raw_value: object) -> str | None:
    """Return cleaned string value, or ``None`` if empty/invalid."""
    text = str(raw_value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    return text


def _format_date(value: str) -> str:
    """Ensure dates are 8-digit strings (YYYYMMDD)."""
    return str(value).strip().zfill(8)


def _parse_amount(raw_value: object) -> float:
    """Convert formatted currency strings to floats.

    Rules:
    - Strip commas.
    - Support bracket negatives: (123.45) -> -123.45
    - Empty / invalid -> 0.0
    """
    cleaned = str(raw_value).strip()
    if cleaned.lower() in {"", "nan", "none", "null"}:
        return 0.0

    cleaned = cleaned.replace(",", "")
    cleaned = re.sub(r"\(([^)]+)\)", r"-\1", cleaned)

    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _format_amount(amount: float) -> str:
    """Format numeric amount to two decimal places as a string."""
    return f"{amount:.2f}"


def _build_core_rows(date: str, values: dict[str, str]) -> list[dict[str, str]]:
    """Return the core Xero TKTS rows for a single date."""
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

    for _, event_row in df.iterrows():
        event_name = _clean_value(event_row[_EVENT_COLUMN])
        if not event_name:
            continue

        if event_name.casefold() in _SKIP_EVENTS:
            continue

        value = _clean_value(event_row[_CCDVA_LESS_CHARGES_COLUMN])
        if value is None:
            continue

        rows.append(
            {
                "Date": date,
                "Heading": event_name,
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


def _build_reconciliation_rows(date: str, values: dict[str, str]) -> list[dict[str, str]]:
    """Return reconciliation summary rows for a given date.

    Layout:
    - Banner row: text in ``Date`` column (full left), Heading/Value empty.
    - Other rows: blank ``Date``, normal Heading/Value.
    """
    actual = _parse_amount(values.get("actual_total", "0"))
    expected = _parse_amount(values.get("expected_total", "0"))
    difference = expected - actual
    status = "Matched" if round(difference, 2) == 0 else "Not Matched"

    notes = _clean_value(values.get("ticketoffice_notes", "")) or ""

    # Banner row – full left side (Date column)
    heading_row = {
        "Date": ">>>>>>>>>>>>>>>>Reconciliation Notes>>>>>>>>>>>>>>>>",
        "Heading": "",
        "Value": "",
    }

    # Detail rows – Date intentionally blank
    summary_rows = [
        {"Date": "", "Heading": "Actual Total", "Value": _format_amount(actual)},
        {"Date": "", "Heading": "Expected Total", "Value": _format_amount(expected)},
        {"Date": "", "Heading": "Difference", "Value": _format_amount(difference)},
        {"Date": "", "Heading": "Reconciliation Status", "Value": status},
        {"Date": "", "Heading": "Ticket Office Notes", "Value": notes},
    ]

    return [heading_row, *summary_rows]


def build_xero_ticket_outputs() -> None:
    """Create Xero TKTS CSV files for each date found in ``aggregate_data.csv``.

    Path layout:

    XERO_TKTS_OUTPUT_BASE_DIR /
        "output_xero_tkts_YYYYMMDD" /
            output_xero_tkts_<YYYYMMDD>.csv
    """
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

    # Root directory from config: outputs/output_xero_tkts
    XERO_TKTS_OUTPUT_BASE_DIR.mkdir(parents=True, exist_ok=True)

    # Fixed subfolder name: output_xero_tkts_YYYYMMDD
    output_folder = XERO_TKTS_OUTPUT_BASE_DIR / "output_xero_tkts_YYYYMMDD"
    output_folder.mkdir(parents=True, exist_ok=True)

    # Normalise date column for consistency.
    base_df["date"] = base_df["date"].map(_format_date)

    for _, row in base_df.iterrows():
        date = row["date"]

        out_path: Path = output_folder / f"output_xero_tkts_{date}.csv"

        # Build values dict with safe defaults.
        values: dict[str, str] = {}
        for col in _REQUIRED_COLUMNS:
            if col == "date":
                continue

            raw_value = str(row[col]).strip()
            values[col] = "0" if raw_value.lower() in {"", "nan", "none", "null"} else raw_value

        rows: list[dict[str, str]] = []

        # 1) Core rows
        rows.extend(_build_core_rows(date, values))

        # 2) Event CCDVA rows
        rows.extend(_build_event_ccdva_rows(date))

        # 3) Reconciliation block at the very end
        rows.extend(_build_reconciliation_rows(date, values))

        pd.DataFrame(rows, columns=["Date", "Heading", "Value"]).to_csv(
            out_path,
            index=False,
        )

        log.info(
            "Xero TKTS output – created %s with %d row(s).",
            out_path,
            len(rows),
        )


__all__ = ["build_xero_ticket_outputs"]
