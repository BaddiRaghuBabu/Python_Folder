from __future__ import annotations

"""Merge CCDVA less charges totals into aggregate_data.csv."""

import re

import pandas as pd

from ..config import KLARNA_SEMOP_TABLE_OUTPUT_DIR, TICKETOFFICE_SALE_COMBINED_CSV
from ..logger import log

_COLUMN_NAME = "xero_ccdva_less_charges"
_EVENT_LABEL = "xero_ccdva_less_charges-->"
_VALUE_COLUMN = "ccdva_less_charges"


def _extract_iso_date_from_stem(stem: str) -> str | None:
    match = re.search(r"(\d{8})", stem)
    if match:
        return match.group(1)
    return None


def _clean_value(value: object) -> str | None:
    if value is None:
        return None

    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned or cleaned.lower() == "nan":
            return None
        return cleaned

    if pd.isna(value):
        return None

    return str(value)


def _load_totals_by_date() -> tuple[dict[str, str], set[str]]:
    """Return mapping of date to CCDVA less charges total and set of available dates."""

    totals_by_date: dict[str, str] = {}
    available_dates: set[str] = set()

    for csv_file in sorted(KLARNA_SEMOP_TABLE_OUTPUT_DIR.glob("*.csv")):
        date = _extract_iso_date_from_stem(csv_file.stem)
        if not date:
            log.warning(
                "CCDVA less charges aggregate – could not determine date from %s; skipping.",
                csv_file.name,
            )
            continue

        available_dates.add(date)

        try:
            df = pd.read_csv(csv_file, dtype=str)
        except Exception as exc:  # noqa: BLE001
            log.error(
                "CCDVA less charges aggregate – failed to read %s: %s.",
                csv_file.name,
                exc,
            )
            totals_by_date.setdefault(date, "Data Unavailable")
            continue

        required_cols = {"Event", _VALUE_COLUMN}
        if not required_cols.issubset(df.columns):
            log.warning(
                "CCDVA less charges aggregate – %s missing required columns %s; "
                "marking date %s as unavailable.",
                csv_file.name,
                sorted(required_cols),
                date,
            )
            totals_by_date.setdefault(date, "Data Unavailable")
            continue

        event_mask = (
            df["Event"].astype(str).str.strip().str.casefold()
            == _EVENT_LABEL.casefold()
        )
        if not event_mask.any():
            log.warning(
                "CCDVA less charges aggregate – total row '%s' not found in %s; "
                "marking date %s as unavailable.",
                _EVENT_LABEL,
                csv_file.name,
                date,
            )
            totals_by_date.setdefault(date, "Data Unavailable")
            continue

        value = df.loc[event_mask, _VALUE_COLUMN].iloc[0]
        cleaned = _clean_value(value)
        if cleaned is None:
            totals_by_date.setdefault(date, "Data Unavailable")
        else:
            totals_by_date[date] = cleaned

    return totals_by_date, available_dates


def build_xero_ccdva_less_charges_column() -> None:
    """Add CCDVA less charges totals into aggregate_data.csv."""

    try:
        base_df = pd.read_csv(TICKETOFFICE_SALE_COMBINED_CSV, dtype=str)
    except FileNotFoundError:
        log.error(
            "CCDVA less charges aggregate – base file %s not found; cannot add '%s' column.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            _COLUMN_NAME,
        )
        return

    if "date" not in base_df.columns:
        log.error(
            "CCDVA less charges aggregate – base file %s missing 'date' column.",
            TICKETOFFICE_SALE_COMBINED_CSV,
        )
        return

    base_df["date"] = base_df["date"].astype(str).str.zfill(8)

    if not KLARNA_SEMOP_TABLE_OUTPUT_DIR.exists():
        log.error(
            "CCDVA less charges aggregate – directory %s not found; setting '%s' "
            "to 'File Unavailable'.",
            KLARNA_SEMOP_TABLE_OUTPUT_DIR,
            _COLUMN_NAME,
        )
        base_df[_COLUMN_NAME] = "File Unavailable"
        base_df.sort_values("date", inplace=True)
        base_df.reset_index(drop=True, inplace=True)
        base_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")
        return

    totals_by_date, available_dates = _load_totals_by_date()

    column_values: list[str] = []
    for date in base_df["date"]:
        if date in totals_by_date:
            column_values.append(totals_by_date[date])
        elif date in available_dates:
            column_values.append("Data Unavailable")
        else:
            column_values.append("File Unavailable")

    base_df[_COLUMN_NAME] = column_values
    base_df.sort_values("date", inplace=True)
    base_df.reset_index(drop=True, inplace=True)
    base_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")

    log.info(
        "aggregate_data.csv updated with '%s' column using Klarna Season/Event totals from %s (%d row(s)).",
        _COLUMN_NAME,
        KLARNA_SEMOP_TABLE_OUTPUT_DIR,
        len(base_df),
    )


__all__ = ["build_xero_ccdva_less_charges_column"]