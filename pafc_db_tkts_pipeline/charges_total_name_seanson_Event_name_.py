from __future__ import annotations

"""Build a helper report with Klarna Season/Event names and Charges total names."""

from pathlib import Path

import pandas as pd

from .config import (
    CHARGES_EVENT_TOTAL_REPORT_DIR,
    CHARGES_EVENT_TOTAL_REPORT_XLSX,
    CHARGES_TOTALS_OUTPUT_DIR,
    KLARNA_SEMOP_TABLE_OUTPUT_DIR,
)
from .logger import log


_TOTALS_WORKBOOK = "charges_totals_all_dates.xlsx"
_EVENT_COLUMN = "Event"
_SKIP_EVENT_VALUES = {"total for the period"}
_SKIP_TOTAL_NAME_VALUES = {"total income"}


def _clean_series(values: pd.Series) -> list[str]:
    cleaned: list[str] = []
    for value in values.dropna().astype(str):
        stripped = value.strip()
        if not stripped:
            continue
        cleaned.append(stripped)
    return cleaned


def _collect_events() -> list[str]:
    csv_files = sorted(KLARNA_SEMOP_TABLE_OUTPUT_DIR.glob("*.csv"))
    if not csv_files:
        log.warning(
            "Charges/Klarna report – no Season/Event CSV files found in %s.",
            KLARNA_SEMOP_TABLE_OUTPUT_DIR,
        )
        return []

    events: list[str] = []
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
        except Exception as exc:  # noqa: BLE001
            log.error(
                "Charges/Klarna report – failed to read Season/Event CSV %s: %s",
                csv_file,
                exc,
            )
            continue

        if _EVENT_COLUMN not in df.columns:
            log.warning(
                "Charges/Klarna report – file %s is missing '%s' column; skipping.",
                csv_file.name,
                _EVENT_COLUMN,
            )
            continue

        for value in _clean_series(df[_EVENT_COLUMN]):
            if value.casefold() in _SKIP_EVENT_VALUES:
                continue
            events.append(value)

    return events


def _collect_total_names() -> list[str]:
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
    for value in _clean_series(df["total_name"]):
        if value.casefold() in _SKIP_TOTAL_NAME_VALUES:
            continue
        total_names.append(value)

    return total_names


def _write_report(events: list[str], total_names: list[str]) -> Path | None:
    if not events and not total_names:
        log.warning(
            "Charges/Klarna report – no Event or total_name data available; report not created."
        )
        return None

    CHARGES_EVENT_TOTAL_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    max_len = max(len(events), len(total_names))
    padded_events = events + [None] * (max_len - len(events))
    padded_totals = total_names + [None] * (max_len - len(total_names))

    df = pd.DataFrame({"Event": padded_events, "total_name": padded_totals})
    df.to_excel(CHARGES_EVENT_TOTAL_REPORT_XLSX, index=False)
    log.info(
        "Charges/Klarna report – wrote %s with %d rows.",
        CHARGES_EVENT_TOTAL_REPORT_XLSX,
        len(df),
    )
    return CHARGES_EVENT_TOTAL_REPORT_XLSX


def generate_charges_total_name_season_event_report() -> Path | None:
    """Create an Excel report with Season/Event names and Charges total names."""

    events = _collect_events()
    totals = _collect_total_names()
    return _write_report(events, totals)


__all__ = ["generate_charges_total_name_season_event_report"]