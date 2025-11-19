from __future__ import annotations

"""Logic for extracting Season/Event names from Klarna CSV exports."""

import pandas as pd

from .config import KLARNA_SEMOP_TABLE_OUTPUT_DIR
from .logger import log

_EVENT_COLUMN = "Event"
_SKIP_EVENT_VALUES = {"total for the period"}


def clean_series(values: pd.Series) -> list[str]:
    """Normalize a pandas series by stripping whitespace and removing empties."""

    cleaned: list[str] = []
    for value in values.dropna().astype(str):
        stripped = value.strip()
        if not stripped:
            continue
        cleaned.append(stripped)
    return cleaned


def unique_preserve_order(values: list[str]) -> list[str]:
    """Return values preserving first occurrence order while dropping duplicates."""

    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique.append(value)
    return unique


def collect_events() -> list[str]:
    """Gather Season/Event names from Klarna SEMOP CSV exports."""

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

        for value in clean_series(df[_EVENT_COLUMN]):
            if value.casefold() in _SKIP_EVENT_VALUES:
                continue
            events.append(value)

    return unique_preserve_order(events)


__all__ = [
    "collect_events",
    "clean_series",
    "unique_preserve_order",
]