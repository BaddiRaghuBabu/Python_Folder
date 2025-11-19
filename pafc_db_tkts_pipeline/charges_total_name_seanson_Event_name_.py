from __future__ import annotations

"""Build a helper report with Klarna Season/Event names and Charges total names."""

from pathlib import Path

import pandas as pd

from .config import CHARGES_EVENT_TOTAL_REPORT_DIR, CHARGES_EVENT_TOTAL_REPORT_XLSX
from .event_ import collect_events
from .logger import log

from .total_name import collect_total_names


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

    events = collect_events()
    totals = collect_total_names()
    return _write_report(events, totals)


__all__ = ["generate_charges_total_name_season_event_report"]