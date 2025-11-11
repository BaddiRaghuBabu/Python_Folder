from __future__ import annotations

import pandas as pd

from ..config import (
    TICKETOFFICE_CSV,
    SALEITEMSMOP_EXCEL,
    CHARGES_CSV,
    KLARNA_CSV,
    KLARNA_SEMOP_CSV,
    MEMBERSHIP_CSV,
)
from ..logger import log


def _norm_dates(s: pd.Series) -> pd.Series:
    return s.astype(str).str.zfill(8)


def _clean_note(v: str | None) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    return s


def build_date_notes_frame() -> pd.DataFrame:
    """
    Build a base DataFrame with UNIQUE dates from all mini-pipelines
    and TicketOffice notes.

    Notes column rules:

      - If TicketOffice has a non-empty note for that date:
            ticketoffice_notes = that note
      - Else if TicketOffice has a row for that date but note is empty:
            ticketoffice_notes = "Null"
      - Else (no TicketOffice file/row for that date at all):
            ticketoffice_notes = "File Unavailable"

    Returns a DataFrame with columns:
        - date
        - ticketoffice_notes
    """
    try:
        # TicketOffice has date + notes
        ticket_df = pd.read_csv(TICKETOFFICE_CSV, dtype=str)

        # Other summaries – only need dates
        sale_dates = pd.read_excel(SALEITEMSMOP_EXCEL, dtype=str)["date"]
        membership_dates = pd.read_csv(MEMBERSHIP_CSV, dtype=str)["date"]
        charges_dates = pd.read_csv(CHARGES_CSV, dtype=str)["date"]
        klarna_dates = pd.read_csv(KLARNA_CSV, dtype=str)["date"]
        semop_dates = pd.read_csv(KLARNA_SEMOP_CSV, dtype=str)["date"]
    except FileNotFoundError as exc:
        log.error(
            "build_date_notes_frame skipped – missing summary file: %s",
            exc,
        )
        raise

    # Normalise TicketOffice dates
    ticket_df["date"] = _norm_dates(ticket_df["date"])
    ticket_dates_set = set(ticket_df["date"])

    # Map: date -> first non-empty note
    notes_by_date: dict[str, str] = {}
    for _, row in ticket_df.iterrows():
        d = str(row["date"]).zfill(8)
        note = _clean_note(row.get("notes"))
        if note:
            notes_by_date.setdefault(d, note)

    # Stack all dates in the required order
    ticket_dates = ticket_df["date"]
    all_dates = pd.concat(
        [
            ticket_dates,
            _norm_dates(sale_dates),
            _norm_dates(membership_dates),
            _norm_dates(charges_dates),
            _norm_dates(klarna_dates),
            _norm_dates(semop_dates),
        ],
        ignore_index=True,
    )

    # Remove duplicates, keep first occurrence
    all_dates = all_dates.drop_duplicates().reset_index(drop=True)

    # Build ticketoffice_notes column aligned with all_dates
    notes_col: list[str] = []
    for d in all_dates:
        if d in notes_by_date:
            note = notes_by_date[d]          # real note from TicketOffice
        elif d in ticket_dates_set:
            note = "Null"                    # TicketOffice row but empty note
        else:
            note = "File Unavailable"          # no TicketOffice file/row
        notes_col.append(note)

    df = pd.DataFrame({"date": all_dates, "ticketoffice_notes": notes_col})

    log.info("Built base date+ticketoffice_notes frame with %d row(s).", len(df))
    return df
