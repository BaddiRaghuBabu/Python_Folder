from __future__ import annotations

import pandas as pd

from ..config import SALEITEMSMOP_EXCEL, TICKETOFFICE_SALE_COMBINED_CSV
from ..logger import log
from .ticketoffice_date_notes_aggregate import build_date_notes_frame


def _clean_amount(v: str | None) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    return s


def build_aggregate_base_with_saleitemsmop() -> None:
    """
    Build the first version of aggregate_data.csv with three columns:
        - date
        - ticketoffice_notes
        - saleitemsmop_total

    Sources:
      - date + ticketoffice_notes  from TicketOffice & union of all mini-pipeline dates
      - saleitemsmop_total         from saleitemsmop_summary.xlsx

    Per-date rules for saleitemsmop_total:
      - If a saleitemsmop row exists and has a non-empty total_amount:
            saleitemsmop_total = that amount
      - Else if a saleitemsmop row exists but total_amount is empty / NaN:
            saleitemsmop_total = "Data Unavailable"
      - Else (no saleitemsmop row/file for that date):
            saleitemsmop_total = "File Unavailable"
    """
    try:
        # date + ticketoffice_notes
        base_df = build_date_notes_frame()
        sale_df = pd.read_excel(SALEITEMSMOP_EXCEL, dtype=str)
    except FileNotFoundError as exc:
        log.error(
            "aggregate_data base build skipped – missing summary file: %s",
            exc,
        )
        return

    # Normalise saleitemsmop dates
    sale_df["date"] = sale_df["date"].astype(str).str.zfill(8)

    # Set of all dates that have a saleitemsmop row (file present for that date)
    sale_dates_set = set(sale_df["date"])

    # Map: date -> first non-empty total_amount
    amounts_by_date: dict[str, str] = {}
    for _, row in sale_df.iterrows():
        d = str(row["date"]).zfill(8)
        amt = _clean_amount(row.get("total_amount"))
        if amt:
            amounts_by_date.setdefault(d, amt)

    # Attach saleitemsmop_total to base_df with the new rules
    amount_col: list[str] = []
    for d in base_df["date"]:
        if d in amounts_by_date:
            # Have a real numeric amount
            amt = amounts_by_date[d]
            amount_col.append(amt)
        elif d in sale_dates_set:
            # There is a saleitemsmop row/file for this date, but total is empty
            amount_col.append("Data Unavailable")
        else:
            # No saleitemsmop file/row for this date
            amount_col.append("File Unavailable")

    out_df = base_df.copy()
    out_df["saleitemsmop_total"] = amount_col

    # Sort by date
    out_df.sort_values("date", inplace=True)
    out_df.reset_index(drop=True, inplace=True)

    TICKETOFFICE_SALE_COMBINED_CSV.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")

    log.info(
        "Stage 2 – Creating file aggregate_data.csv DONE "
        "(written to outputs/ with %d record(s)).",
        len(out_df),
    )
