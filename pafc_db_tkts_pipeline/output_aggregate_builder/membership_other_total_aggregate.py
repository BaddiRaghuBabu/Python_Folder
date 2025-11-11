from __future__ import annotations

import pandas as pd

from ..config import MEMBERSHIP_CSV, TICKETOFFICE_SALE_COMBINED_CSV
from ..logger import log


def _clean_value(v: str | None) -> str | None:
    """
    Treat empty / NaN as missing.
    Keep any real text such as 'Data Unavailable' unchanged.
    """
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    return s


def build_membership_other_total_columns() -> None:
    """
    Add Membership 'Other' and 'Total' Value Sold into aggregate_data.csv.

    Input files:
      - MEMBERSHIP_CSV
            columns: date, other, total
      - TICKETOFFICE_SALE_COMBINED_CSV
            existing aggregate_data.csv with at least:
                date, ticketoffice_notes, saleitemsmop_total

    Output:
      - Overwrites TICKETOFFICE_SALE_COMBINED_CSV with columns including:
            date,
            ticketoffice_notes,
            saleitemsmop_total,
            mddto_evergreen_other,
            mddto_evergreen_total

        Per-date rules:
          * If a membership row exists for that date:
                - if 'other'/'total' has a real value (including 'Data Unavailable'):
                      mddto_evergreen_other / mddto_evergreen_total = that value
                - if 'other'/'total' is truly empty / NaN:
                      mddto_evergreen_other / mddto_evergreen_total = "Data Unavailable"
          * If no membership row exists for that date at all:
                mddto_evergreen_other = "File Unavailable"
                mddto_evergreen_total = "File Unavailable"
    """
    try:
        base_df = pd.read_csv(TICKETOFFICE_SALE_COMBINED_CSV, dtype=str)
        membership_df = pd.read_csv(MEMBERSHIP_CSV, dtype=str)
    except FileNotFoundError as exc:
        log.error(
            "build_membership_other_total_columns skipped – missing file: %s",
            exc,
        )
        return

    # Normalise dates
    base_df["date"] = base_df["date"].astype(str).str.zfill(8)
    membership_df["date"] = membership_df["date"].astype(str).str.zfill(8)

    # Track which dates have *any* membership row
    membership_dates_set = set(membership_df["date"])

    # Build per-date values for other / total (first non-empty value)
    other_by_date: dict[str, str] = {}
    total_by_date: dict[str, str] = {}

    for _, row in membership_df.iterrows():
        d = str(row["date"]).zfill(8)

        other_val = _clean_value(row.get("other"))
        if d not in other_by_date and other_val is not None:
            other_by_date[d] = other_val

        total_val = _clean_value(row.get("total"))
        if d not in total_by_date and total_val is not None:
            total_by_date[d] = total_val

    # Build output columns following the rules
    out_other: list[str] = []
    out_total: list[str] = []

    for d in base_df["date"]:
        if d in membership_dates_set:
            # There is at least one membership row for this date
            if d in other_by_date:
                o_val = other_by_date[d]
            else:
                o_val = "Data Unavailable"

            if d in total_by_date:
                t_val = total_by_date[d]
            else:
                t_val = "Data Unavailable"
        else:
            # No membership file/row for this date
            o_val = "File Unavailable"
            t_val = "File Unavailable"

        out_other.append(o_val)
        out_total.append(t_val)

    merged = base_df.copy()
    merged["mddto_evergreen_other"] = out_other
    merged["mddto_evergreen_total"] = out_total

    # Keep date sorted
    merged.sort_values("date", inplace=True)
    merged.reset_index(drop=True, inplace=True)

    merged.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")

    log.info(
        "aggregate_data.csv updated with Membership "
        "'mddto_evergreen_other' and 'mddto_evergreen_total' "
        "columns at %s (%d row(s)).",
        TICKETOFFICE_SALE_COMBINED_CSV,
        len(merged),
    )
