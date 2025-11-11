from __future__ import annotations

from typing import Any

import math
import pandas as pd

from ..config import MEMBERSHIP_CSV, TICKETOFFICE_SALE_COMBINED_CSV
from ..logger import log

_COL_NAME = "mddto_waiting_list"


def _clean_value(value: Any) -> str | None:
    """Normalise membership Waiting List gross string; return None for blanks."""
    if value is None:
        return None

    if isinstance(value, float) and math.isnan(value):
        return None

    s = str(value).strip()
    if not s:
        return None

    if s.upper() in {"NA", "N/A", "NULL"}:
        return None

    return s


def build_membership_waiting_list_gross_column() -> None:
    """
    Add mddto_waiting_list column to aggregate_data.csv.

    Rules:
      • If there is a membership PDF / row for that date and the value
        exists -> use that value.
      • If there is a membership PDF / row but the value is missing/blank
        -> "Data Unavailable".
      • If there is no membership PDF / row for that date at all
        -> "File Unavailable".
    """
    log.info("build_membership_waiting_list_gross_column – starting.")

    # Load base aggregate (must already exist from previous steps)
    try:
        base_df = pd.read_csv(TICKETOFFICE_SALE_COMBINED_CSV, dtype=str)
    except FileNotFoundError:
        log.error(
            "build_membership_waiting_list_gross_column – base aggregate '%s' "
            "not found. Did you run build_aggregate_base_with_saleitemsmop()?",
            TICKETOFFICE_SALE_COMBINED_CSV,
        )
        return

    if base_df.empty:
        log.warning(
            "build_membership_waiting_list_gross_column – aggregate_data.csv is empty."
        )

    base_df["date"] = base_df["date"].astype(str).str.zfill(8)

    # Load membership summary
    try:
        membership_df = pd.read_csv(MEMBERSHIP_CSV, dtype=str)
    except FileNotFoundError:
        log.error(
            "build_membership_waiting_list_gross_column – membership summary '%s' "
            "not found. All rows will be marked 'File Unavailable'.",
            MEMBERSHIP_CSV,
        )
        base_df[_COL_NAME] = "File Unavailable"
        base_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")
        log.info(
            "aggregate_data.csv updated with '%s' column (all 'File Unavailable').",
            _COL_NAME,
        )
        return

    if membership_df.empty:
        log.warning(
            "build_membership_waiting_list_gross_column – membership summary is empty; "
            "all rows will be marked 'File Unavailable'."
        )
        base_df[_COL_NAME] = "File Unavailable"
        base_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")
        log.info(
            "aggregate_data.csv updated with '%s' column (all 'File Unavailable').",
            _COL_NAME,
        )
        return

    membership_df["date"] = membership_df["date"].astype(str).str.zfill(8)
    membership_dates = set(membership_df["date"])

    if _COL_NAME not in membership_df.columns:
        log.error(
            "build_membership_waiting_list_gross_column – column '%s' not found in %s; "
            "treating as if Waiting List data is unavailable for all membership files.",
            _COL_NAME,
            MEMBERSHIP_CSV,
        )

        values: list[str] = []
        for d in base_df["date"]:
            if d in membership_dates:
                values.append("Data Unavailable")
            else:
                values.append("File Unavailable")

        base_df[_COL_NAME] = values
        base_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")
        log.info(
            "aggregate_data.csv updated with '%s' column using fallback values.",
            _COL_NAME,
        )
        return

    # Build mapping date -> waiting_list value (if present)
    waiting_by_date: dict[str, str] = {}
    for _, row in membership_df.iterrows():
        d = str(row["date"]).zfill(8)
        val = _clean_value(row.get(_COL_NAME))
        # Prefer a non-blank value; ignore blanks
        if val is not None:
            waiting_by_date[d] = val

    out_values: list[str] = []
    for d in base_df["date"]:
        if d in waiting_by_date:
            out_values.append(waiting_by_date[d])
        elif d in membership_dates:
            out_values.append("Data Unavailable")
        else:
            out_values.append("File Unavailable")

    base_df[_COL_NAME] = out_values
    base_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")
    log.info(
        "aggregate_data.csv updated with '%s' column from Membership summary.",
        _COL_NAME,
    )
