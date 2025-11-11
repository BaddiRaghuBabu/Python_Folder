from __future__ import annotations

import pandas as pd

from ..config import MEMBERSHIP_CSV, TICKETOFFICE_SALE_COMBINED_CSV
from ..logger import log


def _clean_gross(v: str | None) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    return s


def build_membership_misc_group_gross_column() -> None:
    """
    Add Membership 'Misc Group' Gross Value (Inc Charges) into aggregate_data.csv.

    Input files
    -----------
    - MEMBERSHIP_CSV
          columns include: date, mddto_misc_group_gross
    - TICKETOFFICE_SALE_COMBINED_CSV
          aggregate_data.csv with at least:
              date, ticketoffice_notes, saleitemsmop_total,
              mddto_evergreen_other, mddto_evergreen_total,
              mddto_miles_gross

    Output
    ------
    - Overwrites TICKETOFFICE_SALE_COMBINED_CSV, adding column:
          mddto_misc_group_gross

      Rules:
        * if membership has a non-empty value for that date:
              mddto_misc_group_gross = that value
        * if membership row exists but value is blank/NaN:
              mddto_misc_group_gross = "Data Unavailable"
        * if NO membership row exists for that date or MEMBERSHIP_CSV
          is missing:
              mddto_misc_group_gross = "File Unavailable"
    """
    try:
        base_df = pd.read_csv(TICKETOFFICE_SALE_COMBINED_CSV, dtype=str)
    except FileNotFoundError as exc:
        log.error(
            "build_membership_misc_group_gross_column skipped – "
            "missing aggregate file: %s",
            exc,
        )
        return

    base_df["date"] = base_df["date"].astype(str).str.zfill(8)

    try:
        membership_df = pd.read_csv(MEMBERSHIP_CSV, dtype=str)
    except FileNotFoundError:
        log.error(
            "build_membership_misc_group_gross_column – %s missing; "
            "setting mddto_misc_group_gross='File Unavailable' for all rows.",
            MEMBERSHIP_CSV,
        )
        base_df["mddto_misc_group_gross"] = "File Unavailable"
    else:
        membership_df["date"] = membership_df["date"].astype(str).str.zfill(8)

        if "mddto_misc_group_gross" not in membership_df.columns:
            log.error(
                "build_membership_misc_group_gross_column – column "
                "'mddto_misc_group_gross' not found in %s; "
                "treating as File Unavailable.",
                MEMBERSHIP_CSV,
            )
            base_df["mddto_misc_group_gross"] = "File Unavailable"
        else:
            membership_df = membership_df[["date", "mddto_misc_group_gross"]]

            gross_by_date: dict[str, str] = {}
            for _, row in membership_df.iterrows():
                d = row["date"]
                raw = row.get("mddto_misc_group_gross")
                val = _clean_gross(raw)
                if val is not None:
                    gross_by_date.setdefault(d, val)
                else:
                    gross_by_date.setdefault(d, "Data Unavailable")

            col: list[str] = []
            for d in base_df["date"]:
                if d in gross_by_date:
                    col.append(gross_by_date[d])
                else:
                    col.append("File Unavailable")

            base_df["mddto_misc_group_gross"] = col

    base_df.sort_values("date", inplace=True)
    base_df.reset_index(drop=True, inplace=True)

    base_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")

    log.info(
        "aggregate_data.csv updated with 'mddto_misc_group_gross' column at %s "
        "(%d row(s)).",
        TICKETOFFICE_SALE_COMBINED_CSV,
        len(base_df),
    )
