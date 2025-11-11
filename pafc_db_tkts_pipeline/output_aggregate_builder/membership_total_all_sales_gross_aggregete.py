from __future__ import annotations

import pandas as pd

from ..config import MEMBERSHIP_CSV, TICKETOFFICE_SALE_COMBINED_CSV
from ..logger import log


def build_membership_total_all_sales_gross_column() -> None:
    """
    Add 'mddto_total_all_sales' to aggregate_data.csv by joining on date
    from membershipdailydetailedtotals_summary.csv.
    """
    if not TICKETOFFICE_SALE_COMBINED_CSV.exists():
        log.error(
            "build_membership_total_all_sales_gross_column – base aggregate file %s not found.",
            TICKETOFFICE_SALE_COMBINED_CSV,
        )
        return

    agg_df = pd.read_csv(TICKETOFFICE_SALE_COMBINED_CSV, dtype=str)

    if not MEMBERSHIP_CSV.exists():
        log.error(
            "build_membership_total_all_sales_gross_column – membership summary %s not found; treating as File Unavailable.",
            MEMBERSHIP_CSV,
        )
        agg_df["mddto_total_all_sales"] = "File Unavailable"
    else:
        mdf = pd.read_csv(MEMBERSHIP_CSV, dtype=str)
        if "mddto_total_all_sales" not in mdf.columns:
            log.error(
                "build_membership_total_all_sales_gross_column – column 'mddto_total_all_sales' not found in %s; treating as File Unavailable.",
                MEMBERSHIP_CSV,
            )
            agg_df["mddto_total_all_sales"] = "File Unavailable"
        else:
            mdf["date"] = mdf["date"].astype(str).str.zfill(8)
            agg_df["date"] = agg_df["date"].astype(str).str.zfill(8)
            mdf = mdf[["date", "mddto_total_all_sales"]].copy()
            agg_df = agg_df.merge(mdf, on="date", how="left")
            agg_df["mddto_total_all_sales"] = agg_df["mddto_total_all_sales"].fillna(
                "Data Unavailable"
            )

    agg_df.to_csv(TICKETOFFICE_SALE_COMBINED_CSV, index=False, encoding="utf-8-sig")
    log.info(
        "aggregate_data.csv updated with 'mddto_total_all_sales' column at %s (%d row(s)).",
        TICKETOFFICE_SALE_COMBINED_CSV,
        len(agg_df),
    )
