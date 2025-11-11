from __future__ import annotations

"""
Master runner for the PAFC DB TKTS pipelines.

5-stage view
------------
Stage 1 – checksums.py
    • Discover & validate input folders/files

Stage 2 – date_extractor.py
    • Extract dates from PDFs / Excels

Stage 3 – ticket_office_notes_extractor.py
    • Extract TicketOffice Notes

Stage 4 – saleitemsmop_total_amoun_extractor.py
    • Extract saleitemsmop grand totals

Stage 5 – this orchestrator + aggregate builder
    • Run all mini-pipelines in order
    • Build aggregate_data.csv
    • Log final summary
"""

import sys

from pafc_db_tkts_pipeline.logger import log
from pafc_db_tkts_pipeline.fixed_line_items import (
    run_ticketoffice_pipeline,
    run_saleitemsmop_pipeline,
    run_charges_pipeline,
    run_klarna_pipeline,
    run_klarna_seasoneventmop_pipeline,
    run_membership_pipeline,
)
from pafc_db_tkts_pipeline.output_aggregate_builder import (
    build_aggregate_base_with_saleitemsmop,       # 1–3: date, notes, saleitemsmop_total
    build_membership_other_total_columns,         # 4–5: evergreen other/total
    build_membership_miles_gross_column,          # 6th column
    build_membership_misc_group_gross_column,     # 7th column
    build_membership_waiting_list_gross_column,   # 8th column
    build_membership_total_all_sales_gross_column,# 9th column
    build_klarna_dailytakings_data_columns,       # 10–14: Klarna MoPs
)


def main() -> None:
    """Run all mini-pipelines in order and exit with proper status code."""
    log.info("PAFC DB TKTS master pipeline starting.")

    results: dict[str, int] = {}

    # 1. TicketOffice
    ticketoffice_count = run_ticketoffice_pipeline()
    if ticketoffice_count < 0:
        log.error(
            "Stage 5 – ERROR – pipeline aborted because TicketOffice mini-pipeline FAILED."
        )
        sys.exit(1)
    results["TicketOffice"] = ticketoffice_count

    # 2. saleitemsmop
    sale_count = run_saleitemsmop_pipeline()
    if sale_count < 0:
        log.error(
            "Stage 5 – ERROR – pipeline aborted because saleitemsmop mini-pipeline FAILED."
        )
        sys.exit(1)
    results["saleitemsmop"] = sale_count

    # 3. Charges
    charges_count = run_charges_pipeline()
    if charges_count < 0:
        log.error(
            "Stage 5 – ERROR – pipeline aborted because Charges mini-pipeline FAILED."
        )
        sys.exit(1)
    results["Charges"] = charges_count

    # 4. Klarna DailyTakings
    klarna_count = run_klarna_pipeline()
    if klarna_count < 0:
        log.error(
            "Stage 5 – ERROR – pipeline aborted because Klarna DailyTakings mini-pipeline FAILED."
        )
        sys.exit(1)
    results["Klarna DailyTakings"] = klarna_count

    # 5. Klarna SeasonEvent MoP
    semop_count = run_klarna_seasoneventmop_pipeline()
    if semop_count < 0:
        log.error(
            "Stage 5 – ERROR – pipeline aborted because Klarna SeasonEvent MoP mini-pipeline FAILED."
        )
        sys.exit(1)
    results["Klarna SeasonEvent MoP"] = semop_count

    # 6. Membership
    membership_count = run_membership_pipeline()
    if membership_count < 0:
        log.error(
            "Stage 5 – ERROR – pipeline aborted because Membership mini-pipeline FAILED."
        )
        sys.exit(1)
    results["Membership Daily Detailed Totals"] = membership_count

    # Build aggregate_data.csv
    build_aggregate_base_with_saleitemsmop()
    build_membership_other_total_columns()
    build_membership_miles_gross_column()
    build_membership_misc_group_gross_column()
    build_membership_waiting_list_gross_column()
    build_membership_total_all_sales_gross_column()
    build_klarna_dailytakings_data_columns()

    # Final summary
    log.info(
        "Stage 5 – Completed Pipeline – all mini-pipelines and aggregate_data.csv "
        "finished successfully."
    )
    for name, count in results.items():
        log.info("  • %s pipeline finished with %d record(s).", name, count)


if __name__ == "__main__":
    main()
