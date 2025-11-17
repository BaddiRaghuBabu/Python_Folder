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
from typing import Callable


from pafc_db_tkts_pipeline.logger import log
from pafc_db_tkts_pipeline.checksums import run_all_stage1_checksums

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
    build_total_ticketing_income_column,          # 15th column
    build_total_postal_charges_column,            # 16th column
    
)

from pafc_db_tkts_pipeline.charges_total_name_seanson_Event_name_ import (
    generate_charges_total_name_season_event_report,
)


def main() -> None:
    """Run all mini-pipelines in order and exit with proper status code."""
    log.info("PAFC DB TKTS master pipeline starting.")
    try:
        stage1_results = run_all_stage1_checksums()
    except Exception:
        sys.exit(1)
    results: list[tuple[str, int]] = []

    stage_runners: list[tuple[str, Callable[[], int]]] = [
(
            "TicketOffice",
            lambda: run_ticketoffice_pipeline(stage1_results.ticketoffice_excels),
        ),
        ("saleitemsmop", lambda: run_saleitemsmop_pipeline(stage1_results.saleitemsmop_pdfs)),
        ("Charges", lambda: run_charges_pipeline(stage1_results.charges_excels)),
        (
            "Membership Daily Detailed Totals",
            lambda: run_membership_pipeline(stage1_results.membership_pdfs),
        ),
        ("Klarna DailyTakings", lambda: run_klarna_pipeline(stage1_results.klarna_pdfs)),
        (
            "Klarna SeasonEvent MoP",
            lambda: run_klarna_seasoneventmop_pipeline(
                stage1_results.klarna_seasoneventmop_pdfs
            ),
        ),
    ]

    for step_index, (name, runner) in enumerate(stage_runners, start=1):
        count = _run_stage(step_index, name, runner)
        results.append((name, count))

    # Build aggregate_data.csv
    build_aggregate_base_with_saleitemsmop()
    build_membership_other_total_columns()
    build_membership_miles_gross_column()
    build_membership_misc_group_gross_column()
    build_membership_waiting_list_gross_column()
    build_membership_total_all_sales_gross_column()
    build_klarna_dailytakings_data_columns()
    build_total_ticketing_income_column()
    build_total_postal_charges_column()
    generate_charges_total_name_season_event_report()

    # Final summary
    log.info(
        "Stage 5 – Completed Pipeline – all mini-pipelines and aggregate_data.csv "
        "finished successfully."
    )

    _log_summary(results)


def _run_stage(step_index: int, name: str, runner: Callable[[], int]) -> int:
    """Execute a mini-pipeline with consistent, expressive logging."""

    prefix = f"Stage 5 – Step {step_index}"
    log.info("%s – %s pipeline starting.", prefix, name)
    count = runner()
    if count < 0:
        log.error(
            "%s – ERROR – pipeline aborted because %s mini-pipeline FAILED.",
            prefix,
            name,
        )
        sys.exit(1)

    log.info("%s – %s pipeline finished with %d record(s).", prefix, name, count)
    return count


def _log_summary(results: list[tuple[str, int]]) -> None:
    """Print a clean, aligned summary of all stage results."""

    name_width = max((len(name) for name, _ in results), default=0)
    log.info("Stage 5 – Summary of processed mini-pipelines:")
    for name, count in results:
        padded_name = name.ljust(name_width)
        log.info("  • %s | %4d record(s)", padded_name, count)



if __name__ == "__main__":
    main()
