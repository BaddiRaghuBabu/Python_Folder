from __future__ import annotations

"""
PAFC DB TKTS pipeline package.

Stages overview
---------------
Stage 1 – checksums.py
    • Discover & validate folders / files

Stage 2 – date_extractor.py
    • Extract dates from PDFs / Excels

Stage 3 – ticket_office_notes_extractor.py
    • Extract TicketOffice Notes

Stage 4 – saleitemsmop_total_amoun_extractor.py
    • Extract saleitemsmop grand total

Stage 5 – pafc_db_tkts_pipeline_orchestrator.py
    • Run all mini-pipelines + build aggregate_data.csv
"""
from .fixed_line_items import (
    run_ticketoffice_pipeline,
    run_saleitemsmop_pipeline,
    run_charges_pipeline,
    run_klarna_pipeline,
    run_klarna_seasoneventmop_pipeline,
    run_membership_pipeline,
)
