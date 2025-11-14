from __future__ import annotations

from pathlib import Path

# Root for all TKTS work
TKTS_ROOT = Path(
    r"C:\Users\RaghuBaddi\OneDrive - Valuenode Private Limited\RB VD SHARE\TKTS"
)

INPUT_ROOT = TKTS_ROOT / "Inputs"

# ---- Input folders ---------------------------------------------------------

# saleitemsmop PDFs
INPUT_DIR = INPUT_ROOT / "saleitemsmop_YYYYMMDD"

# Daily Banking – Ticket Office (Excel)
TICKETOFFICE_INPUT_DIR = INPUT_ROOT / "TicketOffice_YYYYMMDD"

# Charges (Excel)
CHARGES_INPUT_DIR = INPUT_ROOT / "charges_YYYYMMDD"

# Klarna Daily Takings PDFs
KLARNA_INPUT_DIR = INPUT_ROOT / "klarna_dailytakings_YYYYMMDD"

# Klarna Season / Events MoP PDFs
KLARNA_SEMOP_INPUT_DIR = INPUT_ROOT / "klarna_seasoneventmop_YYYYMMDD"

# Membership Daily Detailed Totals PDFs
MEMBERSHIP_INPUT_DIR = INPUT_ROOT / "membershipdailydetailedtotalonly_YYYYMMDD"

# ---- Outputs ---------------------------------------------------------------

OUTPUT_DIR = TKTS_ROOT / "outputs"

SALEITEMSMOP_EXCEL = OUTPUT_DIR / "saleitemsmop_summary.xlsx"
TICKETOFFICE_CSV = OUTPUT_DIR / "ticketoffice_dailybanking_notes.csv"
CHARGES_CSV = OUTPUT_DIR / "charges_summary.csv"
CHARGES_POSTAL_OUTPUT_DIR = OUTPUT_DIR / "charges_all_postel"
CHARGES_TOTALS_OUTPUT_DIR = OUTPUT_DIR / "all_values"
KLARNA_CSV = OUTPUT_DIR / "klarna_dailytakings_summary.csv"
KLARNA_SEMOP_CSV = OUTPUT_DIR / "klarna_seasoneventmop_summary.csv"
KLARNA_SEMOP_TABLE_OUTPUT_DIR = OUTPUT_DIR / "season_events"
MEMBERSHIP_CSV = OUTPUT_DIR / "membershipdailydetailedtotals_summary.csv"


# Temp CSV for Miles Away Travel Club gross value (legacy – still used by the
# existing miles aggregate helper)
MEMBERSHIP_MILES_GROSS_CSV = OUTPUT_DIR / "membership_miles_gross_tmp.csv"

# Final combined (aggregate) view
TICKETOFFICE_SALE_COMBINED_CSV = OUTPUT_DIR / "aggregate_data.csv"
