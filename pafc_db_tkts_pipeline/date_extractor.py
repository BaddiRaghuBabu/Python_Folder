from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re

import pdfplumber
import pandas as pd


# ---------------------------------------------------------------------------
# saleitemsmop – extract date from PDF
# ---------------------------------------------------------------------------

def extract_saleitemsmop_date(pdf_path: Path) -> str:
    """
    Read 'From DD/MM/YYYY' from the saleitemsmop PDF.
    Returns YYYYMMDD.
    """
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text() or ""

    m = re.search(r"From\s+(\d{2}/\d{2}/\d{4})", text)
    if not m:
        raise ValueError("Could not find 'From DD/MM/YYYY' date in saleitemsmop PDF")

    date_str = m.group(1)
    dt = datetime.strptime(date_str, "%d/%m/%Y")
    return dt.strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# Helper – read Excel with no header
# ---------------------------------------------------------------------------

def _read_excel_no_header(path: Path) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=0, header=None)


# ---------------------------------------------------------------------------
# TicketOffice – extract date from Excel
# ---------------------------------------------------------------------------

def extract_ticketoffice_date(path: Path) -> str:
    """
    Extract date from TicketOffice Daily Banking sheet.

    Looks for "Date:" label then reads value to the right.
    Returns YYYYMMDD.
    """
    df = _read_excel_no_header(path)

    # Locate "Date:"
    date_pos = list(zip(*((df == "Date:").values.nonzero())))
    if not date_pos:
        raise ValueError("Could not find 'Date:' label in TicketOffice sheet")

    r, c = date_pos[0]
    date_value = None
    for col in range(c + 1, df.shape[1]):
        v = df.iat[r, col]
        if pd.isna(v) or str(v).strip() == "":
            continue
        date_value = v
        break

    if date_value is None:
        raise ValueError("Could not find date value to the right of 'Date:'")

    if isinstance(date_value, (datetime, pd.Timestamp)):
        dt = pd.to_datetime(date_value).to_pydatetime()
    else:
        text = str(date_value).strip()
        dt = None
        for fmt in ("%d-%b-%y", "%d-%b-%Y", "%d/%m/%Y", "%d/%m/%y"):
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                pass
        if dt is None:
            raise ValueError(f"Unrecognised TicketOffice date format: {text!r}")

    return dt.strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# Charges – extract date from Excel
# ---------------------------------------------------------------------------

def extract_charges_date(path: Path) -> str:
    """
    Parse header like:
      'Charge Transactions for Wed 13 Aug 2025 00:00 - Wed 13 Aug 2025 23:59'
    Only use the header date range and require From == To.
    """
    df = pd.read_excel(path, sheet_name=0, header=None, nrows=8)
    text = " ".join(str(v) for v in df.to_numpy().ravel() if not pd.isna(v))

    m = re.search(
        r"Charge Transactions for\s+\w+\s+(\d{1,2}\s+\w{3}\s+\d{4})\s+00:00\s*-\s*"
        r"\w+\s+(\d{1,2}\s+\w{3}\s+\d{4})\s+23:59",
        text,
        flags=re.IGNORECASE,
    )
    if not m:
        raise ValueError(
            f"Could not parse Charges header date range from text: {text!r}"
        )

    from_str, to_str = m.groups()
    dt_from = datetime.strptime(from_str, "%d %b %Y")
    dt_to = datetime.strptime(to_str, "%d %b %Y")

    if dt_from.date() != dt_to.date():
        raise ValueError(f"Charges header From {from_str} != To {to_str}")

    return dt_from.strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# Klarna DailyTakings – extract date from PDF
# ---------------------------------------------------------------------------

def extract_klarna_dailytakings_date(path: Path) -> str:
    """
    Daily Summary Report - Totals ...
    Header contains:
        Between Fri 11 Apr 2025, 12:00AM and Fri 11 Apr 2025, 11:59PM
    """
    with pdfplumber.open(path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text() or ""

    one_line = " ".join(text.splitlines())
    m = re.search(
        r"Between\s+\w+\s+(\d{1,2}\s+\w{3}\s+\d{4}),\s+\d{1,2}:\d{2}[AP]M\s+and\s+"
        r"\w+\s+(\d{1,2}\s+\w{3}\s+\d{4}),\s+\d{1,2}:\d{2}[AP]M",
        one_line,
        flags=re.IGNORECASE,
    )
    if not m:
        raise ValueError(
            f"Could not parse Klarna header date range from text: {one_line!r}"
        )

    from_str, to_str = m.groups()
    dt_from = datetime.strptime(from_str, "%d %b %Y")
    dt_to = datetime.strptime(to_str, "%d %b %Y")

    if dt_from.date() != dt_to.date():
        raise ValueError(f"Klarna header From {from_str} != To {to_str}")

    return dt_from.strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# Klarna Season/Event MoP – extract date from PDF
# ---------------------------------------------------------------------------

def extract_klarna_seasoneventmop_date(path: Path) -> str:
    """
    MOP Analysis for Season/Events for Period 04/05/2025 00:00:00 to 04/05/2025 23:59:59
    Returns YYYYMMDD.
    """
    with pdfplumber.open(path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text() or ""

    one_line = " ".join(text.splitlines())
    m = re.search(
        r"for Period\s+(\d{2}/\d{2}/\d{4})\s+[\d:]+\s+to\s+(\d{2}/\d{2}/\d{4})\s+[\d:]+",
        one_line,
        flags=re.IGNORECASE,
    )
    if not m:
        raise ValueError(
            f"Could not parse Klarna Season/Event MoP date range from: {one_line!r}"
        )

    from_str, to_str = m.groups()
    dt_from = datetime.strptime(from_str, "%d/%m/%Y")
    dt_to = datetime.strptime(to_str, "%d/%m/%Y")

    if dt_from.date() != dt_to.date():
        raise ValueError(
            f"Klarna Season/Event MoP From {from_str} != To {to_str}"
        )

    return dt_from.strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# Membership Daily Detailed Totals – extract date from PDF
# ---------------------------------------------------------------------------

def extract_membership_date(pdf_path: Path) -> str:
    """
    Transaction Date/Time Range : 05/02/2025 00:00 to 05/02/2025 23:59
    From and To dates MUST be the same day.
    Returns YYYYMMDD.
    """
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text() or ""

    one_line = " ".join(text.splitlines())
    m = re.search(
        r"Transaction Date/Time Range\s*:\s*"
        r"(\d{2}/\d{2}/\d{4})\s+\d{2}:\d{2}\s+to\s+"
        r"(\d{2}/\d{2}/\d{4})\s+\d{2}:\d{2}",
        one_line,
        flags=re.IGNORECASE,
    )
    if not m:
        raise ValueError(
            f"Could not parse Membership Date/Time Range from text: {one_line!r}"
        )

    from_str, to_str = m.groups()
    dt_from = datetime.strptime(from_str, "%d/%m/%Y")
    dt_to = datetime.strptime(to_str, "%d/%m/%Y")

    if dt_from.date() != dt_to.date():
        raise ValueError(
            f"Membership header From date {from_str} != To date {to_str}"
        )

    return dt_from.strftime("%Y%m%d")
