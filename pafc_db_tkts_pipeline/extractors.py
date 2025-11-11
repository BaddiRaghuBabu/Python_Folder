from __future__ import annotations

"""
Stage 1 content helpers + Stage 2/3 mini-pipelines.

This module is responsible for:

  • Opening a single PDF/Excel file.
  • Parsing dates, totals, and notes from the header.
  • Converting dates to YYYYMMDD strings.
  • Enforcing filename patterns.

AND

  • Running each mini-pipeline end-to-end:
        - call Stage 1 folder checks,
        - build Stage 2 rows,
        - call Stage 3 writers,
        - return record counts,
        - build the combined TicketOffice + saleitemsmop CSV.
"""

from datetime import datetime
from pathlib import Path
import re

import pdfplumber
import pandas as pd

from .logger import log
from .checksums import (
    stage1_discover_files,                   # saleitemsmop PDFs
    stage1_discover_ticketoffice_excels,
    stage1_discover_charges_excels,
    stage1_discover_klarna_pdfs,
    stage1_discover_klarna_seasoneventmop_pdfs,
    stage1_discover_membership_pdfs,
)
from .output import (
    write_summary,                     # saleitemsmop Excel
    write_ticketoffice_csv,
    write_charges_csv,
    write_klarna_csv,
    write_klarna_seasoneventmop_csv,
    write_membership_csv,
)
from .config import (
    TICKETOFFICE_CSV,
    SALEITEMSMOP_EXCEL,
    TICKETOFFICE_SALE_COMBINED_CSV,
)

# ---------------------------------------------------------------------------
# Filename regex patterns
# ---------------------------------------------------------------------------

SALEITEMSMOP_FILENAME_RE = re.compile(r"^saleitemsmop_(\d{8})$", re.IGNORECASE)
TICKETOFFICE_FILENAME_RE = re.compile(r"^TicketOffice_(\d{8})$", re.IGNORECASE)
CHARGES_FILENAME_RE = re.compile(r"^charges_(\d{8})$", re.IGNORECASE)
KLARNA_FILENAME_RE = re.compile(r"^klarna_dailytakings_(\d{8})$", re.IGNORECASE)
KLARNA_SEMOP_FILENAME_RE = re.compile(
    r"^klarna_seasoneventmop_(\d{8})$", re.IGNORECASE
)
MEMBERSHIP_FILENAME_RE = re.compile(
    r"^membershipdailydetailedtotalonly_(\d{8})$", re.IGNORECASE
)

# ---------------------------------------------------------------------------
# saleitemsmop PDF helpers (per-file)
# ---------------------------------------------------------------------------


def extract_date_and_total(pdf_path: Path) -> tuple[str, str]:
    """
    Read 'From DD/MM/YYYY' and bottom grand total (e.g. 0.00).
    Returns (YYYYMMDD, total_amount_string).
    """
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text() or ""

    # Date
    m = re.search(r"From\s+(\d{2}/\d{2}/\d{4})", text)
    if not m:
        raise ValueError("Could not find 'From DD/MM/YYYY' date in saleitemsmop PDF")

    date_str = m.group(1)
    dt = datetime.strptime(date_str, "%d/%m/%Y")
    iso_date = dt.strftime("%Y%m%d")

    # Grand total – last line that looks like 123.45
    total_str: str | None = None
    for line in reversed(text.splitlines()):
        s = line.strip()
        if re.fullmatch(r"\d+\.\d{2}", s):
            total_str = s
            break

    if total_str is None:
        raise ValueError("Could not find saleitemsmop grand total amount in PDF")

    return iso_date, total_str


def ensure_filename_matches(pdf_path: Path, iso_date: str) -> None:
    stem = pdf_path.stem
    m = SALEITEMSMOP_FILENAME_RE.match(stem)

    if m:
        name_date = m.group(1)
        if name_date != iso_date:
            raise ValueError(f"Filename date {name_date} != PDF date {iso_date}")
        return

    target = pdf_path.with_name(
        f"saleitemsmop_{iso_date}{pdf_path.suffix.lower()}"
    )
    if target.exists() and target != pdf_path:
        raise FileExistsError(
            f"Cannot rename {pdf_path.name} -> {target.name} (target exists)"
        )
    if target != pdf_path:
        pdf_path.rename(target)
        log.info(f"Renamed {pdf_path.name} -> {target.name}")


# ---------------------------------------------------------------------------
# TicketOffice Excel helpers (per-file)
# ---------------------------------------------------------------------------


def _read_excel_no_header(path: Path) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=0, header=None)


def extract_ticketoffice_date_and_notes(path: Path) -> tuple[str, str]:
    """
    Extract:
      - Date from the "Date:" header row.
      - Free-text Notes on or below the "Notes:" label.
    Returns (YYYYMMDD, notes_or_Null).
    """
    df = _read_excel_no_header(path)

    # --- Date ---
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

    iso_date = dt.strftime("%Y%m%d")

    # --- Notes ---
    notes_pos = list(zip(*((df == "Notes:").values.nonzero())))
    notes_text = ""
    if notes_pos:
        nr, nc = notes_pos[0]
        collected: list[str] = []

        # Look on the same row as 'Notes:' and any rows below it,
        # in columns from nc onwards, skipping the label cell itself.
        for row in range(nr, df.shape[0]):
            row_vals: list[str] = []
            for col in range(nc, df.shape[1]):
                if row == nr and col == nc:
                    continue  # skip the "Notes:" label cell itself
                v = df.iat[row, col]
                if pd.isna(v):
                    continue
                s = str(v).strip()
                if s:
                    row_vals.append(s)
            if row_vals:
                collected.append(" ".join(row_vals))

        notes_text = " ".join(collected).strip()

    if not notes_text:
        notes_text = "Null"

    return iso_date, notes_text


def ensure_ticketoffice_filename(path: Path, iso_date: str) -> None:
    stem = path.stem
    m = TICKETOFFICE_FILENAME_RE.match(stem)

    if m:
        name_date = m.group(1)
        if name_date != iso_date:
            raise ValueError(f"Filename date {name_date} != sheet date {iso_date}")
        return

    target = path.with_name(f"TicketOffice_{iso_date}{path.suffix.lower()}")
    if target.exists() and target != path:
        raise FileExistsError(
            f"Cannot rename {path.name} -> {target.name} (target exists)"
        )
    if target != path:
        path.rename(target)
        log.info(f"Renamed {path.name} -> {target.name}")


# ---------------------------------------------------------------------------
# Charges Excel helpers (per-file)
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


def ensure_charges_filename(path: Path, iso_date: str) -> None:
    stem = path.stem
    m = CHARGES_FILENAME_RE.match(stem)

    if m:
        name_date = m.group(1)
        if name_date != iso_date:
            raise ValueError(f"Filename date {name_date} != sheet date {iso_date}")
        return

    target = path.with_name(f"charges_{iso_date}{path.suffix.lower()}")
    if target.exists() and target != path:
        raise FileExistsError(
            f"Cannot rename {path.name} -> {target.name} (target exists)"
        )
    if target != path:
        path.rename(target)
        log.info(f"Renamed {path.name} -> {target.name}")


# ---------------------------------------------------------------------------
# Klarna DailyTakings PDF helpers (per-file)
# ---------------------------------------------------------------------------


def extract_klarna_date(path: Path) -> str:
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


def ensure_klarna_filename(path: Path, iso_date: str) -> None:
    stem = path.stem
    m = KLARNA_FILENAME_RE.match(stem)

    if m:
        name_date = m.group(1)
        if name_date != iso_date:
            raise ValueError(f"Filename date {name_date} != Klarna header {iso_date}")
        return

    target = path.with_name(f"klarna_dailytakings_{iso_date}{path.suffix.lower()}")
    if target.exists() and target != path:
        raise FileExistsError(
            f"Cannot rename {path.name} -> {target.name} (target exists)"
        )
    if target != path:
        path.rename(target)
        log.info(f"Renamed {path.name} -> {target.name}")


# ---------------------------------------------------------------------------
# Klarna Season/Event MoP PDF helpers (per-file)
# ---------------------------------------------------------------------------


def extract_klarna_seasoneventmop_date(path: Path) -> str:
    """
    MOP Analysis for Season/Events for Period 04/05/2025 00:00:00 to 04/05/2025 23:59:59
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


def ensure_klarna_seasoneventmop_filename(path: Path, iso_date: str) -> None:
    stem = path.stem
    m = KLARNA_SEMOP_FILENAME_RE.match(stem)

    if m:
        name_date = m.group(1)
        if name_date != iso_date:
            raise ValueError(
                f"Filename date {name_date} != Season/Event MoP header {iso_date}"
            )
        return

    target = path.with_name(
        f"klarna_seasoneventmop_{iso_date}{path.suffix.lower()}"
    )
    if target.exists() and target != path:
        raise FileExistsError(
            f"Cannot rename {path.name} -> {target.name} (target exists)"
        )
    if target != path:
        path.rename(target)
        log.info(f"Renamed {path.name} -> {target.name}")


# ---------------------------------------------------------------------------
# Membership Daily Detailed Totals PDF helpers (per-file)
# ---------------------------------------------------------------------------


def extract_membership_date(pdf_path: Path) -> str:
    """
    Transaction Date/Time Range : 05/02/2025 00:00 to 05/02/2025 23:59
    From and To dates MUST be the same day.
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


def ensure_membership_filename(pdf_path: Path, iso_date: str) -> None:
    stem = pdf_path.stem
    m = MEMBERSHIP_FILENAME_RE.match(stem)

    if m:
        name_date = m.group(1)
        if name_date != iso_date:
            raise ValueError(
                f"Filename date {name_date} != Membership header date {iso_date}"
            )
        return

    target = pdf_path.with_name(
        f"membershipdailydetailedtotalonly_{iso_date}{pdf_path.suffix.lower()}"
    )
    if target.exists() and target != pdf_path:
        raise FileExistsError(
            f"Cannot rename {pdf_path.name} -> {target.name} (target exists)"
        )
    if target != pdf_path:
        pdf_path.rename(target)
        log.info(f"Renamed {pdf_path.name} -> {target.name}")


# ===========================================================================
# MINI-PIPELINES (Stage 2 + Stage 3)
# ===========================================================================

# saleitemsmop ---------------------------------------------------------------


def _stage2_build_saleitemsmop_rows(pdfs: list[Path]):
    rows: list[dict[str, str]] = []
    errors: list[str] = []

    for pdf_path in pdfs:
        try:
            iso_date, total = extract_date_and_total(pdf_path)
            ensure_filename_matches(pdf_path, iso_date)
            rows.append({"date": iso_date, "total_amount": total})
        except Exception as exc:  # noqa: BLE001
            msg = f"{pdf_path.name}: {exc}"
            errors.append(msg)
            log.error(f"Stage 2 ERROR – {msg}")

    if errors:
        log.error(
            "Stage 2 FAILED – saleitemsmop PDF(s) had errors; see messages above."
        )
    else:
        log.info("Stage 2 PASS – WIP rows built for all saleitemsmop PDFs.")

    return rows, errors


def run_saleitemsmop_pipeline() -> int:
    log.info("saleitemsmop pipeline starting..")
    try:
        pdfs = stage1_discover_files()
    except Exception:  # noqa: BLE001
        return -1

    rows, errors = _stage2_build_saleitemsmop_rows(pdfs)
    if errors or not rows:
        log.error("saleitemsmop pipeline aborted – Stage 2 FAILED; Excel not created.")
        return -1

    write_summary(rows)
    log.info(f"saleitemsmop pipeline finished with {len(rows)} record(s).")
    return len(rows)


# TicketOffice ---------------------------------------------------------------


def _stage2_build_ticketoffice_rows(files: list[Path]):
    rows: list[dict[str, str]] = []
    errors: list[str] = []

    for path in files:
        try:
            iso_date, notes = extract_ticketoffice_date_and_notes(path)
            ensure_ticketoffice_filename(path, iso_date)
            rows.append({"date": iso_date, "notes": notes})
        except Exception as exc:  # noqa: BLE001
            msg = f"{path.name}: {exc}"
            errors.append(msg)
            log.error(f"TicketOffice Stage 2 ERROR – {msg}")

    if errors:
        log.error(
            "TicketOffice Stage 2 FAILED – some Excel file(s) had errors; "
            "see messages above."
        )
    else:
        log.info(
            "TicketOffice Stage 2 PASS – WIP rows built for all TicketOffice Excels."
        )

    return rows, errors


def run_ticketoffice_pipeline() -> int:
    log.info("TicketOffice Daily Banking pipeline starting..")
    try:
        excels = stage1_discover_ticketoffice_excels()
    except Exception:  # noqa: BLE001
        return -1

    rows, errors = _stage2_build_ticketoffice_rows(excels)
    if errors or not rows:
        log.error(
            "TicketOffice pipeline aborted – Stage 2 FAILED; CSV not created."
        )
        return -1

    write_ticketoffice_csv(rows)
    log.info(f"TicketOffice pipeline finished with {len(rows)} record(s).")
    return len(rows)


# Charges --------------------------------------------------------------------


def _stage2_build_charges_rows(files: list[Path]):
    rows: list[dict[str, str]] = []
    errors: list[str] = []

    for path in files:
        try:
            iso_date = extract_charges_date(path)
            ensure_charges_filename(path, iso_date)
            rows.append({"date": iso_date})
        except Exception as exc:  # noqa: BLE001
            msg = f"{path.name}: {exc}"
            errors.append(msg)
            log.error(f"Charges Stage 2 ERROR – {msg}")

    if errors:
        log.error(
            "Charges Stage 2 FAILED – Charges Excel file(s) had errors; "
            "see messages above."
        )
    else:
        log.info("Charges Stage 2 PASS – header dates verified and WIP rows built.")

    return rows, errors


def run_charges_pipeline() -> int:
    log.info("Charges Daily Banking pipeline starting..")
    try:
        excels = stage1_discover_charges_excels()
    except Exception:  # noqa: BLE001
        return -1

    rows, errors = _stage2_build_charges_rows(excels)
    if errors or not rows:
        log.error("Charges pipeline aborted – Stage 2 FAILED; CSV not created.")
        return -1

    write_charges_csv(rows)
    log.info(f"Charges pipeline finished with {len(rows)} record(s).")
    return len(rows)


# Klarna DailyTakings --------------------------------------------------------


def _stage2_build_klarna_rows(pdfs: list[Path]):
    rows: list[dict[str, str]] = []
    errors: list[str] = []

    for path in pdfs:
        try:
            iso_date = extract_klarna_date(path)
            ensure_klarna_filename(path, iso_date)
            rows.append({"date": iso_date})
        except Exception as exc:  # noqa: BLE001
            msg = f"{path.name}: {exc}"
            errors.append(msg)
            log.error(f"Klarna Stage 2 ERROR – {msg}")

    if errors:
        log.error(
            "Klarna Stage 2 FAILED – DailyTakings PDF(s) had errors; "
            "see messages above."
        )
    else:
        log.info(
            "Klarna Stage 2 PASS – DailyTakings header dates verified and "
            "WIP rows built."
        )

    return rows, errors


def run_klarna_pipeline() -> int:
    log.info("Klarna DailyTakings pipeline starting..")
    try:
        pdfs = stage1_discover_klarna_pdfs()
    except Exception:  # noqa: BLE001
        return -1

    rows, errors = _stage2_build_klarna_rows(pdfs)
    if errors or not rows:
        log.error(
            "Klarna pipeline aborted – Stage 2 FAILED; CSV not created."
        )
        return -1

    write_klarna_csv(rows)
    log.info(f"Klarna DailyTakings pipeline finished with {len(rows)} record(s).")
    return len(rows)


# Klarna Season/Event MoP ----------------------------------------------------


def _stage2_build_klarna_seasoneventmop_rows(pdfs: list[Path]):
    rows: list[dict[str, str]] = []
    errors: list[str] = []

    for path in pdfs:
        try:
            iso_date = extract_klarna_seasoneventmop_date(path)
            ensure_klarna_seasoneventmop_filename(path, iso_date)
            rows.append({"date": iso_date})
        except Exception as exc:  # noqa: BLE001
            msg = f"{path.name}: {exc}"
            errors.append(msg)
            log.error(f"Klarna SeasonEvent MoP Stage 2 ERROR – {msg}")

    if errors:
        log.error(
            "Klarna SeasonEvent MoP Stage 2 FAILED – PDF(s) had errors; "
            "see messages above."
        )
    else:
        log.info(
            "Klarna SeasonEvent MoP Stage 2 PASS – header dates verified and "
            "WIP rows built."
        )

    return rows, errors


def run_klarna_seasoneventmop_pipeline() -> int:
    log.info("Klarna SeasonEvent MoP pipeline starting..")
    try:
        pdfs = stage1_discover_klarna_seasoneventmop_pdfs()
    except Exception:  # noqa: BLE001
        return -1

    rows, errors = _stage2_build_klarna_seasoneventmop_rows(pdfs)
    if errors or not rows:
        log.error(
            "Klarna SeasonEvent MoP pipeline aborted – Stage 2 FAILED; CSV not created."
        )
        return -1

    write_klarna_seasoneventmop_csv(rows)
    log.info(
        "Klarna SeasonEvent MoP pipeline finished with "
        f"{len(rows)} record(s)."
    )
    return len(rows)


# Membership -----------------------------------------------------------------


def _stage2_build_membership_rows(pdfs: list[Path]):
    rows: list[dict[str, str]] = []
    errors: list[str] = []

    for path in pdfs:
        try:
            iso_date = extract_membership_date(path)
            ensure_membership_filename(path, iso_date)
            rows.append({"date": iso_date})
        except Exception as exc:  # noqa: BLE001
            msg = f"{path.name}: {exc}"
            errors.append(msg)
            log.error(f"Membership Stage 2 ERROR – {msg}")

    if errors:
        log.error(
            "Membership Stage 2 FAILED – Membership PDF(s) had errors; "
            "see messages above."
        )
    else:
        log.info(
            "Membership Stage 2 PASS – header dates & filenames verified and "
            "WIP rows built."
        )

    return rows, errors


def run_membership_pipeline() -> int:
    log.info("Membership Daily Detailed Totals pipeline starting..")
    try:
        pdfs = stage1_discover_membership_pdfs()
    except Exception:  # noqa: BLE001
        return -1

    rows, errors = _stage2_build_membership_rows(pdfs)
    if errors or not rows:
        log.error(
            "Membership pipeline aborted – Stage 2 FAILED; CSV not created."
        )
        return -1

    write_membership_csv(rows)
    log.info(
        f"Membership Daily Detailed Totals pipeline finished with "
        f"{len(rows)} record(s)."
    )
    return len(rows)


# Combined TicketOffice + saleitemsmop --------------------------------------


def build_ticketoffice_sale_combined() -> None:
    """
    Simple append of TicketOffice and saleitemsmop rows.

    Output CSV: TICKETOFFICE_SALE_COMBINED_CSV with columns:
      - date
      - notes
      - total_amount
    """
    try:
        to_df = pd.read_csv(TICKETOFFICE_CSV, dtype=str)
        sm_df = pd.read_excel(SALEITEMSMOP_EXCEL, dtype=str)
    except FileNotFoundError as exc:
        log.error(
            "Combined TicketOffice+saleitemsmop view skipped – missing file: %s",
            exc,
        )
        return

    # normalise dates
    to_df["date"] = to_df["date"].astype(str).str.zfill(8)
    sm_df["date"] = sm_df["date"].astype(str).str.zfill(8)

    def clean_note(v: str | None) -> str:
        if v is None or str(v).strip() == "":
            return "Null"
        return str(v)

    def clean_amount(v: str | None) -> str:
        if v is None or str(v).strip() == "":
            return "Not found file"
        return str(v)

    rows: list[dict[str, str]] = []

    # Block 1: TicketOffice rows
    for _, r in to_df.iterrows():
        d = str(r["date"]).zfill(8)
        note = clean_note(r.get("notes"))
        rows.append(
            {
                "date": d,
                "notes": note,
                "total_amount": "Not found file",
            }
        )

    # Block 2: saleitemsmop rows
    for _, r in sm_df.iterrows():
        d = str(r["date"]).zfill(8)
        amount = clean_amount(r.get("total_amount"))
        rows.append(
            {
                "date": d,
                "notes": "Not found file",
                "total_amount": amount,
            }
        )

    combined = pd.DataFrame(rows, columns=["date", "notes", "total_amount"])

    TICKETOFFICE_SALE_COMBINED_CSV.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(
        TICKETOFFICE_SALE_COMBINED_CSV,
        index=False,
        encoding="utf-8-sig",
    )

    log.info(
        "Combined TicketOffice + saleitemsmop CSV created at %s with %d row(s) "
        "(TicketOffice %d + saleitemsmop %d).",
        TICKETOFFICE_SALE_COMBINED_CSV,
        len(combined),
        len(to_df),
        len(sm_df),
    )
