from __future__ import annotations

from pathlib import Path
import re

from .config import (
    INPUT_DIR,
    TICKETOFFICE_INPUT_DIR,
    CHARGES_INPUT_DIR,
    KLARNA_INPUT_DIR,
    KLARNA_SEMOP_INPUT_DIR,
    MEMBERSHIP_INPUT_DIR,
)
from .logger import log

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
# Stage 1 – common folder checks
# ---------------------------------------------------------------------------

def _ensure_folder_has_only(folder: Path, allowed_suffixes: set[str]) -> list[Path]:
    """
    Stage 1 helper – Ensure folder exists, non-empty and contains only allowed
    file types. Logs a generic Stage 1 PASS when successful.
    """
    if not folder.exists():
        msg = f"Stage 1 FAILED – input folder missing: {folder}"
        log.error(msg)
        raise FileNotFoundError(msg)

    files = [p for p in sorted(folder.iterdir()) if p.is_file()]
    if not files:
        msg = f"Stage 1 FAILED – no files found in input folder: {folder}"
        log.error(msg)
        raise FileNotFoundError(msg)

    bad = [p for p in files if p.suffix.lower() not in allowed_suffixes]
    if bad:
        names = ", ".join(p.name for p in bad)
        msg = (
            f"Stage 1 FAILED – non-allowed file(s) present; "
            f"allowed types {allowed_suffixes}: {names}"
        )
        log.error(msg)
        raise ValueError(msg)

    # Generic Stage 1 PASS message
    log.info("Stage 1 – ALL CheckSums PASS to Stage 2")
    return files


# ---- saleitemsmop PDFs -----------------------------------------------------

def stage1_discover_files() -> list[Path]:
    return _ensure_folder_has_only(INPUT_DIR, {".pdf"})


# ---- TicketOffice Excels ---------------------------------------------------

def stage1_discover_ticketoffice_excels() -> list[Path]:
    return _ensure_folder_has_only(TICKETOFFICE_INPUT_DIR, {".xls", ".xlsx"})


# ---- Charges Excels --------------------------------------------------------

def stage1_discover_charges_excels() -> list[Path]:
    return _ensure_folder_has_only(CHARGES_INPUT_DIR, {".xls", ".xlsx"})


# ---- Klarna DailyTakings PDFs ---------------------------------------------

def stage1_discover_klarna_pdfs() -> list[Path]:
    return _ensure_folder_has_only(KLARNA_INPUT_DIR, {".pdf"})


# ---- Klarna SeasonEvent MoP PDFs ------------------------------------------

def stage1_discover_klarna_seasoneventmop_pdfs() -> list[Path]:
    return _ensure_folder_has_only(KLARNA_SEMOP_INPUT_DIR, {".pdf"})


# ---- Membership PDFs -------------------------------------------------------

def stage1_discover_membership_pdfs() -> list[Path]:
    return _ensure_folder_has_only(MEMBERSHIP_INPUT_DIR, {".pdf"})


# ---------------------------------------------------------------------------
# Filename enforcement helpers
# ---------------------------------------------------------------------------

def ensure_saleitemsmop_filename(pdf_path: Path, iso_date: str) -> None:
    stem = pdf_path.stem
    m = SALEITEMSMOP_FILENAME_RE.match(stem)

    if m:
        name_date = m.group(1)
        if name_date != iso_date:
            raise ValueError(f"Filename date {name_date} != saleitemsmop date {iso_date}")
        return

    target = pdf_path.with_name(f"saleitemsmop_{iso_date}{pdf_path.suffix.lower()}")
    if target.exists() and target != pdf_path:
        raise FileExistsError(
            f"Cannot rename {pdf_path.name} -> {target.name} (target exists)"
        )
    if target != pdf_path:
        pdf_path.rename(target)
        log.info("Renamed %s -> %s", pdf_path.name, target.name)


def ensure_ticketoffice_filename(path: Path, iso_date: str) -> None:
    stem = path.stem
    m = TICKETOFFICE_FILENAME_RE.match(stem)

    if m:
        name_date = m.group(1)
        if name_date != iso_date:
            raise ValueError(f"Filename date {name_date} != TicketOffice date {iso_date}")
        return

    target = path.with_name(f"TicketOffice_{iso_date}{path.suffix.lower()}")
    if target.exists() and target != path:
        raise FileExistsError(
            f"Cannot rename {path.name} -> {target.name} (target exists)"
        )
    if target != path:
        path.rename(target)
        log.info("Renamed %s -> %s", path.name, target.name)


def ensure_charges_filename(path: Path, iso_date: str) -> None:
    stem = path.stem
    m = CHARGES_FILENAME_RE.match(stem)

    if m:
        name_date = m.group(1)
        if name_date != iso_date:
            raise ValueError(f"Filename date {name_date} != Charges date {iso_date}")
        return

    target = path.with_name(f"charges_{iso_date}{path.suffix.lower()}")
    if target.exists() and target != path:
        raise FileExistsError(
            f"Cannot rename {path.name} -> {target.name} (target exists)"
        )
    if target != path:
        path.rename(target)
        log.info("Renamed %s -> %s", path.name, target.name)


def ensure_klarna_filename(path: Path, iso_date: str) -> None:
    stem = path.stem
    m = KLARNA_FILENAME_RE.match(stem)

    if m:
        name_date = m.group(1)
        if name_date != iso_date:
            raise ValueError(f"Filename date {name_date} != Klarna date {iso_date}")
        return

    target = path.with_name(f"klarna_dailytakings_{iso_date}{path.suffix.lower()}")
    if target.exists() and target != path:
        raise FileExistsError(
            f"Cannot rename {path.name} -> {target.name} (target exists)"
        )
    if target != path:
        path.rename(target)
        log.info("Renamed %s -> %s", path.name, target.name)


def ensure_klarna_seasoneventmop_filename(path: Path, iso_date: str) -> None:
    stem = path.stem
    m = KLARNA_SEMOP_FILENAME_RE.match(stem)

    if m:
        name_date = m.group(1)
        if name_date != iso_date:
            raise ValueError(
                f"Filename date {name_date} != Klarna SeasonEvent MoP date {iso_date}"
            )
        return

    target = path.with_name(f"klarna_seasoneventmop_{iso_date}{path.suffix.lower()}")
    if target.exists() and target != path:
        raise FileExistsError(
            f"Cannot rename {path.name} -> {target.name} (target exists)"
        )
    if target != path:
        path.rename(target)
        log.info("Renamed %s -> %s", path.name, target.name)


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
        log.info("Renamed %s -> %s", pdf_path.name, target.name)
