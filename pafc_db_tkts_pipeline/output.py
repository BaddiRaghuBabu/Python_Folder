from __future__ import annotations

import pandas as pd

from .config import (
    OUTPUT_DIR,
    SALEITEMSMOP_EXCEL,
    TICKETOFFICE_CSV,
    CHARGES_CSV,
    KLARNA_CSV,
    KLARNA_SEMOP_CSV,
    MEMBERSHIP_CSV,
)
from .logger import log


def _ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# saleitemsmop – Excel summary (date, total_amount)
# ---------------------------------------------------------------------------


def write_summary(rows: list[dict[str, str]]) -> None:
    _ensure_output_dir()
    df = pd.DataFrame(rows, columns=["date", "total_amount"])
    df.sort_values("date", inplace=True)
    df.to_excel(SALEITEMSMOP_EXCEL, index=False)
    log.info(
        "Stage 2 – Creating file saleitemsmop_summary.xlsx DONE "
        "(written to outputs/ with %d record(s)).",
        len(df),
    )


# ---------------------------------------------------------------------------
# TicketOffice – CSV (date, notes)
# ---------------------------------------------------------------------------


def write_ticketoffice_csv(rows: list[dict[str, str]]) -> None:
    _ensure_output_dir()
    df = pd.DataFrame(rows, columns=["date", "notes"])
    df.sort_values("date", inplace=True)
    df.to_csv(TICKETOFFICE_CSV, index=False, encoding="utf-8-sig")
    log.info(
        "Stage 2 – Creating file ticketoffice_dailybanking_notes.csv "
        "DONE (written to outputs/ with %d record(s)).",
        len(df),
    )


# ---------------------------------------------------------------------------
# Charges – CSV (date)
# ---------------------------------------------------------------------------


def write_charges_csv(rows: list[dict[str, str]]) -> None:
    _ensure_output_dir()
    df = pd.DataFrame(rows, columns=["date"])
    df.sort_values("date", inplace=True)
    df.to_csv(CHARGES_CSV, index=False, encoding="utf-8-sig")
    log.info(
        "Stage 2 – Creating file charges_summary.csv "
        "DONE (written to outputs/ with %d record(s)).",
        len(df),
    )


# ---------------------------------------------------------------------------
# Klarna DailyTakings – CSV (date + MoP totals)
# ---------------------------------------------------------------------------


def write_klarna_csv(rows: list[dict[str, str]]) -> None:
    """
    Write Klarna DailyTakings summary.

    Each row dict may contain:
        date,
        k_dailytakings_cash,
        k_dailytakings_credit,
        k_dailytakings_debit,
        k_dailytakings_voucher,
        k_dailytakings_account
    """
    if not rows:
        log.warning(
            "write_klarna_csv called with no rows – Klarna summary not written."
        )
        return

    _ensure_output_dir()

    df = pd.DataFrame(rows)

    if "date" in df.columns:
        df["date"] = df["date"].astype(str).str.zfill(8)
        df.sort_values("date", inplace=True)

    df.to_csv(KLARNA_CSV, index=False, encoding="utf-8-sig")
    log.info(
        "Stage 2 – Creating file klarna_dailytakings_summary.csv "
        "DONE (written to outputs/ with %d record(s)).",
        len(df),
    )


# ---------------------------------------------------------------------------
# Klarna Season / Events MoP – CSV (date)
# ---------------------------------------------------------------------------


def write_klarna_seasoneventmop_csv(rows: list[dict[str, str]]) -> None:
    _ensure_output_dir()
    df = pd.DataFrame(rows, columns=["date"])
    df.sort_values("date", inplace=True)
    df.to_csv(KLARNA_SEMOP_CSV, index=False, encoding="utf-8-sig")
    log.info(
        "Stage 2 – Creating file klarna_seasoneventmop_summary.csv "
        "DONE (written to outputs/ with %d record(s)).",
        len(df),
    )


# ---------------------------------------------------------------------------
# Membership – CSV (flexible columns)
# ---------------------------------------------------------------------------


def write_membership_csv(rows: list[dict[str, str]]) -> None:
    """
    Write Membership Daily Detailed Totals summary.

    We keep all keys present in the row dicts so that new columns
    (Miles, Misc Group, Waiting List, Total All Sales, etc.) flow
    straight through.
    """
    if not rows:
        log.warning(
            "write_membership_csv called with no rows – membership summary not written."
        )
        return

    _ensure_output_dir()

    df = pd.DataFrame(rows)

    if "date" in df.columns:
        df["date"] = df["date"].astype(str).str.zfill(8)
        df.sort_values("date", inplace=True)

    df.to_csv(MEMBERSHIP_CSV, index=False, encoding="utf-8-sig")
    log.info(
        "Membership summary CSV written to %s with %d row(s).",
        MEMBERSHIP_CSV,
        len(df),
    )
