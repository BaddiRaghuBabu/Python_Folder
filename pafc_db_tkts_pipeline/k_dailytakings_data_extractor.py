from __future__ import annotations

import re
from pathlib import Path

import pdfplumber

from .logger import log

# Amounts like 25.00, 2,128.45, -9.00, (9.00)
_AMOUNT_RE = re.compile(r"\(?-?\d{1,3}(?:,\d{3})*\.\d{2}\)?")


def _normalise_amount(raw: str) -> str:
    """
    Turn strings like '2,128.45' or '(9.00)' into '2128.45' / '9.00'.
    Any minus/brackets are removed – we just want the absolute value.
    """
    cleaned = (
        raw.replace("(", "")
        .replace(")", "")
        .replace(",", "")
        .strip()
    )
    # drop leading minus if present
    if cleaned.startswith("-"):
        cleaned = cleaned[1:]

    try:
        value = float(cleaned)
        return f"{value:.2f}"
    except ValueError:
        return cleaned


def _last_amount_on_line(line: str) -> str | None:
    matches = _AMOUNT_RE.findall(line)
    if not matches:
        return None
    return _normalise_amount(matches[-1])


def extract_klarna_dailytakings_mops(pdf_path: Path) -> dict[str, str]:
    """
    From a Klarna Daily Summary Report - Totals PDF, extract the
    *Sale Payments* 'Total' column for the following Methods of Payment:

        Cash, Credit, Debit, Voucher, Account

    Returns a dict with keys:

        k_dailytakings_cash
        k_dailytakings_credit
        k_dailytakings_debit
        k_dailytakings_voucher
        k_dailytakings_account

    Any missing / unparsable value is returned as 'Data Unavailable'.
    """
    target_labels = {
        "Cash": "k_dailytakings_cash",
        "Credit": "k_dailytakings_credit",
        "Debit": "k_dailytakings_debit",
        "Voucher": "k_dailytakings_voucher",
        "Account": "k_dailytakings_account",
    }

    # Default: assume *data* missing → "Data Unavailable"
    result: dict[str, str] = {
        col: "Data Unavailable" for col in target_labels.values()
    }

    with pdfplumber.open(str(pdf_path)) as pdf:
        if not pdf.pages:
            log.error("Klarna MoP extractor – %s has no pages.", pdf_path.name)
            return result

        page = pdf.pages[0]
        text = page.extract_text() or ""

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        log.error("Klarna MoP extractor – empty text for %s.", pdf_path.name)
        return result

    # Restrict to the "Sale Payments" block so the Account line from
    # "Account Payments" is NOT picked up.
    try:
        sale_start_idx = next(
            i for i, ln in enumerate(lines) if ln.startswith("Sale Payments")
        )
    except StopIteration:
        log.error(
            "Klarna MoP extractor – 'Sale Payments' header not found in %s.",
            pdf_path.name,
        )
        return result

    try:
        sale_end_idx = next(
            i
            for i, ln in enumerate(lines[sale_start_idx + 1 :], start=sale_start_idx + 1)
            if ln.startswith("Sale Payments Total")
        )
    except StopIteration:
        sale_end_idx = len(lines)

    sale_block = lines[sale_start_idx:sale_end_idx]

    # Look for each label in this block
    for label, out_col in target_labels.items():
        pattern = re.compile(rf"^{re.escape(label)}\b", flags=re.IGNORECASE)

        for ln in sale_block:
            if not pattern.match(ln):
                continue

            amount = _last_amount_on_line(ln)
            if amount is None:
                # Very defensive – normally all are on the same line
                log.error(
                    "Klarna MoP extractor – could not parse amount on "
                    "line for '%s' in %s: %r",
                    label,
                    pdf_path.name,
                    ln,
                )
                result[out_col] = "Data Unavailable"
            else:
                result[out_col] = amount
            break
        else:
            # label not found in sale block → treat as error
            log.error(
                "Klarna MoP extractor – '%s' row not found in %s; "
                "setting %s to 'Data Unavailable'.",
                label,
                pdf_path.name,
                out_col,
            )
            result[out_col] = "Data Unavailable"

    return result
