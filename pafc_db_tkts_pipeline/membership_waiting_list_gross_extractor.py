from __future__ import annotations

from pathlib import Path
import re

import pdfplumber

from .logger import log


def _normalise_number(raw: str) -> str:
    """
    Normalise a numeric string from the PDF into a standard '1234.56' format.
    Keeps parentheses to indicate negatives only if they are part of the text.
    """
    if raw is None:
        raise ValueError("Empty numeric value for Waiting List gross")

    s = str(raw).strip()

    # Handle negatives in brackets, e.g. "(123.45)"
    negative = s.startswith("(") and s.endswith(")")
    s = s.strip("()").replace(",", "")

    if not s:
        raise ValueError("Blank numeric value for Waiting List gross")

    try:
        value = float(s)
    except ValueError as exc:  # noqa: BLE001
        raise ValueError(f"Cannot parse Waiting List gross value '{raw}'") from exc

    if negative:
        value = -value

    return f"{value:.2f}"


def extract_mddto_waiting_list_gross(pdf_path: Path | str) -> str:
    """
    Extract the 'Waiting List' Gross Value (Inc Charges) from
    Membership Daily Detailed Totals PDF.

    Looks for:

        'Waiting List' section
           ...
           'Gross Value (Inc Charges)   <number>'
    """
    pdf_path = Path(pdf_path)
    target_group = "Waiting List"
    target_label = "Gross Value (Inc Charges)"

    log.info(
        "extract_mddto_waiting_list_gross – scanning %s for '%s' / '%s'",
        pdf_path.name,
        target_group,
        target_label,
    )

    with pdfplumber.open(pdf_path) as pdf:
        for page_idx, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""

            if target_group not in text or target_label not in text:
                continue

            lines = text.splitlines()
            try:
                start_idx = next(
                    i for i, line in enumerate(lines) if target_group in line
                )
            except StopIteration:
                # Fallback to page-wide search below
                start_idx = 0

            lines_down = lines[start_idx:]

            # Debug dump of the section starting from "Waiting List"
            log.info(
                "Waiting List debug – page %d of %s – lines from 'Waiting List' down:",
                page_idx,
                pdf_path.name,
            )
            for i, line in enumerate(lines_down, start=1):
                log.info("  [Wait %03d] %s", i, line)

            # First, try to find the label & number on one of these lines
            pattern = re.compile(r"Gross Value \(Inc Charges\)\s+([(\d][0-9.,()]+)")
            for i, line in enumerate(lines_down, start=1):
                m = pattern.search(line)
                if not m:
                    continue

                raw_val = m.group(1)
                norm = _normalise_number(raw_val)
                log.info(
                    "Waiting List gross – extracted '%s' from line %d on page %d of %s",
                    norm,
                    i,
                    page_idx,
                    pdf_path.name,
                )
                return norm

            # Fallback: search across the whole page text
            m = pattern.search(text)
            if m:
                raw_val = m.group(1)
                norm = _normalise_number(raw_val)
                log.info(
                    "Waiting List gross – extracted '%s' from page %d of %s (fallback)",
                    norm,
                    page_idx,
                    pdf_path.name,
                )
                return norm

    # If we reach here, we didn't find it anywhere
    msg = (
        f"Waiting List gross – could not locate '{target_label}' on any page "
        f"of {pdf_path.name}"
    )
    log.error(msg)
    raise ValueError(msg)
