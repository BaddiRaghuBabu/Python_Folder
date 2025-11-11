from __future__ import annotations

from pathlib import Path
import re

import pdfplumber

from .logger import log


def _normalise_number(raw: str) -> str:
    """
    Normalise a numeric string from the PDF into a standard '1234.56' format.
    On any parse issue, logs an error and returns 'Data Unavailable'.
    """
    if raw is None:
        log.error("Waiting List gross – empty numeric value %r", raw)
        return "Data Unavailable"

    s = str(raw).strip()

    # Handle negatives in brackets, e.g. "(123.45)"
    negative = s.startswith("(") and s.endswith(")")
    s = s.strip("()").replace(",", "")

    if not s:
        log.error("Waiting List gross – blank numeric value after cleaning %r", raw)
        return "Data Unavailable"

    try:
        value = float(s)
    except ValueError as exc:  # noqa: BLE001
        log.error(
            "Waiting List gross – cannot parse numeric value %r: %s",
            raw,
            exc,
        )
        return "Data Unavailable"

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

    On any failure, logs an error and returns 'Data Unavailable'.
    """
    pdf_path = Path(pdf_path)
    target_group = "Waiting List"
    target_label = "Gross Value (Inc Charges)"

    pattern = re.compile(r"Gross Value \(Inc Charges\)\s+([(\d][0-9.,()]+)")

    try:
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
                    # Fallback: search whole page
                    start_idx = 0

                lines_down = lines[start_idx:]

                # First, try to find the label & number in the section from Waiting List down
                for line in lines_down:
                    m = pattern.search(line)
                    if not m:
                        continue

                    raw_val = m.group(1)
                    norm = _normalise_number(raw_val)
                    return norm

                # Fallback: search across the whole page text
                m = pattern.search(text)
                if m:
                    raw_val = m.group(1)
                    norm = _normalise_number(raw_val)
                    return norm

        # If we reach here, we didn't find it anywhere
        log.debug(
            "Waiting List gross – could not locate '%s' on any page of %s",
            target_label,
            pdf_path.name,
        )
        return "Data Unavailable"

    except Exception as exc:  # noqa: BLE001
        log.error(
            "Waiting List gross – unexpected error while parsing %s: %s",
            pdf_path.name,
            exc,
        )
        return "Data Unavailable"
