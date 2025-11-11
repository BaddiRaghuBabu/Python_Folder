from __future__ import annotations

from pathlib import Path
import re

import pdfplumber

from .logger import log

# Money like 25.00 or 2,418.00
_MONEY_RE = re.compile(r"\d[\d,]*\.\d{2}")


def _log_miles_page_block(pdf_name: str, page_no: int, lines: list[str]) -> None:
    """
    Debug helper – log all lines from the 'Miles Away Travel Club' line
    downwards so we can see exactly what text was read from the PDF.
    """
    # Find the first line that contains 'Miles Away Travel Club'
    start_idx = 0
    for i, line in enumerate(lines):
        if "Miles Away Travel Club" in line:
            start_idx = i
            break

    block = lines[start_idx:]

    log.info(
        "Miles debug – page %d of %s – lines from 'Miles Away Travel Club' down:",
        page_no,
        pdf_name,
    )
    for idx, text in enumerate(block, start=1):
        log.info("  %03d | %s", idx, text)


def extract_mddto_miles_gross(pdf_path: Path) -> str:
    """
    Extract the Miles Away Travel Club 'Gross Value (Inc Charges)' amount.

    Logic
    -----
    * Scan ALL pages in the Membership Daily Detailed Totals PDF.
    * Find the page that contains BOTH:
          - 'Miles Away Travel Club'
          - 'Gross Value (Inc Charges)'
    * On that page:
          - Log all lines from 'Miles Away Travel Club' downward
            (for debugging / verification).
          - Find the line containing 'Gross Value (Inc Charges)'
          - Take the FIRST money value on that line (e.g. 25.00).
          - If no money on the same line, look at the next 3 lines.

    Returns
    -------
    str
        The gross amount as text, e.g. '25.00'.

    If anything goes wrong (no Miles page, no Gross label, etc.),
    this function logs an error and returns 'Data Unavailable'.
    """
    pdf_path = Path(pdf_path)

    try:
        with pdfplumber.open(pdf_path) as pdf:
            miles_page_found = False

            for page_no, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""

                # Only interested in pages that have the Miles header
                if "Miles Away Travel Club" not in text:
                    continue

                # We also require the Gross Value label on that page
                if "Gross Value (Inc Charges)" not in text:
                    continue

                miles_page_found = True

                # Split into non-empty lines
                lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

                # LOG the whole Miles block so you can see everything
                _log_miles_page_block(pdf_path.name, page_no, lines)

                # Now find the Gross Value line
                gross_idx = None
                for i, line in enumerate(lines):
                    if "Gross Value (Inc Charges)" in line:
                        gross_idx = i
                        break

                if gross_idx is None:
                    log.error(
                        "Miles gross – 'Gross Value (Inc Charges)' label not found on "
                        "Miles page in %s (page %d).",
                        pdf_path.name,
                        page_no,
                    )
                    return "Data Unavailable"

                # Try same line, then up to 3 lines after
                for j in range(gross_idx, min(gross_idx + 4, len(lines))):
                    m = _MONEY_RE.search(lines[j])
                    if m:
                        amount = m.group(0)
                        log.info(
                            "Miles gross – extracted '%s' from line %d on page %d of %s.",
                            amount,
                            j + 1,
                            page_no,
                            pdf_path.name,
                        )
                        return amount

                log.error(
                    "Miles gross – label found but no money value nearby in %s (page %d).",
                    pdf_path.name,
                    page_no,
                )
                return "Data Unavailable"

        if not miles_page_found:
            log.error(
                "Miles gross – no page containing both 'Miles Away Travel Club' and "
                "'Gross Value (Inc Charges)' in %s.",
                pdf_path.name,
            )
        else:
            log.error(
                "Miles gross – Miles page found but gross value not extracted in %s.",
                pdf_path.name,
            )
        return "Data Unavailable"

    except Exception as exc:  # noqa: BLE001
        log.error(
            "Miles gross – unexpected error while parsing %s: %s",
            pdf_path.name,
            exc,
        )
        return "Data Unavailable"
