from __future__ import annotations

from pathlib import Path
import re

import pdfplumber

from .logger import log

# Money like 25.00, 2,418.00, (15.00) or -15.00
_MONEY_RE = re.compile(r"\(?-?\d[\d,]*\.\d{2}\)?")


def _normalise_amount(raw: str) -> str:
    """Convert a PDF money string to a standard '1234.56' or '-1234.56'."""
    s = raw.strip()

    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1]

    if s.startswith("-"):
        negative = True
        s = s[1:]

    s = s.replace(",", "")

    try:
        value = float(s)
    except ValueError:
        return raw.strip()

    if negative:
        value = -value

    return f"{value:.2f}"


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

                # Find the Gross Value line
                gross_idx: int | None = None
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
                        amount = _normalise_amount(m.group(0))
                        return amount

                log.debug(
                    "Miles gross – label found but no money value nearby in %s (page %d).",
                    pdf_path.name,
                    page_no,
                )
                return "Data Unavailable"

        if not miles_page_found:
            log.debug(
                "Miles gross – no page containing both 'Miles Away Travel Club' and "
                "'Gross Value (Inc Charges)' in %s.",
                pdf_path.name,
            )
        else:
            log.debug(
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
