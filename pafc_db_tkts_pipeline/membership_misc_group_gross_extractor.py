from __future__ import annotations

from pathlib import Path
import re

import pdfplumber

from .logger import log

# Money like 25.00 or 2,418.00
_MONEY_RE = re.compile(r"\d[\d,]*\.\d{2}")


def extract_mddto_misc_group_gross(pdf_path: Path) -> str:
    """
    Extract the 'Gross Value (Inc Charges)' amount for the Misc Group section.

    Logic
    -----
    * Scan ALL pages in the Membership Daily Detailed Totals PDF.
    * Find the page that contains BOTH:
          - 'Misc Group'
          - 'Gross Value (Inc Charges)'
    * On that page:
          - Find the line containing 'Gross Value (Inc Charges)'.
          - Search that line and the next 3 lines for a money value.
          - Return the FIRST money value found (e.g. '216.00').

    Returns
    -------
    str
        The gross amount as text, or 'Data Unavailable' if it cannot be found.
    """
    pdf_path = Path(pdf_path)

    try:
        with pdfplumber.open(pdf_path) as pdf:
            misc_page_found = False

            for page_no, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""

                if "Misc Group" not in text:
                    continue
                if "Gross Value (Inc Charges)" not in text:
                    continue

                misc_page_found = True

                lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

                gross_idx: int | None = None
                for i, line in enumerate(lines):
                    if "Gross Value (Inc Charges)" in line:
                        gross_idx = i
                        break

                if gross_idx is None:
                    log.error(
                        "Misc Group gross – label 'Gross Value (Inc Charges)' "
                        "not found on Misc page in %s (page %d).",
                        pdf_path.name,
                        page_no,
                    )
                    return "Data Unavailable"

                # Try same line + next 3 lines
                for j in range(gross_idx, min(gross_idx + 4, len(lines))):
                    m = _MONEY_RE.search(lines[j])
                    if m:
                        amount = m.group(0)
                        return amount

                log.error(
                    "Misc Group gross – label found but no money value nearby "
                    "in %s (page %d).",
                    pdf_path.name,
                    page_no,
                )
                return "Data Unavailable"

        if not misc_page_found:
            log.debug(
                "Misc Group gross – no page containing both 'Misc Group' and "
                "'Gross Value (Inc Charges)' in %s.",
                pdf_path.name,
            )
        else:
            log.debug(
                "Misc Group gross – Misc page found but gross value not extracted "
                "in %s.",
                pdf_path.name,
            )
        return "Data Unavailable"

    except Exception as exc:  # noqa: BLE001
        log.error(
            "Misc Group gross – unexpected error while parsing %s: %s",
            pdf_path.name,
            exc,
        )
        return "Data Unavailable"
