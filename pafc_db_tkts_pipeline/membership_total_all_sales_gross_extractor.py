from __future__ import annotations

import re
from pathlib import Path

import pdfplumber

from .logger import log

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_AMOUNT_RE = re.compile(r"\(?-?\d{1,3}(?:,\d{3})*\.\d{2}\)?")


def _normalise_amount(raw: str) -> str:
    """
    Normalise a numeric string such as '(2,659.00)' to '2659.00'.
    """
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

def _find_first_amount(line: str) -> str | None:
    m = _AMOUNT_RE.search(line)
    if not m:
        return None
    return _normalise_amount(m.group(0))


# ---------------------------------------------------------------------------
# Main extractor
# ---------------------------------------------------------------------------


def extract_mddto_total_all_sales_gross(pdf_path: Path) -> str:
    """
    From a Membership DDT PDF, find the 'Total All Sales' section and
    return the 'Gross Value (Inc Charges)' amount as a string.

    For the 2025 layout, the numeric value is on the *line above* the
    'Gross Value (Inc Charges)' label, so we search upwards first.

    On failure, logs an error and returns 'Data Unavailable'.
    """
    header = "Total All Sales"
    label = "Gross Value (Inc Charges)"

    # Lines that must NOT be used for the Gross Value figure even if they
    # contain a number.
    DISALLOWED_LABEL_FRAGMENTS = (
        "VAT",
        "Net Value (Inc Charges)",
        "Booking Charge",
    )

    pdf_path = Path(pdf_path)

    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page_idx, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                if not lines:
                    continue

                page_has_header = any(header in ln for ln in lines)
                page_has_label = any(label in ln for ln in lines)
                if not (page_has_header and page_has_label):
                    continue

                # Work only from the 'Total All Sales' line downward.
                try:
                    start_idx = next(i for i, ln in enumerate(lines) if header in ln)
                except StopIteration:  # very unlikely now
                    start_idx = 0

                sub_lines = lines[start_idx:]

                # Locate the label within this section
                for offset, ln in enumerate(sub_lines):
                    if label not in ln:
                        continue

                    # 1) Same line as the label
                    amount = _find_first_amount(ln)
                    if amount is not None:
                        return amount

                    # 2) Search *upwards* first, nearest amount wins
                    for back in range(1, 6):  # look up to 5 lines above
                        idx = offset - back
                        if idx < 0:
                            break
                        prev_line = sub_lines[idx]
                        if any(frag in prev_line for frag in DISALLOWED_LABEL_FRAGMENTS):
                            continue
                        amount = _find_first_amount(prev_line)
                        if amount is not None:
                            return amount

                    # 3) Fallback – search *downwards* (skipping VAT / Net Value / Booking)
                    for ln2 in sub_lines[offset + 1 : offset + 6]:
                        if any(frag in ln2 for frag in DISALLOWED_LABEL_FRAGMENTS):
                            continue
                        amount = _find_first_amount(ln2)
                        if amount is not None:
                            return amount

        # If we reach here, nothing suitable was found
        log.error(
            "Total All Sales gross – no suitable value found for '%s' in %s.",
            label,
            pdf_path.name,
        )
        return "Data Unavailable"

    except Exception as exc:  # noqa: BLE001
        log.error(
            "Total All Sales gross – unexpected error while parsing %s: %s",
            pdf_path.name,
            exc,
        )
        return "Data Unavailable"
