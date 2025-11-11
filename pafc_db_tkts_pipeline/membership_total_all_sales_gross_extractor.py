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
    cleaned = raw.replace("(", "").replace(")", "").replace("-", "").replace(",", "")
    cleaned = cleaned.strip()
    try:
        value = float(cleaned)
        return f"{value:.2f}"
    except ValueError:
        return cleaned


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

            log.info(
                "Total All Sales debug – page %d of %s – lines from '%s' down:",
                page_idx,
                pdf_path.name,
                header,
            )
            for i, ln in enumerate(sub_lines, start=1):
                log.info("  [TAS %03d] %s", i, ln)

            # Locate the label within this section
            for offset, ln in enumerate(sub_lines):
                if label not in ln:
                    continue

                # 1) Same line as the label
                amount = _find_first_amount(ln)
                if amount is not None:
                    line_no = start_idx + offset + 1
                    log.info(
                        "Total All Sales gross – extracted '%s' from SAME line %d "
                        "on page %d of %s.",
                        amount,
                        line_no,
                        page_idx,
                        pdf_path.name,
                    )
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
                        line_no = start_idx + idx + 1
                        log.info(
                            "Total All Sales gross – extracted '%s' from line %d "
                            "(%d line(s) ABOVE label) on page %d of %s.",
                            amount,
                            line_no,
                            back,
                            page_idx,
                            pdf_path.name,
                        )
                        return amount

                # 3) Fallback – search *downwards* (skipping VAT / Net Value / Booking)
                for extra_offset, ln2 in enumerate(sub_lines[offset + 1 : offset + 6], start=1):
                    if any(frag in ln2 for frag in DISALLOWED_LABEL_FRAGMENTS):
                        continue
                    amount = _find_first_amount(ln2)
                    if amount is not None:
                        line_no = start_idx + offset + 1 + extra_offset
                        log.info(
                            "Total All Sales gross – extracted '%s' from line %d "
                            "(%d line(s) BELOW label) on page %d of %s.",
                            amount,
                            line_no,
                            extra_offset,
                            page_idx,
                            pdf_path.name,
                        )
                        return amount

    log.error(
        "Total All Sales gross – no suitable value found for '%s' in %s.",
        label,
        pdf_path.name,
    )
    raise ValueError(f"Total All Sales gross not found in {pdf_path.name}")
