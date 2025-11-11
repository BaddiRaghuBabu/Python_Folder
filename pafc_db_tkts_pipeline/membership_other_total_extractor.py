from __future__ import annotations

from pathlib import Path
import re

import pdfplumber

from .logger import log  # only used for ERROR-level logging

# Match money values like:
#   78.00
#   1,251.00
#   (75.00)
_MONEY_RE = re.compile(r"\(?\d[\d,]*\.\d{2}\)?")


def _words_to_lines(words, y_tolerance: float = 2.0) -> list[list[dict]]:
    """
    Group pdfplumber words into visual lines using their 'top' coordinate.
    """
    lines: list[list[dict]] = []
    current_y: float | None = None
    current_line: list[dict] = []

    for w in sorted(words, key=lambda w: (w["top"], w["x0"])):
        y = w["top"]
        if current_y is None or abs(y - current_y) > y_tolerance:
            if current_line:
                lines.append(current_line)
            current_y = y
            current_line = [w]
        else:
            current_line.append(w)

    if current_line:
        lines.append(current_line)

    return lines


def extract_membership_other_and_total(
    pdf_path: Path,
) -> tuple[str, str]:
    """
    Extract 'Other' and 'Total' **Value Sold** from page 1 of a
    'Membership Daily Detailed Totals' PDF.

    Behaviour:

    - If the page does NOT contain all of:
          'Evergreen', 'Method of Payment', 'Value Sold'
      we simply return:
          ("Data Not Available In File", "Data Not Available In File")
      and DO NOT log any ERROR.

    - We take only the top **Method of Payment** block:
          from the line containing 'Method of Payment'
          down to (but not including) the line containing 'Membership Type'
          (or end-of-page if that never appears).

    - In that block:
        * For a line with 'Other' → take the **last money value** as Other.
        * For a line with 'Total' → take the **last money value** as Total.

    - If there is **no 'Other' row**, we return:
          other = "Data Not Available In File"
      (Total can still be a real number.)

    - If there is **no 'Total' row**, we return:
          total = "Data Not Available In File"

    Returns
    -------
    (other_value_sold, total_value_sold)
    — both are always strings (never None).
    """
    pdf_path = Path(pdf_path)

    try:
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[0]
            full_text = page.extract_text() or ""

            # 1) Check if this looks like the Evergreen / MoP layout.
            must_have = ("Evergreen", "Method of Payment", "Value Sold")
            missing = [word for word in must_have if word not in full_text]
            if missing:
                # Layout is different – quietly return fallback values.
                return "Data Not Available In File", "Data Not Available In File"

            words = page.extract_words()

        # 2) Build visual lines and turn them into plain text strings
        word_lines = _words_to_lines(words)
        line_texts: list[str] = [
            " ".join(w["text"] for w in line) for line in word_lines
        ]

        # 3) Find the Method-of-Payment block (start index)
        start_idx: int | None = None
        for i, text in enumerate(line_texts):
            if "Method of Payment" in text:
                start_idx = i
                break

        if start_idx is None:
            # No MoP block – treat as no data, but not an error
            return "Data Not Available In File", "Data Not Available In File"

        # 4) Find where the Membership Type section begins (stop index)
        stop_idx = len(line_texts)
        for i in range(start_idx + 1, len(line_texts)):
            if "Membership Type" in line_texts[i]:
                stop_idx = i
                break

        mop_lines = line_texts[start_idx:stop_idx]

        other_val: str | None = None
        total_val: str | None = None

        for text in mop_lines:
            lower = text.lower()

            # Skip headers
            if "method of payment" in lower or "no sold value sold" in lower:
                continue

            tokens = lower.split()
            is_other = "other" in tokens
            is_total = "total" in tokens

            if not (is_other or is_total):
                continue

            # Find all money values on the line; take the last one as Value Sold
            amounts = _MONEY_RE.findall(text)
            if not amounts:
                continue

            value = amounts[-1]

            if is_other and other_val is None:
                other_val = value
            if is_total and total_val is None:
                total_val = value

        # 5) Apply fallbacks so we NEVER raise, and NEVER return None
        if other_val is None:
            other_val = "Data Not Available In File"
        if total_val is None:
            total_val = "Data Not Available In File"

        return other_val, total_val

    except Exception as exc:  # noqa: BLE001
        log.error(
            "Membership Other/Total – unexpected error while parsing %s: %s",
            pdf_path.name,
            exc,
        )
        return "Data Not Available In File", "Data Not Available In File"
