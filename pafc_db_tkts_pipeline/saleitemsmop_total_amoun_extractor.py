from __future__ import annotations

from pathlib import Path
import re

import pdfplumber


def extract_saleitemsmop_total_amount(pdf_path: Path) -> str:
    """
    Read bottom grand total from saleitemsmop PDF.
    Looks for the last line that looks like '123.45'.
    Returns total_amount as string.
    """
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text() or ""

    total_str: str | None = None
    for line in reversed(text.splitlines()):
        s = line.strip()
        if re.fullmatch(r"\d+\.\d{2}", s):
            total_str = s
            break

    if total_str is None:
        raise ValueError("Could not find saleitemsmop grand total amount in PDF")

    return total_str
