from __future__ import annotations

from pathlib import Path
import math
import re
from typing import Iterable

import pandas as pd

from .config import CHARGES_TOTALS_OUTPUT_DIR
from .logger import log


TOTAL_PREFIX = "Total "

# Texts that are just headers / section labels, not "income names"
_HEADER_TOKENS = {
    "INCOME",
    "NON INCOME",
    "Method of Payment",
    "Charge Type",
    "Number of Charges",
    "Value",
    "VAT",
    "Net",
    "Retained Value",
}


def _extract_date_from_filename(filename: str) -> str | None:
    match = re.search(r"(\d{8})", filename)
    if not match:
        return None
    return match.group(1)


def _parse_float(value: object) -> float | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)

    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None

    negative = text.startswith("(") and text.endswith(")")
    cleaned = text.strip("()").replace(",", "")

    try:
        number = float(cleaned)
    except ValueError:
        return None

    if negative:
        number = -number
    return number


def _row_has_income_detail(texts: list[str]) -> bool:
    """
    Return True if this row contains an 'income name' / charge row.

    We look for any cell that:
      - has non-empty text
      - is NOT a header token (INCOME, NON INCOME, Charge Type, Number of Charges, etc.)
      - does NOT start with 'Total '
      - is NOT just a number

    Example rows that return True:
      'Home 2025/26'
      'Season Ticket Signed For (4.00 Fixed per Booking)'

    Example rows that return False:
      'INCOME'
      'Charge Type'
      'Number of Charges'
      'Value'
      'Total Home 2025/26'
      '', ' ', '0', '4.00'
    """
    for text in texts:
        if not text:
            continue
        if text in _HEADER_TOKENS:
            continue
        if text.startswith(TOTAL_PREFIX):
            continue
        # ignore things that are purely numeric like "4.00" or "(4.00)"
        if _parse_float(text) is not None:
            continue
        return True
    return False


def extract_totals_from_file(path: Path) -> list[dict[str, object]]:
    log.debug("Charges totals – reading %s", path)
    try:
        df = pd.read_excel(path, header=None)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed to read {path.name}: {exc}") from exc

    rows: list[dict[str, object]] = []
    inside_income = False
    inside_non_income = False
    section_has_content = False  # True when row like "Season Ticket Signed For ..." seen
    date_str = _extract_date_from_filename(path.name)
    seen_totals: set[str] = set()

    for _, row in df.iterrows():
        raw_values = list(row)
        texts: list[str] = []
        for cell in raw_values:
            if isinstance(cell, float) and math.isnan(cell):
                texts.append("")
            elif cell is None:
                texts.append("")
            else:
                texts.append(str(cell).strip())

        # Section markers
        if any(text == "INCOME" for text in texts):
            inside_income = True
            inside_non_income = False
            section_has_content = False  # new section starts empty
            continue

        if any(text == "NON INCOME" for text in texts):
            inside_income = False
            inside_non_income = True
            section_has_content = False  # new section starts empty
            continue

        if any(text == "Method of Payment" for text in texts):
            inside_income = False
            inside_non_income = False
            continue

        # Ignore rows outside INCOME / NON INCOME
        if not inside_income and not inside_non_income:
            continue

        # --------------------------------------------------
        # Detect whether this INCOME / NON INCOME section
        # has any "income name" row BEFORE a Total ... line.
        # E.g. 'Season Ticket Signed For (4.00 Fixed per Booking)'
        # --------------------------------------------------
        if _row_has_income_detail(texts):
            section_has_content = True

        # Find a "Total ..." label on this row
        total_name = next(
            (text for text in texts if text.startswith(TOTAL_PREFIX)), None
        )
        if not total_name:
            continue

        if total_name in seen_totals:
            continue

        # Collect numeric cells
        numeric_values: list[float] = []
        for cell in raw_values:
            number = _parse_float(cell)
            if number is not None:
                numeric_values.append(number)

        # Special rule for "Total Home ..."
        # Case 1: INCOME + (only headers / blanks) + Total Home  -> 0
        # Case 2: INCOME + at least one income-name row (e.g. Season Ticket Signed For)
        #         + Total Home  -> use actual Value
        if (
            total_name.startswith("Total Home")
            and inside_income
            and not section_has_content
        ):
            value: float | None = 0
        elif not numeric_values:
            value = None
        elif len(numeric_values) >= 2:
            value = numeric_values[1]
        else:
            value = numeric_values[0]

        rows.append(
            {
                "date": date_str,
                "file": path.name,
                "total_name": total_name,
                "value": value,
                "category": "NON INCOME" if inside_non_income else "INCOME",
            }
        )

        seen_totals.add(total_name)

    return rows


def _write_per_file_summary(rows: list[dict[str, object]], output_dir: Path) -> None:
    if not rows:
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=["date", "total_name", "value", "category"])
    date_str = rows[0]["date"] or "unknown"
    out_path = output_dir / f"charges_value_{date_str}.xlsx"
    df.to_excel(out_path, index=False)
    log.info(
        "Charges totals – wrote per-file summary %s with %d row(s).",
        out_path.name,
        len(df),
    )


def write_charges_totals_excels(paths: Iterable[Path]) -> tuple[int, list[str]]:
    all_rows: list[dict[str, object]] = []
    errors: list[str] = []

    for path in sorted(paths):
        try:
            rows = extract_totals_from_file(Path(path))
        except Exception as exc:  # noqa: BLE001
            log.error("Charges totals – %s", exc)
            errors.append(Path(path).name)
            continue

        if not rows:
            log.error("Charges totals – no totals found in %s.", Path(path).name)
            errors.append(Path(path).name)
            continue

        _write_per_file_summary(rows, CHARGES_TOTALS_OUTPUT_DIR)
        all_rows.extend(rows)

    if all_rows:
        CHARGES_TOTALS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        combined = pd.DataFrame(
            all_rows, columns=["date", "total_name", "value", "category"]
        )
        combined.sort_values(["date", "total_name", "category"], inplace=True)
        out_path = CHARGES_TOTALS_OUTPUT_DIR / "charges_totals_all_dates.xlsx"
        combined.to_excel(out_path, index=False)
        log.info(
            "Charges totals – wrote combined workbook %s with %d row(s).",
            out_path.name,
            len(combined),
        )

    if errors:
        log.error(
            "Charges totals – FAILED for %d file(s); see messages above.",
            len(errors),
        )

    return len(all_rows), errors


__all__ = [
    "extract_totals_from_file",
    "write_charges_totals_excels",
]
