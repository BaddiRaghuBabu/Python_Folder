
from __future__ import annotations

from pathlib import Path
import math
import re
from typing import Iterable

import pandas as pd

from .config import CHARGES_TOTALS_OUTPUT_DIR
from .logger import log


TOTAL_PREFIX = "Total "


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


def extract_totals_from_file(path: Path) -> list[dict[str, object]]:
    log.debug("Charges totals – reading %s", path)
    try:
        df = pd.read_excel(path, header=None)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed to read {path.name}: {exc}") from exc

    rows: list[dict[str, object]] = []
    inside_income = False
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

        if any(text == "INCOME" for text in texts):
            inside_income = True
            continue

        if any(text == "Method of Payment" for text in texts) or any(
            text == "NON INCOME" for text in texts
        ):
            inside_income = False
            continue

        if not inside_income:
            continue

        total_name = next((text for text in texts if text.startswith(TOTAL_PREFIX)), None)
        if not total_name:
            continue

        if total_name in seen_totals:
            continue

        numeric_values: list[float] = []
        for cell in raw_values:
            number = _parse_float(cell)
            if number is not None:
                numeric_values.append(number)

        if not numeric_values:
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
            }
        )

        seen_totals.add(total_name)

    return rows


def _write_per_file_summary(rows: list[dict[str, object]], output_dir: Path) -> None:
    if not rows:
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=["date", "total_name", "value"])
    date_str = rows[0]["date"] or "unknown"
    out_path = output_dir / f"charges_value_{date_str}.xlsx"
    df.to_excel(out_path, index=False)
    log.info("Charges totals – wrote per-file summary %s with %d row(s).", out_path.name, len(df))


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
        combined = pd.DataFrame(all_rows, columns=["date", "total_name", "value"])
        combined.sort_values(["date", "total_name"], inplace=True)
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