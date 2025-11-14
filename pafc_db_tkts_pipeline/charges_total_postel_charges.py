from __future__ import annotations

from pathlib import Path
import re
from typing import Iterable

import pandas as pd

from .config import CHARGES_POSTAL_OUTPUT_DIR
from .logger import log


POSTAL_LABEL = "Postal Charge"


def _parse_float(value: str, file_name: str, row_idx: int) -> float | None:
    """Normalise numbers like '1,700.00' or '(2.00)' into floats."""

    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    negative = text.startswith("(") and text.endswith(")")
    cleaned = text.strip("()").replace(",", "")

    try:
        number = float(cleaned)
    except ValueError:
        log.error(
            "Charges postal – cannot parse Value '%s' in %s (row %s).",
            value,
            file_name,
            row_idx,
        )
        return None

    if negative:
        number = -number
    return number


def _get_income_block(df: pd.DataFrame, file_name: str) -> pd.DataFrame | None:
    """Return the rows between the first INCOME and the first Total INCOME."""

    df = df.fillna("")

    income_mask = df.apply(
        lambda row: row.astype(str)
        .str.contains(r"\bINCOME\b", case=False, regex=True)
        .any(),
        axis=1,
    )
    income_indices = [idx for idx, flag in income_mask.items() if flag]
    if not income_indices:
        log.error("Charges postal – 'INCOME' not found in %s.", file_name)
        return None
    start_idx = income_indices[0]

    total_income_mask = df.apply(
        lambda row: row.astype(str)
        .str.contains("Total INCOME", case=False)
        .any(),
        axis=1,
    )
    total_indices = [idx for idx, flag in total_income_mask.items() if flag and idx >= start_idx]
    if not total_indices:
        log.error("Charges postal – 'Total INCOME' not found in %s.", file_name)
        return None
    stop_idx = total_indices[0]

    return df.iloc[start_idx : stop_idx + 1].copy()


def _locate_header(work: pd.DataFrame, file_name: str) -> tuple[int, int, int] | None:
    """Locate the header row and the Charge Type / Value column indices."""

    header_row_idx: int | None = None
    header_row = None
    for idx in work.index:
        row = work.loc[idx].astype(str)
        if any(cell.strip() == "Charge Type" for cell in row) and any(
            cell.strip() == "Value" for cell in row
        ):
            header_row_idx = idx
            header_row = row
            break

    if header_row_idx is None or header_row is None:
        log.error(
            "Charges postal – header row with 'Charge Type'/'Value' not found in %s.",
            file_name,
        )
        return None

    charge_col: int | None = None
    value_col: int | None = None
    for col_idx, cell in header_row.items():
        text = str(cell).strip()
        if text == "Charge Type":
            charge_col = col_idx
        elif text == "Value":
            value_col = col_idx

    if charge_col is None or value_col is None:
        log.error(
            "Charges postal – could not locate 'Charge Type' or 'Value' columns in %s.",
            file_name,
        )
        return None

    return header_row_idx, charge_col, value_col


def extract_postal_rows(path: Path) -> list[dict[str, str]]:
    """Collect Postal Charge rows from the Charges Excel INCOME block."""

    try:
        df = pd.read_excel(path, header=None, dtype=str, engine="xlrd")
    except Exception as exc:  # noqa: BLE001
        log.error("Charges postal – failed to read %s: %s", path.name, exc)
        return []

    block = _get_income_block(df, path.name)
    if block is None:
        return []

    header_info = _locate_header(block, path.name)
    if header_info is None:
        return []

    header_row_idx, charge_col, value_col = header_info

    results: list[dict[str, str]] = []
    for idx in block.index:
        if idx <= header_row_idx:
            continue

        charge_text = str(block.at[idx, charge_col]).strip()
        if POSTAL_LABEL.lower() not in charge_text.lower():
            continue

        value_text = str(block.at[idx, value_col]).strip()
        if not value_text:
            continue

        results.append(
            {
                "charge_type": charge_text,
                "value": value_text,
                "row_index": str(idx),
            }
        )

    return results


def write_postal_detail_excel(path: Path, output_dir: Path) -> bool:
    """Write one charges_postel<date>.xlsx workbook with detail rows and totals."""

    rows = extract_postal_rows(path)

    match = re.search(r"(\d{8})", path.name)
    date_str = match.group(1) if match else path.stem

    total = 0.0
    for row in rows:
        number = _parse_float(row["value"], path.name, int(row["row_index"]))
        if number is None:
            continue
        total += number

    data_rows = [
        {"date": date_str, "Charge Type": row["charge_type"], "Value": row["value"]}
        for row in rows
    ]

    data_rows.append(
        {"date": date_str, "Charge Type": "Total Charges Postal", "Value": f"{total:.2f}"}
    )

    df_out = pd.DataFrame(data_rows, columns=["date", "Charge Type", "Value"])

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"charges_postel_{date_str}.xlsx"
    sheet_name = f"charges_{date_str}"

    try:
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            df_out.to_excel(writer, index=False, sheet_name=sheet_name)
    except Exception as exc:  # noqa: BLE001
        log.error(
            "Charges postal – failed to write detail Excel %s: %s",
            out_path.name,
            exc,
        )
        return False

    log.info(
        "Charges postal – detail workbook %s created with %d row(s).",
        out_path.name,
        len(df_out),
    )
    return True


def write_charges_postal_detail_excels(paths: Iterable[Path]) -> tuple[int, list[str]]:
    """Create Postal Charge detail workbooks for every Charges Excel provided."""

    successes = 0
    errors: list[str] = []

    for path in paths:
        result = write_postal_detail_excel(Path(path), CHARGES_POSTAL_OUTPUT_DIR)
        if result:
            successes += 1
        else:
            errors.append(Path(path).name)

    if errors:
        log.error(
            "Charges postal – FAILED to create %d workbook(s); see messages above.",
            len(errors),
        )
    else:
        log.info(
            "Charges postal – all %d workbook(s) created in %s.",
            successes,
            CHARGES_POSTAL_OUTPUT_DIR,
        )

    return successes, errors


__all__ = [
    "extract_postal_rows",
    "write_postal_detail_excel",
    "write_charges_postal_detail_excels",
]
