from __future__ import annotations

from pathlib import Path
import logging
import re

import pandas as pd


# ---------------------------------------------------------------------------
# Simple logger (ERROR only)
# ---------------------------------------------------------------------------

log = logging.getLogger("charges_postal")
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    )
    log.addHandler(handler)
log.setLevel(logging.ERROR)


POSTAL_LABEL = "Postal Charge"


def _parse_float(value: str, file_name: str, row_idx: int) -> float | None:
    """Normalise '8.50', '1,700.00', '(2.00)' -> float; log only on error."""
    if value is None:
        return None

    s = str(value).strip()
    if not s:
        return None

    negative = s.startswith("(") and s.endswith(")")
    s = s.strip("()").replace(",", "")

    try:
        num = float(s)
    except ValueError:
        log.error(
            "Charges postal – cannot parse Value '%s' in %s (row %s).",
            value,
            file_name,
            row_idx,
        )
        return None

    if negative:
        num = -num
    return num


def _get_income_block(df: pd.DataFrame, file_name: str) -> pd.DataFrame | None:
    """Return rows between first INCOME and first 'Total INCOME' (inclusive)."""
    df = df.fillna("")

    income_mask = df.apply(
        lambda row: row.astype(str)
        .str.contains(r"\bINCOME\b", case=False, regex=True)
        .any(),
        axis=1,
    )
    income_indices = [i for i, v in income_mask.items() if v]
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
    total_indices = [i for i, v in total_income_mask.items() if v and i >= start_idx]
    if not total_indices:
        log.error("Charges postal – 'Total INCOME' not found in %s.", file_name)
        return None
    stop_idx = total_indices[0]

    return df.iloc[start_idx : stop_idx + 1].copy()


def _locate_header(work: pd.DataFrame, file_name: str) -> tuple[int, int, int] | None:
    """Return (header_row_index, charge_col_index, value_col_index)."""
    header_row_idx = None
    header_row = None
    for idx in work.index:
        row = work.loc[idx].astype(str)
        if any(cell.strip() == "Charge Type" for cell in row) and any(
            cell.strip() == "Value" for cell in row
        ):
            header_row_idx = idx
            header_row = row
            break

    if header_row_idx is None:
        log.error(
            "Charges postal – header row with 'Charge Type'/'Value' not found in %s.",
            file_name,
        )
        return None

    charge_col = None
    value_col = None
    for col_idx, cell in header_row.items():
        txt = str(cell).strip()
        if txt == "Charge Type":
            charge_col = col_idx
        elif txt == "Value":  # ignore 'Retained Value'
            value_col = col_idx

    if charge_col is None or value_col is None:
        log.error(
            "Charges postal – could not locate 'Charge Type' or 'Value' columns in %s.",
            file_name,
        )
        return None

    return header_row_idx, charge_col, value_col


def extract_postal_rows(path: Path) -> list[dict[str, str]]:
    """
    Return list of rows like:
        {"charge_type": "...", "value": "8.50", "row_index": "23"}
    for all Postal Charge lines in INCOME block.
    """
    path = Path(path)

    try:
        df = pd.read_excel(
            path,
            header=None,
            dtype=str,
            engine="xlrd",  # needed for .xls
        )
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


def write_postal_detail_excel(path: Path, output_dir: Path) -> None:
    """
    For one charges_YYYYMMDD.xls:
      - collect all Postal Charge rows in INCOME block
      - compute total
      - write charges_postal_detail_YYYYMMDD.xlsx in output_dir
      Columns: date, Charge Type, Value.
      Sheet name: charges_YYYYMMDD
    """
    path = Path(path)
    rows = extract_postal_rows(path)

    # Extract date from filename
    m = re.search(r"(\d{8})", path.name)
    date_str = m.group(1) if m else path.stem

    total = 0.0
    for r in rows:
        num = _parse_float(r["value"], path.name, int(r["row_index"]))
        if num is None:
            continue
        total += num

    # Detail rows with date as first column
    data_rows = [
        {
            "date": date_str,
            "Charge Type": r["charge_type"],
            "Value": r["value"],
        }
        for r in rows
    ]

    # Total row
    data_rows.append(
        {
            "date": date_str,
            "Charge Type": "TOTAL",
            "Value": f"{total:.2f}",
        }
    )

    df_out = pd.DataFrame(data_rows)

    out_path = output_dir / f"charges_postal_detail_{date_str}.xlsx"
    sheet_name = f"charges_{date_str}"

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            df_out.to_excel(writer, index=False, sheet_name=sheet_name)
    except Exception as exc:  # noqa: BLE001
        log.error(
            "Charges postal – failed to write detail Excel %s: %s",
            out_path,
            exc,
        )


def process_all_charges_files(input_dir: Path, output_dir: Path) -> None:
    for xls_path in sorted(input_dir.glob("charges_*.xls")):
        write_postal_detail_excel(xls_path, output_dir)


if __name__ == "__main__":
    INPUT_DIR = Path(
        r"C:\Users\RaghuBaddi\OneDrive - Valuenode Private Limited\RB VD SHARE\TKTS\Inputs\charges_YYYYMMDD"
    )
    OUTPUT_DIR = Path(
        r"C:\Users\RaghuBaddi\OneDrive - Valuenode Private Limited\RB VD SHARE\TKTS\outputs\charges_all_postel"
    )
    process_all_charges_files(INPUT_DIR, OUTPUT_DIR)  