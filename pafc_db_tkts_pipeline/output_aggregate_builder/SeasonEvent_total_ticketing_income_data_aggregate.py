from __future__ import annotations

import pandas as pd

from ..config import (
    CHARGES_CSV,
    CHARGES_TOTALS_OUTPUT_DIR,
    TICKETOFFICE_SALE_COMBINED_CSV,
)
from ..logger import log

_TOTALS_WORKBOOK = "charges_totals_all_dates.xlsx"
_TARGET_TOTAL_NAME = "Total INCOME"
_COLUMN_NAME = "charges_total"

def _clean_ticketing_value(value: object) -> str | None:
    if value is None:
        return None

    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned or cleaned.lower() == "nan":
            return None
        return cleaned

    if pd.isna(value):
        return None

    return str(value)


def _write_with_constant(base_df: pd.DataFrame, constant: str) -> None:
    base_df[_COLUMN_NAME] = constant
    base_df.sort_values("date", inplace=True)
    base_df.reset_index(drop=True, inplace=True)
    base_df.to_csv(
        TICKETOFFICE_SALE_COMBINED_CSV,
        index=False,
        encoding="utf-8-sig",
    )


def build_total_ticketing_income_column() -> None:
    """Merge 'Total INCOME' from charges totals into aggregate_data.csv."""

    try:
        base_df = pd.read_csv(TICKETOFFICE_SALE_COMBINED_CSV, dtype=str)
    except FileNotFoundError:
        log.error(
            "Total Ticketing Income aggregate – base file %s not found; "
            "cannot add '%s' column.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            _COLUMN_NAME,
        )
        return

    if "date" not in base_df.columns:
        log.error(
            "Total Ticketing Income aggregate – base file %s is missing 'date' column.",
            TICKETOFFICE_SALE_COMBINED_CSV,
        )
        return

    base_df["date"] = base_df["date"].astype(str).str.zfill(8)

    try:
        charges_dates_series = pd.read_csv(CHARGES_CSV, dtype=str)["date"]
    except FileNotFoundError as exc:
        log.error(
            "Total Ticketing Income aggregate – missing charges summary %s; "
            "setting '%s' column to 'File Unavailable'.",
            exc,
            _COLUMN_NAME,
        )
        _write_with_constant(base_df, "File Unavailable")
        return

    charges_dates_set = set(charges_dates_series.astype(str).str.zfill(8))

    totals_path = CHARGES_TOTALS_OUTPUT_DIR / _TOTALS_WORKBOOK
    if not totals_path.exists():
        log.error(
            "Total Ticketing Income aggregate – totals workbook %s not found; "
            "setting '%s' column to 'File Unavailable'.",
            totals_path,
            _COLUMN_NAME,
        )
        _write_with_constant(base_df, "File Unavailable")
        return

    try:
        totals_df = pd.read_excel(totals_path, dtype={"date": str})
    except Exception as exc:  # noqa: BLE001
        log.error(
            "Total Ticketing Income aggregate – failed to read %s: %s; "
            "setting '%s' column to 'File Unavailable'.",
            totals_path,
            exc,
            _COLUMN_NAME,
        )
        _write_with_constant(base_df, "File Unavailable")
        return

    required_cols = {"date", "total_name"}
    if not required_cols.issubset(totals_df.columns):
        log.error(
            "Total Ticketing Income aggregate – workbook %s missing required "
            "columns %s; setting '%s' column to 'File Unavailable'.",
            totals_path,
            sorted(required_cols),
            _COLUMN_NAME,
        )
        _write_with_constant(base_df, "File Unavailable")
        return

    totals_df["date"] = totals_df["date"].astype(str).str.zfill(8)
    totals_df["total_name"] = totals_df["total_name"].astype(str)

    target_mask = (
        totals_df["total_name"].str.strip().str.casefold()
        == _TARGET_TOTAL_NAME.casefold()
    )
    target_rows = totals_df[target_mask]

    values_by_date: dict[str, str] = {}
    for _, row in target_rows.iterrows():
        date = row["date"]
        cleaned_value = _clean_ticketing_value(row.get("value"))
        if cleaned_value is None:
            values_by_date.setdefault(date, "Data Unavailable")
        else:
            values_by_date[date] = cleaned_value

    column_values: list[str] = []
    for date in base_df["date"]:
        if date in values_by_date:
            column_values.append(values_by_date[date])
        elif date in charges_dates_set:
            column_values.append("Data Unavailable")
        else:
            column_values.append("File Unavailable")

    base_df[_COLUMN_NAME] = column_values
    base_df.sort_values("date", inplace=True)
    base_df.reset_index(drop=True, inplace=True)
    base_df.to_csv(
        TICKETOFFICE_SALE_COMBINED_CSV,
        index=False,
        encoding="utf-8-sig",
    )

    log.info(
        "aggregate_data.csv updated with '%s' column sourced from %s (%d row(s)).",
        _COLUMN_NAME,
        totals_path,
        len(base_df),
    )