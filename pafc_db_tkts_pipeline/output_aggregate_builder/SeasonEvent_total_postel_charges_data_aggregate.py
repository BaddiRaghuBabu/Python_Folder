from __future__ import annotations

"""Add Postal Charges totals into aggregate_data.csv."""

from pathlib import Path

import pandas as pd

from ..config import CHARGES_POSTAL_OUTPUT_DIR, TICKETOFFICE_SALE_COMBINED_CSV
from ..logger import log

_COLUMN_NAME = "charges_postal"
_POSTAL_FILENAME_TEMPLATE = "charges_postel_{date}.xlsx"
_TARGET_ROW_LABEL = "Total Charges Postal"


def _clean_postal_value(value: object) -> str | None:
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


def _load_postal_total(path: Path) -> tuple[str, str | None]:
    """Return tuple(status, value) for the postal total inside ``path``."""

    try:
        df = pd.read_excel(path, dtype=str, engine="openpyxl")
    except FileNotFoundError:
        return "missing_file", None
    except Exception as exc:  # noqa: BLE001
        log.error(
            "Postal charges aggregate – failed to read %s: %s", path.name, exc
        )
        return "missing_file", None

    df.columns = [str(col).strip() for col in df.columns]

    required_cols = {"Charge Type", "Value"}
    if not required_cols.issubset(df.columns):
        log.error(
            "Postal charges aggregate – %s missing required columns %s.",
            path.name,
            sorted(required_cols),
        )
        return "data_unavailable", None

    target_mask = (
        df["Charge Type"].astype(str).str.strip().str.casefold()
        == _TARGET_ROW_LABEL.casefold()
    )
    target_rows = df[target_mask]

    if target_rows.empty:
        log.error(
            "Postal charges aggregate – %s missing '%s' row.",
            path.name,
            _TARGET_ROW_LABEL,
        )
        return "data_unavailable", None

    raw_value = target_rows.iloc[0]["Value"]
    cleaned_value = _clean_postal_value(raw_value)
    if cleaned_value is None:
        log.error(
            "Postal charges aggregate – '%s' row in %s has no usable value.",
            _TARGET_ROW_LABEL,
            path.name,
        )
        return "data_unavailable", None

    return "ok", cleaned_value


def _write_with_constant(base_df: pd.DataFrame, constant: str) -> None:
    base_df[_COLUMN_NAME] = constant
    base_df.sort_values("date", inplace=True)
    base_df.reset_index(drop=True, inplace=True)
    base_df.to_csv(
        TICKETOFFICE_SALE_COMBINED_CSV,
        index=False,
        encoding="utf-8-sig",
    )


def build_total_postal_charges_column() -> None:
    """Populate the 'charges_postal' column in aggregate_data.csv."""
    try:
        base_df = pd.read_csv(TICKETOFFICE_SALE_COMBINED_CSV, dtype=str)
    except FileNotFoundError:
        log.error(
            "Postal charges aggregate – base file %s not found; cannot add '%s' column.",
            TICKETOFFICE_SALE_COMBINED_CSV,
            _COLUMN_NAME,
        )
        return

    if "date" not in base_df.columns:
        log.error(
            "Postal charges aggregate – base file %s missing 'date' column.",
            TICKETOFFICE_SALE_COMBINED_CSV,
        )
        return

    base_df["date"] = base_df["date"].astype(str).str.zfill(8)

    if not CHARGES_POSTAL_OUTPUT_DIR.exists():
        log.error(
            "Postal charges aggregate – directory %s not found; setting '%s' to 'File Unavailable'.",
            CHARGES_POSTAL_OUTPUT_DIR,
            _COLUMN_NAME,
        )
        _write_with_constant(base_df, "File Unavailable")
        return

    column_values: list[str] = []

    for date in base_df["date"]:
        postal_path = CHARGES_POSTAL_OUTPUT_DIR / _POSTAL_FILENAME_TEMPLATE.format(
            date=date
        )
        status, value = _load_postal_total(postal_path)
        if status == "ok" and value is not None:
            column_values.append(value)
        elif status == "data_unavailable":
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
        "aggregate_data.csv updated with '%s' column using Postal Charges data from %s (%d row(s)).",
        _COLUMN_NAME,
        CHARGES_POSTAL_OUTPUT_DIR,
        len(base_df),
    )


__all__ = ["build_total_postal_charges_column"]