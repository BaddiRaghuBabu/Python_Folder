from __future__ import annotations

"""Derive CCDVA less charges column for Klarna Season/Event exports."""

from pathlib import Path
from typing import Iterable

import pandas as pd

from .config import KLARNA_SEMOP_TABLE_OUTPUT_DIR
from .logger import log


def _to_numeric(series: pd.Series) -> pd.Series:
    """Return numeric series with NaNs coerced to 0 for arithmetic."""

    return pd.to_numeric(series, errors="coerce").fillna(0)


def _apply_ccdva_less_charges(df: pd.DataFrame) -> pd.DataFrame:
    """Add ccdva_less_charges column and populate total row."""

    working_df = df.copy()
    working_df["ccdva_less_charges"] = (
        _to_numeric(working_df["Total_CCDVA"]) - _to_numeric(working_df["charges_value"])
    )

    total_mask = working_df.get("Event", pd.Series(dtype=str)).astype(str).str.casefold() == "total income"
    detail_mask = ~total_mask
    total_value = working_df.loc[detail_mask, "ccdva_less_charges"].sum()

    if total_mask.any():
        working_df.loc[total_mask, "ccdva_less_charges"] = total_value
    else:
        total_row = {column: "" for column in working_df.columns}
        total_row["Event"] = "xero_ccdva_less_charges-->"
        total_row["ccdva_less_charges"] = total_value
        working_df = pd.concat([working_df, pd.DataFrame([total_row])], ignore_index=True)

    return working_df


def add_ccdva_less_charges_column(pdf_paths: Iterable[Path]) -> bool:
    """
    For each Klarna Season/Event MoP PDF path provided, add a CCDVA less charges column
    to the corresponding CSV export.

    Returns True if all CSVs were processed successfully (or nothing to do),
    False if any CSV failed.
    """

    success = True

    for pdf_path in pdf_paths:
        csv_file = KLARNA_SEMOP_TABLE_OUTPUT_DIR / f"{Path(pdf_path).stem}.csv"

        if not csv_file.exists():
            log.warning(
                "CCDVA less charges – no CSV found for %s; skipping.",
                pdf_path,
            )
            continue

        try:
            df = pd.read_csv(csv_file)
        except Exception as exc:  # noqa: BLE001
            log.error(
                "CCDVA less charges – failed to read CSV %s: %s",
                csv_file.name,
                exc,
            )
            success = False
            continue

        if not {"Total_CCDVA", "charges_value"}.issubset(df.columns):
            log.warning(
                "CCDVA less charges – %s missing required columns; skipping.",
                csv_file.name,
            )
            continue

        updated_df = _apply_ccdva_less_charges(df)

        try:
            updated_df.to_csv(csv_file, index=False)
        except Exception as exc:  # noqa: BLE001
            log.error(
                "CCDVA less charges – failed to write CSV %s: %s",
                csv_file.name,
                exc,
            )
            success = False

    return success


__all__ = ["add_ccdva_less_charges_column"]