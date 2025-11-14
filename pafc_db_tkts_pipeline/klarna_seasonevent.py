from __future__ import annotations

"""Utility helpers for exporting Klarna Season/Event MoP PDFs to CSV."""

from pathlib import Path
import re
from typing import Iterable

import camelot
import pandas as pd

from .config import KLARNA_SEMOP_INPUT_DIR, KLARNA_SEMOP_TABLE_OUTPUT_DIR
from .logger import log

UNWANTED_KEYWORDS = {"date:", "time:", "page", "mop analysis", "xrreports"}
RESERVED_TRAILING_HEADERS = ["Total", "VAT", "Total Ex. VAT"]


def _ensure_output_dir() -> None:
    KLARNA_SEMOP_TABLE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Drop empty rows/columns and filter metadata rows."""

    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")

    kept_rows: list[pd.Series] = []
    for _, row in df.iterrows():
        row_text = " | ".join(str(cell) for cell in row)
        if not any(keyword in row_text.lower() for keyword in UNWANTED_KEYWORDS):
            kept_rows.append(row)

    if not kept_rows:
        return pd.DataFrame()

    return pd.DataFrame(kept_rows).reset_index(drop=True)


def set_event_column(df: pd.DataFrame) -> pd.DataFrame | None:
    """Clean header rows and enforce Event column ordering."""

    if df.shape[0] < 2:
        return None

    raw_headers = df.iloc[0].tolist()
    split_headers: list[str] = []
    for header in raw_headers:
        if isinstance(header, str):
            parts = re.split(r"\s{2,}", header.strip())
            if parts:
                split_headers.extend(part for part in parts if part)
            else:
                split_headers.append(header.strip())
        else:
            split_headers.append(str(header).strip())

    cleaned_headers = [
        header
        for header in split_headers
        if header and not header.lower().startswith("unknown")
    ]

    final_headers = ["Event"]
    if "Event" in cleaned_headers:
        cleaned_headers.remove("Event")

    temp_headers = [
        header
        for header in cleaned_headers
        if header not in RESERVED_TRAILING_HEADERS
        and header.lower() not in {"total", "vat", "ex. vat", "total ex. vat"}
    ]

    final_headers.extend(temp_headers)
    final_headers.extend(RESERVED_TRAILING_HEADERS)

    df_data = df.iloc[1:].copy()
    column_count = df_data.shape[1]

    if len(final_headers) > column_count:
        final_headers = final_headers[:column_count]
    elif len(final_headers) < column_count:
        df_data = df_data.iloc[:, : len(final_headers)]

    df_data.columns = pd.Index(final_headers, name=None)
    return df_data


def _process_pdf(pdf_file: Path) -> bool:
    log.info("Processing Klarna Season/Event MoP PDF: %s", pdf_file.name)
    try:
        tables = camelot.read_pdf(
            str(pdf_file),
            pages="all",
            flavor="stream",
            strip_text="\n",
            row_tol=10,
        )

        if not tables or tables.n == 0:
            raise ValueError("No tables found in PDF")

        cleaned_tables: list[pd.DataFrame] = []
        for table in tables:
            df = clean_dataframe(table.df)
            if df.empty:
                continue

            df_event = set_event_column(df)
            if df_event is not None and not df_event.empty:
                cleaned_tables.append(df_event)

        if not cleaned_tables:
            raise ValueError("No usable table data after cleaning")

        combined_df = pd.concat(cleaned_tables, ignore_index=True)
        _ensure_output_dir()
        out_file = KLARNA_SEMOP_TABLE_OUTPUT_DIR / f"{pdf_file.stem}_final.csv"
        combined_df.to_csv(out_file, index=False)
        log.info("Saved Klarna Season/Event MoP CSV: %s", out_file)
        return True

    except Exception as exc:  # noqa: BLE001
        log.error("Failed processing %s – %s", pdf_file.name, exc)
        return False


def export_klarna_seasonevent_tables(
    pdfs: Iterable[Path] | None = None,
) -> tuple[int, int, int]:
    """Process the provided PDFs (or discover them) into CSV exports."""

    if pdfs is None:
        pdf_files = sorted(KLARNA_SEMOP_INPUT_DIR.glob("*.pdf"))
    else:
        pdf_files = list(pdfs)

    if not pdf_files:
        log.warning("No Klarna Season/Event MoP PDFs supplied for export.")
        return 0, 0, 0

    total = len(pdf_files)
    saved = 0
    for pdf_file in pdf_files:
        if _process_pdf(pdf_file):
            saved += 1

    failed = total - saved
    log.info(
        "Klarna Season/Event MoP table export summary – total: %d | success: %d | failed: %d",
        total,
        saved,
        failed,
    )
    return total, saved, failed


def main() -> None:
    """CLI helper so the module can be run standalone."""

    total, saved, failed = export_klarna_seasonevent_tables()
    log.info(
        "Standalone Klarna Season/Event MoP export finished – total: %d, saved: %d, failed: %d",
        total,
        saved,
        failed,
    )


if __name__ == "__main__":
    main()