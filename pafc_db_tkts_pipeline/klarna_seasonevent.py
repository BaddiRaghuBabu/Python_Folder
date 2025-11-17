from __future__ import annotations

"""Utility helpers for exporting Klarna Season/Event MoP PDFs to CSV."""

from pathlib import Path
import re
from typing import Iterable

import camelot
import pandas as pd
import json
from .config import (
    KLARNA_SEMOP_INPUT_DIR,
    KLARNA_SEMOP_TABLE_OUTPUT_DIR,
    MONTHLY_UNIQUE_EVENTS_CSV,
    MONTHLY_UNIQUE_EVENTS_DIR,
)
from .logger import log
from openai import OpenAI

UNWANTED_KEYWORDS = {"date:", "time:", "page", "mop analysis", "xrreports"}
RESERVED_TRAILING_HEADERS = ["Total", "VAT", "Total Ex. VAT"]
CCDVA_COLUMNS = ["Cash", "Credit", "Debit", "Voucher", "Account"]

_CHARGES_TOTALS_WORKBOOK = "charges_totals_all_dates.xlsx"
_TOTAL_INCOME = "total income"

def _extract_iso_date_from_name(pdf_file: Path) -> str:
    match = re.search(r"(\d{8})", pdf_file.stem)
    if not match:
        raise ValueError(f"Could not determine date from filename: {pdf_file.name}")
    return match.group(1)

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
    df_data.columns = df_data.columns.map(
        lambda col: col.strip() if isinstance(col, str) else col
    )
    return df_data

def _to_numeric(series: pd.Series) -> pd.Series:
    cleaned = (
        series.fillna("0")
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0)


def _prepare_ccdva_totals(df: pd.DataFrame) -> pd.DataFrame:
    working_df = df.copy()
    for column in CCDVA_COLUMNS:
        if column not in working_df.columns:
            working_df[column] = 0
        working_df[column] = _to_numeric(working_df[column])

    working_df["Total_CCDVA"] = working_df[CCDVA_COLUMNS].sum(axis=1)
    return working_df


class _ChargesValueMatcher:
    """Resolve charges totals for Season/Event rows using GPT fuzzy matching."""

    def __init__(self) -> None:
        self._client = OpenAI()
        self._charges_df = self._load_charges_totals()

    def _load_charges_totals(self) -> pd.DataFrame:
        workbook = CHARGES_TOTALS_OUTPUT_DIR / _CHARGES_TOTALS_WORKBOOK
        if not workbook.exists():
            log.warning(
                "Charges lookup – totals workbook %s not found; charges_value will be null.",
                workbook,
            )
            return pd.DataFrame(columns=["date", "total_name", "value"])

        try:
            df = pd.read_excel(workbook)
        except Exception as exc:  # noqa: BLE001
            log.error("Charges lookup – failed to read %s: %s", workbook, exc)
            return pd.DataFrame(columns=["date", "total_name", "value"])

        required = {"date", "total_name", "value"}
        if not required.issubset(df.columns):
            log.error(
                "Charges lookup – workbook %s missing required columns %s; charges_value will be null.",
                workbook,
                sorted(required),
            )
            return pd.DataFrame(columns=["date", "total_name", "value"])

        cleaned = df.copy()
        cleaned["date"] = cleaned["date"].astype(str).str.strip()
        cleaned["total_name"] = (
            cleaned["total_name"].astype(str).str.strip().str.casefold()
        )
        cleaned = cleaned[cleaned["total_name"] != _TOTAL_INCOME]
        return cleaned

    def _prepare_prompt(self, event: str, candidates: list[str]) -> str:
        return json.dumps(
            {
                "event": event,
                "charge_totals": candidates,
                "instruction": (
                    "Select the charge total that best matches the event. Return JSON with "
                    "keys 'match' (true/false) and 'chosen_total_name' (string or null). "
                    "Only choose one when the event and total_name describe the same thing. "
                    "If unsure, set match to false and chosen_total_name to null."
                ),
            }
        )

    def _ask_model(self, event: str, candidates: list[str]) -> str | None:
        prompt = self._prepare_prompt(event, candidates)
        try:
            response = self._client.chat.completions.create(
                model="gpt-5-nano",
                messages=[
                    {
                        "role": "system",
                        "content": "You match football events to charge totals using concise JSON outputs only.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0,
            )
        except Exception as exc:  # noqa: BLE001
            log.error("Charges lookup – OpenAI request failed: %s", exc)
            return None

        content = response.choices[0].message.content
        if not content:
            log.warning("Charges lookup – empty OpenAI response for event '%s'.", event)
            return None

        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            log.error(
                "Charges lookup – unable to parse model response '%s' for event '%s'.",
                content,
                event,
            )
            return None

        if not isinstance(data, dict):
            return None

        if not data.get("match"):
            return None

        chosen = data.get("chosen_total_name")
        if not chosen:
            return None
        return str(chosen).casefold().strip()

    def _select_candidate(self, chosen: str, candidates: pd.DataFrame) -> float | None:
        matched = candidates[candidates["total_name"] == chosen]
        if matched.empty:
            return None
        value = matched.iloc[0]["value"]
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def resolve_value(self, date: str, event: str) -> float | None:
        if not event:
            return None

        candidates = self._charges_df[self._charges_df["date"] == str(date).strip()]
        if candidates.empty:
            return None

        candidate_names = candidates["total_name"].tolist()
        chosen = self._ask_model(event, candidate_names)
        if chosen is None:
            return None

        return self._select_candidate(chosen, candidates)


_charges_matcher: _ChargesValueMatcher | None = None


def _get_charges_matcher() -> _ChargesValueMatcher:
    global _charges_matcher
    if _charges_matcher is None:
        _charges_matcher = _ChargesValueMatcher()
    return _charges_matcher


def _process_pdf(pdf_file: Path) -> bool:
    log.info("Processing Klarna Season/Event MoP PDF: %s", pdf_file.name)
    try:
        iso_date = _extract_iso_date_from_name(pdf_file)
        month = iso_date[:6]
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
        if "Event" in combined_df.columns:
            combined_df = combined_df[
                combined_df["Event"].astype(str).str.strip().str.lower()
                != "total for the period"
            ]

        combined_df.insert(0, "Month", month)
        combined_df.insert(0, "Date", iso_date)
        combined_df = _prepare_ccdva_totals(combined_df)
        matcher = _get_charges_matcher()
        if "Event" in combined_df.columns:
            combined_df["charges_value"] = [
                matcher.resolve_value(iso_date, str(event))
                for event in combined_df["Event"]
            ]
        else:
            combined_df["charges_value"] = None
        _ensure_output_dir()
        out_file = KLARNA_SEMOP_TABLE_OUTPUT_DIR / f"{pdf_file.stem}.csv"
        combined_df.to_csv(out_file, index=False)
        log.info("Saved Klarna Season/Event MoP CSV: %s", out_file)
        return True

    except Exception as exc:  # noqa: BLE001
        log.error("Failed processing %s – %s", pdf_file.name, exc)
        return False

def build_monthly_unique_events_list() -> Path | None:
    """Create a Month/Event listing from existing Season/Event CSV exports."""

    csv_files = sorted(KLARNA_SEMOP_TABLE_OUTPUT_DIR.glob("*.csv"))
    if not csv_files:
        log.warning(
            "No Klarna Season/Event MoP CSV files found in %s.",
            KLARNA_SEMOP_TABLE_OUTPUT_DIR,
        )
        return None

    monthly_events: list[pd.DataFrame] = []
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file, dtype=str)
        except Exception as exc:  # noqa: BLE001
            log.error(
                "Unable to read Season/Event CSV %s – %s", csv_file.name, exc
            )
            continue

        if not {"Month", "Event"}.issubset(df.columns):
            log.warning(
                "Skipping %s because required columns are missing.", csv_file.name
            )
            continue

        cleaned = df[["Month", "Event"]].dropna()
        cleaned = cleaned.assign(
            Month=cleaned["Month"].astype(str).str.strip(),
            Event=cleaned["Event"].astype(str).str.strip(),
        )
        cleaned = cleaned[(cleaned["Month"] != "") & (cleaned["Event"] != "")]
        if not cleaned.empty:
            monthly_events.append(cleaned)

    if not monthly_events:
        log.warning("No Month/Event data available to build unique list.")
        return None

    combined = pd.concat(monthly_events, ignore_index=True)
    combined = combined.drop_duplicates(subset=["Month", "Event"])
    combined = combined.sort_values(["Month", "Event"], ignore_index=True)

    MONTHLY_UNIQUE_EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_csv(MONTHLY_UNIQUE_EVENTS_CSV, index=False)
    log.info("Saved monthly unique events list: %s", MONTHLY_UNIQUE_EVENTS_CSV)
    return MONTHLY_UNIQUE_EVENTS_CSV


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