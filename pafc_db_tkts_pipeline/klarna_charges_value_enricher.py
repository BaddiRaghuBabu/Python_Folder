from __future__ import annotations

"""Enrich Klarna Season/Event MoP tables with charges totals via AI matching."""

from pathlib import Path
import math
import os
import re
from typing import Iterable

import numpy as np
import pandas as pd
from openai import OpenAI

from .config import CHARGES_TOTALS_OUTPUT_DIR, KLARNA_SEMOP_TABLE_OUTPUT_DIR
from .logger import log


def _extract_iso_date_from_name(path: Path) -> str | None:
    match = re.search(r"(\d{8})", path.stem)
    if not match:
        return None
    return match.group(1)


def _load_charges_totals(date_str: str) -> pd.DataFrame | None:
    charges_path = CHARGES_TOTALS_OUTPUT_DIR / f"charges_value_{date_str}.xlsx"
    if not charges_path.exists():
        log.warning(
            "Charges/Klarna enrichment – charges workbook %s not found.",
            charges_path,
        )
        return None

    try:
        df = pd.read_excel(charges_path, dtype={"total_name": str})
    except Exception as exc:  # noqa: BLE001
        log.error(
            "Charges/Klarna enrichment – failed to read %s: %s",
            charges_path,
            exc,
        )
        return None

    required_columns = {"total_name", "value"}
    if not required_columns.issubset(df.columns):
        log.error(
            "Charges/Klarna enrichment – %s missing required column(s) %s.",
            charges_path,
            required_columns - set(df.columns),
        )
        return None

    cleaned = df.copy()
    cleaned["total_name"] = cleaned["total_name"].astype(str).str.strip()
    cleaned["value"] = cleaned["value"].apply(lambda x: None if pd.isna(x) else x)
    cleaned = cleaned[cleaned["total_name"] != ""]
    if cleaned.empty:
        log.warning(
            "Charges/Klarna enrichment – charges workbook %s had no usable rows.",
            charges_path,
        )
        return None

    return cleaned.reset_index(drop=True)


def _build_openai_client() -> OpenAI | None:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        log.error(
            "Charges/Klarna enrichment – OPENAI_API_KEY is not set; cannot run AI matching."
        )
        return None

    return OpenAI(api_key=api_key)


def _cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
    a = np.array(vec_a)
    b = np.array(vec_b)
    denom = np.linalg.norm(a) * np.linalg.norm(b)
    if denom == 0:
        return 0.0
    return float(np.dot(a, b) / denom)


def _embed_texts(client: OpenAI, texts: list[str]) -> list[list[float]]:
    response = client.embeddings.create(model="text-embedding-3-small", input=texts)
    return [item.embedding for item in response.data]


def _match_event_to_total(
    client: OpenAI,
    event: str,
    total_names: list[str],
    total_embeddings: list[list[float]],
) -> tuple[str, float] | None:
    event_embedding = _embed_texts(client, [event])[0]
    best_index = -1
    best_score = -math.inf

    for index, total_embedding in enumerate(total_embeddings):
        score = _cosine_similarity(event_embedding, total_embedding)
        if score > best_score:
            best_score = score
            best_index = index

    if best_index == -1:
        return None

    return total_names[best_index], best_score


def _enrich_csv(
    client: OpenAI,
    csv_file: Path,
    charges_df: pd.DataFrame,
    charges_embeddings: list[list[float]],
) -> bool:
    try:
        df = pd.read_csv(csv_file)
    except Exception as exc:  # noqa: BLE001
        log.error(
            "Charges/Klarna enrichment – failed to read Klarna table %s: %s",
            csv_file.name,
            exc,
        )
        return False

    if "Event" not in df.columns:
        log.warning(
            "Charges/Klarna enrichment – file %s missing Event column; skipping.",
            csv_file.name,
        )
        return False

    total_names = charges_df["total_name"].tolist()
    event_matches: list[float | None] = []

    for event in df["Event"].astype(str):
        event_clean = event.strip()
        if not event_clean:
            event_matches.append(None)
            continue

        match = _match_event_to_total(client, event_clean, total_names, charges_embeddings)
        if match is None:
            event_matches.append(None)
            continue

        matched_total, _score = match
        value = charges_df.loc[charges_df["total_name"] == matched_total, "value"].iloc[0]
        event_matches.append(value)

    df["charges_value"] = event_matches

    try:
        df.to_csv(csv_file, index=False)
    except Exception as exc:  # noqa: BLE001
        log.error(
            "Charges/Klarna enrichment – failed to write updated Klarna table %s: %s",
            csv_file.name,
            exc,
        )
        return False

    log.info(
        "Charges/Klarna enrichment – updated %s with charges_value column.",
        csv_file.name,
    )
    return True


def enrich_klarna_tables_with_charges(pdf_paths: Iterable[Path]) -> bool:
    client = _build_openai_client()
    if client is None:
        return False

    processed_any = False
    total_success = True

    for pdf_path in pdf_paths:
        date_str = _extract_iso_date_from_name(Path(pdf_path))
        if not date_str:
            log.warning(
                "Charges/Klarna enrichment – could not determine date from %s; skipping.",
                Path(pdf_path).name,
            )
            total_success = False
            continue

        charges_df = _load_charges_totals(date_str)
        if charges_df is None:
            total_success = False
            continue

        charges_embeddings = _embed_texts(client, charges_df["total_name"].tolist())

        csv_file = KLARNA_SEMOP_TABLE_OUTPUT_DIR / f"{Path(pdf_path).stem}.csv"
        if not csv_file.exists():
            log.warning(
                "Charges/Klarna enrichment – Klarna table %s not found; skipping.",
                csv_file.name,
            )
            total_success = False
            continue

        processed_any = True
        if not _enrich_csv(client, csv_file, charges_df, charges_embeddings):
            total_success = False

    if not processed_any:
        log.error(
            "Charges/Klarna enrichment – no Klarna Season/Event tables were updated."
        )
        return False

    return total_success


__all__ = ["enrich_klarna_tables_with_charges"]