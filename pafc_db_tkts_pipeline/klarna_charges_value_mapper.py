from __future__ import annotations

"""Append charges values to Klarna Season/Event outputs using OpenAI fuzzy matching."""

from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd
from openai import OpenAI

from .config import CHARGES_TOTALS_OUTPUT_DIR, KLARNA_SEMOP_TABLE_OUTPUT_DIR
from .logger import log

EMBEDDING_MODEL = "text-embedding-3-small"
DEFAULT_SCORE_THRESHOLD = 0.8


def _read_klarna_file(date_str: str) -> tuple[pd.DataFrame, Path] | tuple[None, None]:
    klarna_path = KLARNA_SEMOP_TABLE_OUTPUT_DIR / f"klarna_seasoneventmop_{date_str}.csv"
    if not klarna_path.exists():
        log.error("Klarna Season/Event CSV not found: %s", klarna_path)
        return None, None

    try:
        df = pd.read_csv(klarna_path)
    except Exception as exc:  # noqa: BLE001
        log.error("Unable to read Klarna Season/Event CSV %s – %s", klarna_path.name, exc)
        return None, None

    if "Event" not in df.columns:
        log.error("Klarna Season/Event CSV %s missing 'Event' column.", klarna_path.name)
        return None, None

    return df, klarna_path


def _read_charges_values(date_str: str) -> pd.DataFrame | None:
    charges_path = CHARGES_TOTALS_OUTPUT_DIR / f"charges_value_{date_str}.xlsx"
    if not charges_path.exists():
        log.error("Charges value workbook not found: %s", charges_path)
        return None

    try:
        df = pd.read_excel(charges_path)
    except Exception as exc:  # noqa: BLE001
        log.error("Unable to read charges workbook %s – %s", charges_path.name, exc)
        return None

    missing = {"total_name", "value"} - set(df.columns)
    if missing:
        log.error(
            "Charges workbook %s missing required columns: %s",
            charges_path.name,
            ", ".join(sorted(missing)),
        )
        return None

    return df


def _clean_strings(values: Iterable[object]) -> list[str]:
    cleaned: list[str] = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            cleaned.append(text)
    return cleaned


def _embed_texts(texts: Sequence[str], client: OpenAI) -> list[list[float]]:
    if not texts:
        return []

    response = client.embeddings.create(model=EMBEDDING_MODEL, input=list(texts))
    # The API preserves order, so we can rely on index alignment.
    return [item.embedding for item in response.data]


def _best_match(
    vector: np.ndarray, candidate_vectors: np.ndarray, candidate_labels: Sequence[str]
) -> tuple[str | None, float | None]:
    if candidate_vectors.size == 0:
        return None, None

    denom = np.linalg.norm(candidate_vectors, axis=1) * np.linalg.norm(vector)
    with np.errstate(divide="ignore", invalid="ignore"):
        similarities = np.dot(candidate_vectors, vector) / denom
    similarities = np.nan_to_num(similarities)

    best_idx = int(np.argmax(similarities))
    return candidate_labels[best_idx], float(similarities[best_idx])


def append_charges_values_with_ai(date_str: str, score_threshold: float = DEFAULT_SCORE_THRESHOLD) -> Path | None:
    """
    Append a ``charges_value`` column to the Klarna Season/Event CSV for ``date_str``.

    Rows are matched to charges ``total_name`` values using OpenAI embeddings for
    fuzzy similarity. When the best similarity score is below ``score_threshold``
    the ``charges_value`` cell is left empty.
    """

    klarna_df, klarna_path = _read_klarna_file(date_str)
    if klarna_df is None or klarna_path is None:
        return None

    charges_df = _read_charges_values(date_str)
    if charges_df is None:
        return None

    raw_events = list(klarna_df["Event"])
    event_texts = [text.strip() if isinstance(text, str) else None for text in raw_events]
    events_for_embeddings = [text for text in event_texts if text]

    total_names = _clean_strings(charges_df["total_name"].tolist())
    values_map = {
        name.strip(): charges_df.loc[idx, "value"]
        for idx, name in enumerate(charges_df["total_name"].tolist())
        if isinstance(name, str) and name.strip()
    }

    if not events_for_embeddings:
        log.error("No Event values available in %s to match.", klarna_path.name)
        return None
    if not total_names:
        log.error("No total_name values available in charges workbook %s.", f"charges_value_{date_str}.xlsx")
        return None

    client = OpenAI()

    log.info("Creating embeddings for %d charges total_name entries.", len(total_names))
    total_embeddings = np.array(_embed_texts(total_names, client))

    log.info("Creating embeddings for %d Klarna Event entries.", len(events_for_embeddings))
    event_embeddings = _embed_texts(events_for_embeddings, client)

    matched_values: list[float | None] = []
    embedding_index = 0
    for event in event_texts:
        if not event:
            matched_values.append(None)
            continue

        event_vector = np.array(event_embeddings[embedding_index], dtype=float)
        embedding_index += 1

        best_label, best_score = _best_match(event_vector, total_embeddings, total_names)

        if best_label is None or best_score is None or best_score < score_threshold:
            log.info("No confident match for Event '%s' (score %.3f).", event, best_score or 0)
            matched_values.append(None)
            continue

        matched_value = values_map.get(best_label)
        log.info(
            "Matched Event '%s' -> total_name '%s' with score %.3f (value=%s)",
            event,
            best_label,
            best_score,
            matched_value,
        )
        matched_values.append(matched_value)

    klarna_df["charges_value"] = matched_values
    klarna_df.to_csv(klarna_path, index=False)
    log.info("Updated %s with charges_value column.", klarna_path)
    return klarna_path


__all__ = ["append_charges_values_with_ai"]