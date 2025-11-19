# from __future__ import annotations

# """Enrich Klarna Season/Event MoP tables with charges totals via AI matching."""

# from pathlib import Path
# import math
# import os
# import re
# from typing import Iterable

# import numpy as np
# import pandas as pd
# from openai import OpenAI

# from .config import CHARGES_TOTALS_OUTPUT_DIR, KLARNA_SEMOP_TABLE_OUTPUT_DIR
# from .logger import log


# def _extract_iso_date_from_name(path: Path) -> str | None:
#     match = re.search(r"(\d{8})", path.stem)
#     if not match:
#         return None
#     return match.group(1)


# def _load_charges_totals(date_str: str) -> pd.DataFrame | None:
#     charges_path = CHARGES_TOTALS_OUTPUT_DIR / f"charges_value_{date_str}.xlsx"
#     if not charges_path.exists():
#         log.warning(
#             "Charges/Klarna enrichment – charges workbook %s not found.",
#             charges_path,
#         )
#         return None

#     try:
#         df = pd.read_excel(charges_path, dtype={"total_name": str})
#     except Exception as exc:  # noqa: BLE001
#         log.error(
#             "Charges/Klarna enrichment – failed to read %s: %s",
#             charges_path,
#             exc,
#         )
#         return None

#     required_columns = {"total_name", "value"}
#     if not required_columns.issubset(df.columns):
#         log.error(
#             "Charges/Klarna enrichment – %s missing required column(s) %s.",
#             charges_path,
#             required_columns - set(df.columns),
#         )
#         return None

#     cleaned = df.copy()
#     cleaned["total_name"] = cleaned["total_name"].astype(str).str.strip()
#     cleaned["value"] = cleaned["value"].apply(lambda x: None if pd.isna(x) else x)
#     cleaned = cleaned[cleaned["total_name"] != ""]
#     if cleaned.empty:
#         log.warning(
#             "Charges/Klarna enrichment – charges workbook %s had no usable rows.",
#             charges_path,
#         )
#         return None

#     return cleaned.reset_index(drop=True)


# def _build_openai_client() -> OpenAI | None:
#     api_key = os.environ.get("OPENAI_API_KEY")
#     if not api_key:
#         log.error(
#             "Charges/Klarna enrichment – OPENAI_API_KEY is not set; cannot run AI matching."
#         )
#         return None

#     return OpenAI(api_key=api_key)


# def _cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
#     a = np.array(vec_a)
#     b = np.array(vec_b)
#     denom = np.linalg.norm(a) * np.linalg.norm(b)
#     if denom == 0:
#         return 0.0
#     return float(np.dot(a, b) / denom)


# _MIN_SIMILARITY_SCORE = 0.7


# def _embed_texts(client: OpenAI, texts: list[str]) -> list[list[float]]:
#     response = client.embeddings.create(model="text-embedding-3-small", input=texts)
#     return [item.embedding for item in response.data]


# # ---------------------------------------------------------------------------
# # Helper to detect coach / travel products
# # ---------------------------------------------------------------------------
# def _is_coach_like(text: str) -> bool:
#     """
#     Return True if this name looks like a coach / travel product.

#     In this project we treat as coach-like if it contains:
#     - 'coach'
#     - 'travel'
#     - 'co' as a whole word (short for 'Coach', e.g. 'Wycombe Wanderers Co')
#     """
#     s = str(text).lower()

#     # direct words
#     if "coach" in s or "travel" in s:
#         return True

#     # 'co' ALWAYS means 'coach' for us (whole word only)
#     if re.search(r"\bco\b", s):
#         return True

#     return False


# def _normalize_label(label: str) -> str:
#     """Return a simplified label to catch common name/date variations.

#     - lower case
#     - remove 'total'
#     - remove 'travel'
#     - strip dates like '11/11/25'
#     - remove punctuation
#     - collapse spaces
#     - simple plural normalisation: words ending with 's' (len > 4)
#       lose the final 's' -> 'rovers' -> 'rover', 'wanderers' -> 'wanderer'.
#     """

#     cleaned = label.lower()
#     cleaned = re.sub(r"\btotal\b", "", cleaned)
#     cleaned = re.sub(r"\btravel\b", "", cleaned)
#     cleaned = re.sub(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", "", cleaned)
#     cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
#     cleaned = re.sub(r"\s+", " ", cleaned).strip()

#     tokens = cleaned.split()
#     norm_tokens = [
#         t[:-1] if t.endswith("s") and len(t) > 4 else t
#         for t in tokens
#     ]
#     return " ".join(norm_tokens)


# # ---------------------------------------------------------------------------
# # CHANGED: now returns index into total_names list (not the name itself)
# # ---------------------------------------------------------------------------
# def _build_normalized_total_map(total_names: list[str]) -> dict[str, int]:
#     """
#     Map normalised label -> first index in total_names that produced it.
#     """
#     normalized: dict[str, int] = {}
#     for idx, name in enumerate(total_names):
#         key = _normalize_label(name)
#         if key not in normalized:
#             normalized[key] = idx
#     return normalized


# # ---------------------------------------------------------------------------
# # CHANGED: enforce coach/non-coach separation + single use of each total
# # ---------------------------------------------------------------------------
# def _match_event_to_total(
#     client: OpenAI,
#     event: str,
#     total_names: list[str],
#     total_embeddings: list[list[float]],
#     normalized_totals: dict[str, int],
#     total_is_coach: list[bool],
#     used_indices: set[int],
# ) -> tuple[int, float] | None:
#     """
#     Return (index_into_total_names, similarity_score) or None.

#     Rules:
#     - If normalised labels match, we use that *if* coach/non-coach type matches
#       and the total has not already been used.
#     - Otherwise we fall back to embedding similarity, but restrict to:
#         * same coach/non-coach type
#         * not previously used
#     """
#     event_is_coach = _is_coach_like(event)
#     normalized_event = _normalize_label(event)

#     # 1) Exact normalised match first
#     if normalized_event in normalized_totals:
#         idx = normalized_totals[normalized_event]
#         if total_is_coach[idx] == event_is_coach and idx not in used_indices:
#             return idx, 1.0
#         # if type doesn't match or already used -> ignore and try embeddings

#     # 2) Embedding-based match with filters
#     event_embedding = _embed_texts(client, [event])[0]
#     best_index: int | None = None
#     best_score = -math.inf

#     for idx, total_embedding in enumerate(total_embeddings):
#         # do not reuse the same total_name for multiple events
#         if idx in used_indices:
#             continue

#         # force coach <-> coach and non-coach <-> non-coach
#         if total_is_coach[idx] != event_is_coach:
#             continue

#         score = _cosine_similarity(event_embedding, total_embedding)
#         if score > best_score:
#             best_score = score
#             best_index = idx

#     if best_index is None:
#         return None
#     if best_score < _MIN_SIMILARITY_SCORE:
#         return None

#     return best_index, best_score


# def _enrich_csv(
#     client: OpenAI,
#     csv_file: Path,
#     charges_df: pd.DataFrame,
#     charges_embeddings: list[list[float]],
# ) -> bool:
#     try:
#         df = pd.read_csv(csv_file)
#     except Exception as exc:  # noqa: BLE001
#         log.error(
#             "Charges/Klarna enrichment – failed to read Klarna table %s: %s",
#             csv_file.name,
#             exc,
#         )
#         return False

#     if "Event" not in df.columns:
#         log.warning(
#             "Charges/Klarna enrichment – file %s missing Event column; skipping.",
#             csv_file.name,
#         )
#         return False

#     total_names = charges_df["total_name"].tolist()
#     normalized_totals = _build_normalized_total_map(total_names)

#     # NEW: pre-compute coach / non-coach flag for each total_name
#     total_is_coach = [_is_coach_like(name) for name in total_names]

#     # NEW: track which totals have already been used so we don't reuse them
#     used_indices: set[int] = set()

#     event_matches: list[float | str] = []

#     for event in df["Event"].astype(str):
#         event_clean = event.strip()
#         if not event_clean or event_clean.casefold() == "total income":
#             event_matches.append("")
#             continue

#         match = _match_event_to_total(
#             client,
#             event_clean,
#             total_names,
#             charges_embeddings,
#             normalized_totals,
#             total_is_coach,
#             used_indices,
#         )
#         if match is None:
#             # No suitable total_name (or below similarity threshold)
#             event_matches.append(0)
#             continue

#         matched_index, _score = match
#         used_indices.add(matched_index)

#         value = charges_df.iloc[matched_index]["value"]
#         event_matches.append(value)

#     df["charges_value"] = event_matches

#     try:
#         df.to_csv(csv_file, index=False)
#     except Exception as exc:  # noqa: BLE001
#         log.error(
#             "Charges/Klarna enrichment – failed to write updated Klarna table %s: %s",
#             csv_file.name,
#             exc,
#         )
#         return False

#     return True


# def enrich_klarna_tables_with_charges(pdf_paths: Iterable[Path]) -> bool:
#     client = _build_openai_client()
#     if client is None:
#         return True
#     processed_any = False
#     total_success = True

#     for pdf_path in pdf_paths:
#         date_str = _extract_iso_date_from_name(Path(pdf_path))
#         if not date_str:
#             log.warning(
#                 "Charges/Klarna enrichment – could not determine date from %s; skipping.",
#                 Path(pdf_path).name,
#             )
#             continue

#         charges_df = _load_charges_totals(date_str)
#         if charges_df is None:
#             continue

#         usable_charges_df = charges_df[
#             charges_df["total_name"].str.casefold() != "total income"
#         ].reset_index(drop=True)
#         if usable_charges_df.empty:
#             log.warning(
#                 "Charges/Klarna enrichment – charges workbook %s contains only ignored totals; skipping.",
#                 charges_df,
#             )
#             continue

#         charges_embeddings = _embed_texts(
#             client, usable_charges_df["total_name"].tolist()
#         )

#         csv_file = KLARNA_SEMOP_TABLE_OUTPUT_DIR / f"{Path(pdf_path).stem}.csv"
#         if not csv_file.exists():
#             log.warning(
#                 "Charges/Klarna enrichment – Klarna table %s not found; skipping.",
#                 csv_file.name,
#             )
#             continue

#         processed_any = True
#         if not _enrich_csv(client, csv_file, usable_charges_df, charges_embeddings):
#             total_success = False

#     if not processed_any:
#         log.info(
#             "Charges/Klarna enrichment – no Klarna Season/Event tables were updated;",
#             " skipping enrichment.",
#         )
#         return True
#     return total_success


# __all__ = ["enrich_klarna_tables_with_charges"]
