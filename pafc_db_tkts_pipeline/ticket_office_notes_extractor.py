from __future__ import annotations

from pathlib import Path

import pandas as pd


def _read_excel_no_header(path: Path) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=0, header=None)


def extract_ticketoffice_notes(path: Path) -> str:
    """
    Extract free-text Notes from TicketOffice sheet.

    Looks for a "Notes:" label and then reads all text to the right and below.
    Returns "Null" if no notes found.
    """
    df = _read_excel_no_header(path)

    notes_pos = list(zip(*((df == "Notes:").values.nonzero())))
    notes_text = ""
    if notes_pos:
        nr, nc = notes_pos[0]
        collected: list[str] = []

        # Look on the same row as 'Notes:' and any rows below it,
        # in columns from nc onwards, skipping the label cell itself.
        for row in range(nr, df.shape[0]):
            row_vals: list[str] = []
            for col in range(nc, df.shape[1]):
                if row == nr and col == nc:
                    continue  # skip the "Notes:" label cell itself
                v = df.iat[row, col]
                if pd.isna(v):
                    continue
                s = str(v).strip()
                if s:
                    row_vals.append(s)
            if row_vals:
                collected.append(" ".join(row_vals))

        notes_text = " ".join(collected).strip()

    if not notes_text:
        notes_text = "Null"

    return notes_text
