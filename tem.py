# from __future__ import annotations

# from pathlib import Path
# import re
# import math
# from typing import List, Dict, Optional

# import pandas as pd


# # -------------------------------------------------------------
# # CONFIG – your folders
# # -------------------------------------------------------------
# INPUT_DIR = Path(
#     r"C:\Users\RaghuBaddi\OneDrive - Valuenode Private Limited\RB VD SHARE\TKTS\Inputs\charges_YYYYMMDD"
# )
# OUTPUT_DIR = Path(
#     r"C:\Users\RaghuBaddi\OneDrive - Valuenode Private Limited\RB VD SHARE\TKTS\outputs\all_values"
# )
# OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# TOTAL_PREFIX = "Total "


# def extract_date_from_filename(filename: str) -> Optional[str]:
#     """
#     Extract YYYYMMDD from filenames like 'charges_20250408.xls' -> '20250408'.
#     """
#     m = re.search(r"(\d{8})", filename)
#     if not m:
#         return None
#     yyyymmdd = m.group(1)
#     return yyyymmdd   # <- no hyphens, exactly what you asked


# def parse_float(value) -> Optional[float]:
#     """
#     Convert cell value to float, skipping blanks/NaN.
#     Handles: 8, 8.50, '1,700.00', '(2.00)'.
#     """
#     if isinstance(value, float) and math.isnan(value):
#         return None

#     if value is None:
#         return None

#     if isinstance(value, (int, float)):
#         return float(value)

#     s = str(value).strip()
#     if not s or s.lower() == "nan":
#         return None

#     negative = s.startswith("(") and s.endswith(")")
#     s = s.strip("()").replace(",", "")

#     try:
#         num = float(s)
#     except ValueError:
#         return None

#     if negative:
#         num = -num

#     return num


# def extract_totals_from_file(path: Path) -> List[Dict[str, object]]:
#     """
#     Read one charges_YYYYMMDD.xls and return all totals inside INCOME sections:

#       - any row where some cell starts with 'Total '
#       - for that row, take the **2nd numeric value** as the 'Value'
#         (1st = Number of Charges, 2nd = Value)
#       - skip rows under 'Method of Payment' and 'NON INCOME'
#     """
#     print(f"Extracting totals from: {path.name}")
#     df = pd.read_excel(path, header=None)

#     results: List[Dict[str, object]] = []
#     inside_income = False
#     date_str = extract_date_from_filename(path.name)

#     for _, row in df.iterrows():
#         row_list = list(row)

#         # Normalised text for each cell
#         texts = []
#         for v in row_list:
#             if isinstance(v, float) and math.isnan(v):
#                 texts.append("")
#             elif v is None:
#                 texts.append("")
#             else:
#                 texts.append(str(v).strip())

#         # Turn INCOME mode on/off
#         if any(t == "INCOME" for t in texts):
#             inside_income = True
#             continue

#         if any(t == "Method of Payment" for t in texts) or any(
#             t == "NON INCOME" for t in texts
#         ):
#             inside_income = False
#             continue

#         if not inside_income:
#             continue

#         # Find a cell that starts with 'Total '
#         total_name = None
#         for t in texts:
#             if t.startswith(TOTAL_PREFIX):
#                 total_name = t
#                 break

#         if not total_name:
#             continue

#         # Collect all numeric values in that row (skip NaN/blanks)
#         numeric_vals: List[float] = []
#         for cell in row_list:
#             num = parse_float(cell)
#             if num is not None:
#                 numeric_vals.append(num)

#         # Decide which numeric is the 'Value'
#         if not numeric_vals:
#             value = None
#         elif len(numeric_vals) >= 2:
#             # 1st = Number of Charges, 2nd = Value
#             value = numeric_vals[1]
#         else:
#             value = numeric_vals[0]

#         results.append(
#             {
#                 "date": date_str,       # <-- YYYYMMDD
#                 "file": path.name,      # internal, we will drop later
#                 "total_name": total_name,
#                 "value": value,
#             }
#         )

#     return results


# def main() -> None:
#     all_rows: List[Dict[str, object]] = []

#     for file in sorted(INPUT_DIR.glob("*.xls")):
#         rows = extract_totals_from_file(file)
#         if not rows:
#             continue

#         all_rows.extend(rows)

#         # ---- per-file output: charges_value_YYYYMMDD.xlsx ----
#         df_file = pd.DataFrame(rows)
#         df_file = df_file[["date", "total_name", "value"]]  # remove 'file'
#         date_str = rows[0]["date"] or "unknown"
#         per_file_out = OUTPUT_DIR / f"charges_value_{date_str}.xlsx"
#         df_file.to_excel(per_file_out, index=False)
#         print(f"  wrote per-file summary: {per_file_out.name}")

#     if not all_rows:
#         print("No totals detected in any file.")
#         return

#     # ---- combined output for all dates (optional, nice summary) ----
#     out_df = pd.DataFrame(all_rows)
#     out_df = out_df[["date", "total_name", "value"]]  # drop 'file'
#     combined_out = OUTPUT_DIR / "charges_totals_all_dates.xlsx"
#     out_df.to_excel(combined_out, index=False)

#     print("=========================================")
#     print(f"✔ Total rows written: {len(out_df)}")
#     print(f"➡ Combined file: {combined_out.name}")
#     print("=========================================")


# if __name__ == "__main__":
#     main()
