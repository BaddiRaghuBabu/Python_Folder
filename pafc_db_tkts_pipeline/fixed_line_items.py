from __future__ import annotations

from pathlib import Path

from .logger import log
from .checksums import (
    # Stage 1 – discover & validate inputs
    stage1_discover_files,                       # saleitemsmop PDFs
    stage1_discover_ticketoffice_excels,         # TicketOffice Excels
    stage1_discover_charges_excels,              # Charges Excels
    stage1_discover_klarna_pdfs,                 # Klarna DailyTakings PDFs
    stage1_discover_klarna_seasoneventmop_pdfs,  # Klarna SeasonEvent MoP PDFs
    stage1_discover_membership_pdfs,             # Membership PDFs
    # Filename pattern checks / renames
    ensure_saleitemsmop_filename,
    ensure_ticketoffice_filename,
    ensure_charges_filename,
    ensure_klarna_filename,
    ensure_klarna_seasoneventmop_filename,
    ensure_membership_filename,
)
from .date_extractor import (
    extract_saleitemsmop_date,
    extract_ticketoffice_date,
    extract_charges_date,
    extract_klarna_dailytakings_date,
    extract_klarna_seasoneventmop_date,
    extract_membership_date,
)
from .ticket_office_notes_extractor import extract_ticketoffice_notes
from .saleitemsmop_total_amoun_extractor import extract_saleitemsmop_total_amount
from .membership_other_total_extractor import extract_membership_other_and_total
from .membership_miles_gross_extractor import extract_mddto_miles_gross
from .membership_misc_group_gross_extractor import extract_mddto_misc_group_gross
from .membership_waiting_list_gross_extractor import extract_mddto_waiting_list_gross
from .membership_total_all_sales_gross_extractor import (
    extract_mddto_total_all_sales_gross,
)
from .k_dailytakings_data_extractor import extract_klarna_dailytakings_mops
from .output import (
    write_summary,                     # saleitemsmop Excel (date, total_amount)
    write_ticketoffice_csv,            # TicketOffice CSV (date, notes)
    write_charges_csv,                 # Charges CSV (date)
    write_klarna_csv,                  # Klarna DailyTakings CSV (date + MoPs)
    write_klarna_seasoneventmop_csv,   # Klarna SeasonEvent MoP CSV (date)
    write_membership_csv,              # Membership CSV (date, other, totals, etc.)
)

from .klarna_seasonevent import (
    build_monthly_unique_events_list,
    export_klarna_seasonevent_tables,
)
# from .klarna_charges_value_enricher import enrich_klarna_tables_with_charges
from .charges_total_postel_charges import write_charges_postal_detail_excels
from .charges_totals_from_file import write_charges_totals_excels


# =====================================================================
# saleitemsmop mini-pipeline
# =====================================================================


def _stage2_build_saleitemsmop_rows(
    pdfs: list[Path],
) -> tuple[list[dict[str, str]], list[str]]:
    rows: list[dict[str, str]] = []
    errors: list[str] = []

    for pdf_path in pdfs:
        try:
            iso_date = extract_saleitemsmop_date(pdf_path)
            ensure_saleitemsmop_filename(pdf_path, iso_date)
            total = extract_saleitemsmop_total_amount(pdf_path)
            rows.append({"date": iso_date, "total_amount": total})
        except Exception as exc:  # noqa: BLE001
            msg = f"{pdf_path.name}: {exc}"
            errors.append(msg)
            log.error("saleitemsmop Stage 2 ERROR – %s", msg)

    if errors:
        log.error(
            "saleitemsmop Stage 2 FAILED – some PDF(s) had errors; see messages above."
        )
    else:
        log.info(
            "saleitemsmop Stage 2 PASS – rows built for all saleitemsmop PDFs."
        )

    return rows, errors


def run_saleitemsmop_pipeline(pdfs: list[Path] | None = None) -> int:
    log.info("saleitemsmop pipeline starting..")
    if pdfs is None:
        try:
            pdfs = stage1_discover_files()
        except Exception:  # noqa: BLE001
            return -1
    else:
        log.info("saleitemsmop Stage 1 – using prevalidated PDFs: %d", len(pdfs))

    rows, errors = _stage2_build_saleitemsmop_rows(pdfs)
    if errors or not rows:
        log.error(
            "saleitemsmop pipeline aborted – Stage 2 FAILED; Excel not created."
        )
        return -1

    write_summary(rows)
    log.info("saleitemsmop pipeline finished with %d record(s).", len(rows))
    return len(rows)


# =====================================================================
# TicketOffice mini-pipeline
# =====================================================================


def _stage2_build_ticketoffice_rows(
    files: list[Path],
) -> tuple[list[dict[str, str]], list[str]]:
    rows: list[dict[str, str]] = []
    errors: list[str] = []

    for path in files:
        try:
            iso_date = extract_ticketoffice_date(path)
            ensure_ticketoffice_filename(path, iso_date)
            notes = extract_ticketoffice_notes(path)
            rows.append({"date": iso_date, "notes": notes})
        except Exception as exc:  # noqa: BLE001
            msg = f"{path.name}: {exc}"
            errors.append(msg)
            log.error("TicketOffice Stage 2 ERROR – %s", msg)

    if errors:
        log.error(
            "TicketOffice Stage 2 FAILED – some Excel file(s) had errors; "
            "see messages above."
        )
    else:
        log.info(
            "TicketOffice Stage 2 PASS – rows built for all TicketOffice Excels."
        )

    return rows, errors


def run_ticketoffice_pipeline(excels: list[Path] | None = None) -> int:
    log.info("TicketOffice Daily Banking pipeline starting..")
    if excels is None:
        try:
            excels = stage1_discover_ticketoffice_excels()
        except Exception:  # noqa: BLE001
            return -1
    else:
        log.info("TicketOffice Stage 1 – using prevalidated Excels: %d", len(excels))

    rows, errors = _stage2_build_ticketoffice_rows(excels)
    if errors or not rows:
        log.error(
            "TicketOffice pipeline aborted – Stage 2 FAILED; CSV not created."
        )
        return -1

    write_ticketoffice_csv(rows)
    log.info("TicketOffice pipeline finished with %d record(s).", len(rows))
    return len(rows)


# =====================================================================
# Charges mini-pipeline
# =====================================================================


def _stage2_build_charges_rows(
    files: list[Path],
) -> tuple[list[dict[str, str]], list[str], list[Path]]:
    rows: list[dict[str, str]] = []
    errors: list[str] = []
    successful_paths: list[Path] = []

    for path in files:
        try:
            iso_date = extract_charges_date(path)
            ensure_charges_filename(path, iso_date)
            rows.append({"date": iso_date})
            successful_paths.append(path)
        except Exception as exc:  # noqa: BLE001
            msg = f"{path.name}: {exc}"
            errors.append(msg)
            log.error("Charges Stage 2 ERROR – %s", msg)

    if errors:
        log.error(
            "Charges Stage 2 FAILED – Charges Excel file(s) had errors; "
            "see messages above."
        )
    else:
        log.info(
            "Charges Stage 2 PASS – header dates verified and rows built."
        )

    return rows, errors, successful_paths

def run_charges_pipeline(excels: list[Path] | None = None) -> int:
    log.info("Charges Daily Banking pipeline starting..")
    if excels is None:
        try:
            excels = stage1_discover_charges_excels()
        except Exception:  # noqa: BLE001
            return -1
    else:
        log.info("Charges Stage 1 – using prevalidated Excels: %d", len(excels))
    
    rows, errors, successful_paths = _stage2_build_charges_rows(excels)
    if errors or not rows:
        log.error("Charges pipeline aborted – Stage 2 FAILED; CSV not created.")
        return -1
    
    _, postal_errors = write_charges_postal_detail_excels(successful_paths)
    if postal_errors:
        log.error(
            "Charges pipeline aborted – Postal Charge detail creation FAILED; "
            "CSV not created."
        )
        return -1


    totals_count, total_errors = write_charges_totals_excels(successful_paths)
    if total_errors or totals_count == 0:
        log.error(
            "Charges pipeline aborted – Charges totals extraction FAILED; "
            "Excel not created."
        )
        return -1

    write_charges_csv(rows)
    log.info("Charges pipeline finished with %d record(s).", len(rows))
    return len(rows)


# =====================================================================
# Klarna DailyTakings mini-pipeline (with MoP totals)
# =====================================================================


def _stage2_build_klarna_rows(
    pdfs: list[Path],
) -> tuple[list[dict[str, str]], list[str]]:
    """
    Build one row per Klarna DailyTakings PDF with:
        date,
        k_dailytakings_cash,
        k_dailytakings_credit,
        k_dailytakings_debit,
        k_dailytakings_voucher,
        k_dailytakings_account
    """
    rows: list[dict[str, str]] = []
    errors: list[str] = []

    for path in pdfs:
        try:
            pdf_path = Path(path)
            iso_date = extract_klarna_dailytakings_date(pdf_path)
            ensure_klarna_filename(pdf_path, iso_date)

            try:
                mop_values = extract_klarna_dailytakings_mops(pdf_path)
            except Exception as exc:  # noqa: BLE001
                log.error(
                    "Klarna Stage 2 – MoP extraction error for %s: %s",
                    pdf_path.name,
                    exc,
                )
                mop_values = {
                    "k_dailytakings_cash": "Data Unavailable",
                    "k_dailytakings_credit": "Data Unavailable",
                    "k_dailytakings_debit": "Data Unavailable",
                    "k_dailytakings_voucher": "Data Unavailable",
                    "k_dailytakings_account": "Data Unavailable",
                }

            row: dict[str, str] = {"date": iso_date}
            row.update(mop_values)
            rows.append(row)

        except Exception as exc:  # noqa: BLE001
            msg = f"{Path(path).name}: {exc}"
            errors.append(msg)
            log.error("Klarna Stage 2 ERROR – %s", msg)

    if errors:
        log.error(
            "Klarna Stage 2 FAILED – DailyTakings PDF(s) had errors; "
            "see messages above."
        )
    else:
        log.info(
            "Klarna Stage 2 PASS – DailyTakings header dates verified and "
            "rows built."
        )

    return rows, errors


def run_klarna_pipeline(pdfs: list[Path] | None = None) -> int:
    log.info("Klarna DailyTakings pipeline starting..")
    if pdfs is None:
        try:
            pdfs = stage1_discover_klarna_pdfs()
        except Exception:  # noqa: BLE001
            return -1
    else:
        log.info("Klarna Stage 1 – using prevalidated PDFs: %d", len(pdfs))

    rows, errors = _stage2_build_klarna_rows(pdfs)
    if errors or not rows:
        log.error("Klarna pipeline aborted – Stage 2 FAILED; CSV not created.")
        return -1

    write_klarna_csv(rows)
    log.info("Klarna DailyTakings pipeline finished with %d record(s).", len(rows))
    return len(rows)


# =====================================================================
# Klarna Season/Event MoP mini-pipeline
# =====================================================================


def _stage2_build_klarna_seasoneventmop_rows(
    pdfs: list[Path],
) -> tuple[list[dict[str, str]], list[str], list[Path]]:    
    rows: list[dict[str, str]] = []
    errors: list[str] = []
    processed_paths: list[Path] = []

    for path in pdfs:
        try:
            iso_date = extract_klarna_seasoneventmop_date(path)
            ensure_klarna_seasoneventmop_filename(path, iso_date)
            expected_path = path.with_name(
                f"klarna_seasoneventmop_{iso_date}{path.suffix.lower()}"
            )
            processed_paths.append(expected_path if expected_path.exists() else path)
            rows.append({"date": iso_date})
        except Exception as exc:  # noqa: BLE001
            msg = f"{path.name}: {exc}"
            errors.append(msg)
            log.error("Klarna SeasonEvent MoP Stage 2 ERROR – %s", msg)

    if errors:
        log.error(
            "Klarna SeasonEvent MoP Stage 2 FAILED – PDF(s) had errors; "
            "see messages above."
        )
    else:
        log.info(
            "Klarna SeasonEvent MoP Stage 2 PASS – header dates verified and "
            "rows built."
        )

    return rows, errors, processed_paths

def run_klarna_seasoneventmop_pipeline(pdfs: list[Path] | None = None) -> int:
    log.info("Klarna SeasonEvent MoP pipeline starting..")
    if pdfs is None:
        try:
            pdfs = stage1_discover_klarna_seasoneventmop_pdfs()
        except Exception:  # noqa: BLE001
            return -1
    else:
        log.info("Klarna SeasonEvent MoP Stage 1 – using prevalidated PDFs: %d", len(pdfs))

    rows, errors, processed_paths = _stage2_build_klarna_seasoneventmop_rows(pdfs)    
    if errors or not rows:
        log.error(
            "Klarna SeasonEvent MoP pipeline aborted – Stage 2 FAILED; "
            "CSV not created."
        )
        return -1
    
    export_klarna_seasonevent_tables(processed_paths)
    build_monthly_unique_events_list()
    if not enrich_klarna_tables_with_charges(processed_paths):
        log.error(
            "Klarna SeasonEvent MoP pipeline aborted – charges/value enrichment FAILED."
        )
        return -1
    write_klarna_seasoneventmop_csv(rows)
    log.info(
        "Klarna SeasonEvent MoP pipeline finished with %d record(s).",
        len(rows),
    )
    return len(rows)


# =====================================================================
# Membership Daily Detailed Totals mini-pipeline
# =====================================================================


def _stage2_build_membership_rows(
    pdfs: list[Path],
) -> tuple[list[dict[str, str]], list[str]]:
    rows: list[dict[str, str]] = []
    errors: list[str] = []

    for path in pdfs:
        try:
            pdf_path = Path(path)
            iso_date = extract_membership_date(pdf_path)
            ensure_membership_filename(pdf_path, iso_date)

            # Evergreen "Other" + "Total"
            other_val, total_val = extract_membership_other_and_total(pdf_path)

            # Miles Away Travel Club gross
            try:
                miles_gross = extract_mddto_miles_gross(pdf_path)
            except Exception as exc:  # noqa: BLE001
                log.error(
                    "Membership Stage 2 – Miles gross extraction error for %s: %s",
                    pdf_path.name,
                    exc,
                )
                miles_gross = "Data Unavailable"

            # Misc Group gross
            try:
                misc_gross = extract_mddto_misc_group_gross(pdf_path)
            except Exception as exc:  # noqa: BLE001
                log.error(
                    "Membership Stage 2 – Misc Group gross extraction error for %s: %s",
                    pdf_path.name,
                    exc,
                )
                misc_gross = "Data Unavailable"

            # Waiting List gross
            try:
                waiting_gross = extract_mddto_waiting_list_gross(pdf_path)
            except Exception as exc:  # noqa: BLE001
                log.error(
                    "Membership Stage 2 – Waiting List gross extraction error for %s: %s",
                    pdf_path.name,
                    exc,
                )
                waiting_gross = "Data Unavailable"

            # Total All Sales gross
            try:
                total_all_sales_gross = extract_mddto_total_all_sales_gross(pdf_path)
            except Exception as exc:  # noqa: BLE001
                log.error(
                    "Membership Stage 2 – Total All Sales gross extraction error for %s: %s",
                    pdf_path.name,
                    exc,
                )
                total_all_sales_gross = "Data Unavailable"

            rows.append(
                {
                    "date": iso_date,
                    "other": other_val,
                    "total": total_val,
                    "mddto_miles_gross": miles_gross,
                    "mddto_misc_group_gross": misc_gross,
                    "mddto_waiting_list": waiting_gross,
                    "mddto_total_all_sales": total_all_sales_gross,
                }
            )
        except Exception as exc:  # noqa: BLE001
            msg = f"{Path(path).name}: {exc}"
            errors.append(msg)
            log.error("Membership Stage 2 ERROR – %s", msg)

    if errors:
        log.error(
            "Membership Stage 2 FAILED – Membership PDF(s) had errors; "
            "see messages above."
        )
    else:
        log.info(
            "Membership Stage 2 PASS – header dates, filenames and "
            "MOP totals / gross values verified and rows built."
        )

    return rows, errors


def run_membership_pipeline(pdfs: list[Path] | None = None) -> int:
    log.info("Membership Daily Detailed Totals pipeline starting..")
    if pdfs is None:
        try:
            pdfs = stage1_discover_membership_pdfs()
        except Exception:  # noqa: BLE001
            return -1
    else:
        log.info("Membership Stage 1 – using prevalidated PDFs: %d", len(pdfs))

    rows, errors = _stage2_build_membership_rows(pdfs)
    if errors or not rows:
        log.error(
            "Membership pipeline aborted – Stage 2 FAILED; CSV not created."
        )
        return -1

    write_membership_csv(rows)
    log.info(
        "Membership Daily Detailed Totals pipeline finished with %d record(s).",
        len(rows),
    )
    return len(rows)
