from __future__ import annotations

"""
Helpers to build aggregate_data.csv from the per-mini-pipeline summaries.

Typical flow:

  1. build_aggregate_base_with_saleitemsmop()
        -> creates aggregate_data.csv with:
               date, ticketoffice_notes, saleitemsmop_total

  2. build_membership_other_total_columns()
        -> adds:
               mddto_evergreen_other, mddto_evergreen_total

  3. build_membership_miles_gross_column()
        -> adds:
               mddto_miles_gross

  4. build_membership_misc_group_gross_column()
        -> adds:
               mddto_misc_group_gross

  5. build_membership_waiting_list_gross_column()
        -> adds:
               mddto_waiting_list

  6. build_membership_total_all_sales_gross_column()
        -> adds:
               mddto_total_all_sales

  7. build_klarna_dailytakings_data_columns()
        -> adds:
               k_dailytakings_cash,
               k_dailytakings_credit,
               k_dailytakings_debit,
               k_dailytakings_voucher,
               k_dailytakings_account
  8. build_xero_on_account_column()
        -> adds:
            xero_on_account
  9. build_total_ticketing_income_column()
        -> adds:
            Total Ticketing Income
  11. build_xero_ccdva_less_charges_column()
       -> adds:
            Total Postal Charges
  10. build_xero_ccdva_less_charges_column()
        -> adds:
            xero_ccdva_less_charges

  12. build_xero_evergreen_column()       
        -> adds:
            xero_evergreen
  13. build_xero_booking_fee_column()           
       -> adds:
            xero_booking_fee
  14. build_xero_postage_column()      
       -> adds:
            xero_postage
"""

from .saleitemsmop_total_amount_aggregate import (
    build_aggregate_base_with_saleitemsmop,
)
from .membership_other_total_aggregate import (
    build_membership_other_total_columns,
)
from .membership_miles_gross_aggregete import (
    build_membership_miles_gross_column,
)
from .membership_misc_group_gross_aggregete import (
    build_membership_misc_group_gross_column,
)
from .membership_waiting_list_gross_aggregete import (
    build_membership_waiting_list_gross_column,
)
from .membership_total_all_sales_gross_aggregete import (
    build_membership_total_all_sales_gross_column,
)
from .k_dailytakings_data_aggregate import (
    build_klarna_dailytakings_data_columns,
)
from .SeasonEvent_total_ticketing_income_data_aggregate import (
    build_total_ticketing_income_column,
)
from .SeasonEvent_total_postel_charges_data_aggregate import (
    build_total_postal_charges_column,
)
from .xero_ccdva_less_charges_aggregate import (
    build_xero_ccdva_less_charges_column,
    
)
from .xero_on_account_aggregate import build_xero_on_account_column
from .xero_evergreen_aggregate import build_xero_evergreen_column
from .xero_booking_fee_aggregate import build_xero_booking_fee_column
from .xero_postage_aggregate import build_xero_postage_column

