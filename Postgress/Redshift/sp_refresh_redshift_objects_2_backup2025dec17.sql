-- DROP PROCEDURE edw_prod_dbo.sp_refresh_redshift_objects_2();

CREATE OR REPLACE PROCEDURE edw_prod_dbo.sp_refresh_redshift_objects_2_backup2025dec17()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_start_time TIMESTAMPTZ := clock_timestamp();
    v_rows BIGINT := 0;
BEGIN
    PERFORM set_config('datestyle', 'ISO, MDY', true);

    -- Log Start
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'START', 'START', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 1. dm_ar_invoice_sl_v (FULL)
    v_start_time := clock_timestamp();
    TRUNCATE TABLE edw_prod.redshift_sync.dm_ar_invoice_sl_v;
    INSERT INTO edw_prod.redshift_sync.dm_ar_invoice_sl_v
    ( source_system, trx_number, trx_date, line_type_code, invoice_line_number, line_description, term_due_date, interface_header_context, interface_line_context, exchange_date, exchange_rate, invoice_currency_code, customer_trx_id, customer_trx_line_id, link_to_cust_trx_line_id, memo_line_id, entered_dr, entered_cr, accounted_dr, accounted_cr, ar_acctd_amount, je_header_id, je_name, je_category, je_source, je_status, je_date_created, je_posted_date, je_line_num, je_line_description, status, je_order_number, je_order_line, period_name, period_set_name, start_date, end_date, period_num, entered_period_name, adjustment_period, year_start_date, year_end_date, quarter_num, quarter_start_date, quarter_end_date, "year", fiscal_period, fiscal_quarter, code_combination_id, gl_code_combination, account_type, company, company_desc, product_code, product_code_desc, department, department_desc, natural_account, natural_account_desc, intercompany_code, intercompany_desc, detail_posting_allowed_flag, detail_budgeting_allowed_flag, enabled_flag, summary_flag, template_id, allocation_create_flag, org_id, ou_code, ou_name, cust_trx_type_id, ar_trx_type, ar_trx_type_desc, ar_trx_class, bt_site_use_id, bt_customer_account, bt_customer_name, bt_account_name, bt_customer_class_code, bt_semi_non_semi, bt_site_use_code, bt_site_number, bt_site_name, bt_address_1, bt_address_2, bt_address_3, bt_address_4, bt_city, bt_state, bt_postal_code, bt_country, bt_region, bt_territory, bt_channel, bt_sales_rep, bt_location, bt_primary_flag, bt_status, bt_primary_market_segment, bt_secondary_market_segment, bt_party_id, bt_party_number, bt_party_type, bt_cust_account_id, bt_account_status, bt_site_status, st_site_use_id, st_customer_account, st_customer_name, st_account_name, st_customer_class_code, st_semi_non_semi, st_site_use_code, st_site_number, st_site_name, st_address_1, st_address_2, st_address_3, st_address_4, st_city, st_state, st_postal_code, st_country, st_region, st_territory, st_channel, st_sales_rep, st_location, st_primary_flag, st_status, st_primary_market_segment, st_secondary_market_segment, st_party_id, st_party_number, st_party_type, st_cust_account_id, st_account_status, st_site_status, header_id, line_id, order_number, life_cycle_status, order_header_type, order_status, order_currency, hold_type, customer_po, order_source, order_source_reference, service_activity, service_agreement_number, service_agreement_description, order_date, order_creation_date, order_created_by, booked_date, backlog_date, bill_only_ship_date, request_date, promise_date, schedule_date, actual_ship_date, fulfillment_date, actual_fulfillment_date, latest_acceptable_date, booked_flag, cancelled_flag, fulfilled_flag, line_number, line_status, line_category, line_type, line_type_name, price_list, price_override, product_class, classification, shipment_number, ordered_item, item_type_code, modifier_name, repair_type, return_reason_code, subinventory, tax_code, tax_date, tax_excempt_flag, sales_account, mktg_account_rule, mktg_channel, open_flag, pricing_date, shippable_flag, calc_price_flag, fob_point_code, freight_carrier_code, mktg_product_channel, tax_point_code, schedule_status_code, shipment_priority_code, price_request_code, shipping_method_code, order_uom, pricing_uom, source_type, line_creation_date, line_created_by, inv_trx_number, inv_trx_date, inv_trx_rate_type, inv_exch_date, inv_exch_rate, inv_gl_date, inv_currency_code, ic_po_number, ic_po_line, ref_order_number, ref_order_line, org_code, org_name, org_type, region, src_org_id, inventory_item_id, organization_id, item_number, item_description, organization_code, organization_name, default_ship_org, internal_item, product_family, product_line, product_model, commodity_class, category, sourcing_segment, country_of_origin, eccn, copy_segment, where_used, category_name)
    select * from edw_prod_dbo.dm_ar_invoice_sl_v;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'dm_ar_invoice_sl_v', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 2. fnc_ar_invoice_sl_mv (FULL)
    v_start_time := clock_timestamp();
    TRUNCATE TABLE redshift_sync.fnc_ar_invoice_sl_mv;
    INSERT INTO redshift_sync.fnc_ar_invoice_sl_mv
    ( general_ledger_date, ar_gl_posted_date, customer_class, bill_to_site_name, bill_to_region, bill_to_country, ship_to_site_name, ship_to_region, ship_to_country, transaction_type_name, transaction_date, transaction_number, transaction_line_number, quantity_ordered, invoice_currency_code, order_type, sales_order_number, cust_po_number, repair_type, sales_order_line, line_type_description, inventory_org, ordered_item, item_number, item_description, product_family, product_line, product_model,label_l5, "Product Code & Label", Segment_L1, "Family Segment (L3)", "Product/Segment (L2)", "Family Group (L4)", distribution_amount, functional_distribution_amount, revenue_account, product_segment, account_segment, service_agreement_number, fob_point_code, actual_shipment_date, posting_flag, operating_unit, conversion_rate, distribution_amt_usd, functional_currency, unit_list_price, unit_selling_price, bill_to_customer_name, "Order Taker", "Original Booked Date", "Bill-To Territory", "Ship-To Territory", material_cost, material_overhead_cost, resource_cost, outside_processing_cost, overhead_cost, commodity_code_segment1, invoice_sales_rep, order_source, order_taker, payment_terms, mfg_site, region, sec_financial_reporting, delivery_number, tracking_number, shipment_type, period_name, so_qty_shipped, so_ship_to_country, so_payment_terms, "Cost Account", reference_account, total_distribution_cost, planned_rate_at_mfg_org_curr_to_usd, mfg_frozen_cost_at_planned_in_usd, per_avg_rate_at_mfg_org_curr_to_usd, mfg_frozen_cost_at_per_avg_in_usd, source_system, commercial_invoice_number, so_payment_header_terms)
    Select * from edw_prod_dbo.fnc_ar_invoice_sl_mv;  
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'fnc_ar_invoice_sl_mv', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 3. fnc_ar_je_mv (FULL)
    v_start_time := clock_timestamp();
    Truncate Table redshift_sync.fnc_ar_je_mv;
    INSERT INTO redshift_sync.fnc_ar_je_mv
    ( ou_code, product_family, product_line, product_model, bill_to_customer_name, bill_to_customer_class_code, je_line_num, order_type_name, order_number, order_line_number, order_shipment_number, inv_trx_number, inv_line_number, qty_ordered, gl_date, functional_currency, total_sales_functional, conversion_rate, total_sales_usd, sales_account, natural_account, product_code, account_type, item_number, item_description, fiscal_month, fiscal_year, je_name, region, sec_financial_reporting, ship_to_country, customer_po, service_agreement_number, bill_to_country, bill_to_region, source_type)
    Select * from edw_prod_dbo.fnc_ar_je_mv;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'fnc_ar_je_mv', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 4. ar_aging_ebs (FULL)
    v_start_time := clock_timestamp();
    TRUNCATE TABLE redshift_sync.ar_aging_ebs;
    INSERT INTO redshift_sync.ar_aging_ebs
    ( operating_unit, account_number, customer_name, site_name, site_number, location_number, collector_name, doc_type, reference_number, invoice_date, due_date, sales_order, purchase_order, sales_rep_name, payment_terms, days_late, pct_unpaid, invoice_currency_code, original_amount, outstanding_amount, sob_outstanding_amount, b1, b2, b3, b4, b5, b6, b7, h1, h2, h3, h4, h5, h6, h7, primary_market_segment, secondary_market_segment, receivables_account, region, territory, channel, country, attribute12, internal_notes) 
    sELECT * FROM oc_prod_rpt.ar_aging_ebs;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'ar_aging_ebs', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 5. AR_AGING_OC (FULL)
    v_start_time := clock_timestamp();
    TRUNCATE TABLE redshift_sync.AR_AGING_OC;
    insert into redshift_sync.AR_AGING_OC(report_run_date, transaction_type, "class", legal_entity, bill_to_customer_number, bill_to_customer_name, bill_to_location, bill_to_site, bill_to_region, bill_to_territory, trx_number, creation_date, trx_date, gl_date, due_date, payment_terms, collector, days_past_due_date, salesperson, line_number, project_number, project_name, purchase_order, sales_order, entered_currency, original_amount, entered_currency_balance, functional_currency, functional_currency_balances, accounting_class, functional_distribution_amount, business_unit, entity, product_class, department, natural_account, intercompany, functional_allocated, usd_allocated, pl_description, "current", days_30, days_31_60, days_61_90, days_91_180, days_181_360, days_360, load_date)
    select * from  oc_prod_rpt.AR_AGING_OC  ;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'AR_AGING_OC', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 6. ap_aging_ebs (FULL)
    v_start_time := clock_timestamp();
    TRUNCATE TABLE redshift_sync.ap_aging_ebs;   
    INSERT INTO redshift_sync.ap_aging_ebs
    ( supplier_name, supplier_number, supplier_site_code, supplier_country, invoice_number, distribution_line_number, invoice_type, invoice_date, due_date, gl_period, days_past_due, total_invoice_amt, freight_amt, trx_distrib_amt, func_distrib_amt, line_type, invoice_currency, exchange_rate, current_bal, thirty_days, sixty_days, ninety_days, gt_ninety_days, po_number, po_line, lookup_code, purchase_basis, match_basis, match_type, qty_invoiced, unit_price, extended_price, expense_account, item_number, item_description, product_family, product_class, cloud_ar_inv, cloud_ar_inv_line, org_name, ap_dist_account)  
    Select * from oc_prod_rpt.ap_aging_ebs;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'ap_aging_ebs', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 7. ap_aging_oc (FULL)
    v_start_time := clock_timestamp();
    truncate table redshift_sync.ap_aging_oc;
    INSERT INTO redshift_sync.ap_aging_oc
    (report_run_date, supplier_name, supplier_number, 
    supplier_site_code, supplier_country, business_unit, legal_entity, invoice_number, invoice_type, invoice_date, due_date, hold_status, days_past_due, amount_due_remaining, amount_due_original, invoice_currency, invoice_amount, base_amount, exchange_rate, "current", days_1_30, days_31_60, days_61_90, days_90, trade_account, load_date)
    select * from oc_prod_rpt.ap_aging_oc   ;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'ap_aging_oc', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 8. Baseline Tables (Small Tables)
    -- Price Increase Alias Mapping
    v_start_time := clock_timestamp();
    truncate table edw_prod.redshift_sync."Price Increase Alias Mapping";
    INSERT INTO edw_prod.redshift_sync."Price Increase Alias Mapping" ( region, customer, pn, old_pn) select * from edw_prod.edw_prod_fin_dev."Price Increase Alias Mapping";
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'Price Increase Alias Mapping', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);

    -- ar_invoice_baseline_region_country
    v_start_time := clock_timestamp();
    truncate table redshift_sync.ar_invoice_baseline_region_country;
    INSERT INTO edw_prod.redshift_sync.ar_invoice_baseline_region_country ( customername, bill_to_region, ship_to_country, item_number, transactional_curr_code, max_unit_amount, max_unit_acctd_amount, asp_usd, asp_lc, extended_price_usd, quantity_invoiced, avg_order_exchange_rate, price_type) select * from edw_prod_fin_dev.ar_invoice_baseline_region_country;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'ar_invoice_baseline_region_country', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);

    -- AR_INVOICE_BASELINE_NonTLA_Region
    v_start_time := clock_timestamp();
    truncate table redshift_sync.AR_INVOICE_BASELINE_NonTLA_Region;
    insert into redshift_sync.AR_INVOICE_BASELINE_NonTLA_Region (customername, ship_to_region, ship_to_country, item_number, transactional_curr_code, max_unit_amount, max_unit_acctd_amount, asp_usd, asp_lc, extended_price_usd, quantity_invoiced, avg_order_exchange_rate, price_type, avg_2024_rate) select * from edw_prod_fin_dev.AR_INVOICE_BASELINE_NonTLA_Region;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'AR_INVOICE_BASELINE_NonTLA_Region', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);
    
    -- AR_INVOICE_BASELINE_NonTLA_LP_Region
    v_start_time := clock_timestamp();
    truncate table redshift_sync.AR_INVOICE_BASELINE_NonTLA_LP_Region; 
    insert into redshift_sync.AR_INVOICE_BASELINE_NonTLA_LP_Region (customername, ship_to_region, ship_to_country, item_number, transactional_curr_code, max_unit_amount, max_unit_acctd_amount, asp_usd, asp_lc, extended_price_usd, quantity_invoiced, avg_order_exchange_rate, price_type, avg_2024_rate, "GL Date", rn) select * from edw_prod_fin_dev.AR_INVOICE_BASELINE_NonTLA_LP_Region;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'AR_INVOICE_BASELINE_NonTLA_LP_Region', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);

    -- AR_INVOICE_BASELINE_TLA_LP_Region
    v_start_time := clock_timestamp();
    truncate table redshift_sync.AR_INVOICE_BASELINE_TLA_LP_Region;
    insert into redshift_sync.AR_INVOICE_BASELINE_TLA_LP_Region (customername, ship_to_region, ship_to_country, item_number, transactional_curr_code, max_unit_amount, max_unit_acctd_amount, asp_usd, asp_lc, extended_price_usd, quantity_invoiced, avg_order_exchange_rate, price_type, avg_2024_rate, "GL Date", rn) select * from edw_prod_fin_dev.AR_INVOICE_BASELINE_TLA_LP_Region;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'AR_INVOICE_BASELINE_TLA_LP_Region', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);

    -- AR_INVOICE_BASELINE_Region_Country
    v_start_time := clock_timestamp();
    truncate table redshift_sync.AR_INVOICE_BASELINE_Region_Country;
    insert into redshift_sync.AR_INVOICE_BASELINE_Region_Country(customername, bill_to_region, ship_to_country, item_number, transactional_curr_code, max_unit_amount, max_unit_acctd_amount, asp_usd, asp_lc, extended_price_usd, quantity_invoiced, avg_order_exchange_rate, price_type) select * from edw_prod_fin_dev.AR_INVOICE_BASELINE_Region_Country;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'AR_INVOICE_BASELINE_Region_Country', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);

    -- AR_INVOICE_BASELINE_Region
    v_start_time := clock_timestamp();
    truncate table redshift_sync.AR_INVOICE_BASELINE_Region;
    insert into redshift_sync.AR_INVOICE_BASELINE_Region (customername, ship_to_region, ship_to_country, item_number, transactional_curr_code, max_unit_amount, max_unit_acctd_amount, asp_usd, asp_lc, extended_price_usd, quantity_invoiced, avg_order_exchange_rate, price_type, avg_2024_rate) select * from edw_prod_fin_dev.AR_INVOICE_BASELINE_Region ;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'AR_INVOICE_BASELINE_Region', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);

    -- Log End
    v_start_time := clock_timestamp();
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'END', 'END', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);

EXCEPTION WHEN OTHERS THEN
    v_start_time := clock_timestamp();
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'UNKNOWN', 'GENERIC', v_start_time, clock_timestamp(), 0, 'FAILED', SQLERRM);
    RAISE;
END;
$procedure$
;