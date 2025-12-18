-- DROP PROCEDURE edw_prod_dbo.sp_refresh_redshift_objects_2_nosales_orders_f();

CREATE OR REPLACE PROCEDURE edw_prod_dbo.sp_refresh_redshift_objects_2_nosales_orders_f()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_start_time TIMESTAMPTZ := clock_timestamp();
    v_rows BIGINT := 0;
BEGIN
    PERFORM set_config('datestyle', 'ISO, MDY', true);

    -- Log Start
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'START', 'START', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);


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

    --9. ar_invoice_baseline_region_country
    v_start_time := clock_timestamp();
    truncate table redshift_sync.ar_invoice_baseline_region_country;
    INSERT INTO edw_prod.redshift_sync.ar_invoice_baseline_region_country ( customername, bill_to_region, ship_to_country, item_number, transactional_curr_code, max_unit_amount, max_unit_acctd_amount, asp_usd, asp_lc, extended_price_usd, quantity_invoiced, avg_order_exchange_rate, price_type) select * from edw_prod_fin_dev.ar_invoice_baseline_region_country;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'ar_invoice_baseline_region_country', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);

    --10 AR_INVOICE_BASELINE_NonTLA_Region
    v_start_time := clock_timestamp();
    truncate table redshift_sync.AR_INVOICE_BASELINE_NonTLA_Region;
    insert into redshift_sync.AR_INVOICE_BASELINE_NonTLA_Region (customername, ship_to_region, ship_to_country, item_number, transactional_curr_code, max_unit_amount, max_unit_acctd_amount, asp_usd, asp_lc, extended_price_usd, quantity_invoiced, avg_order_exchange_rate, price_type, avg_2024_rate) select * from edw_prod_fin_dev.AR_INVOICE_BASELINE_NonTLA_Region;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'AR_INVOICE_BASELINE_NonTLA_Region', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);
    
    --11 AR_INVOICE_BASELINE_NonTLA_LP_Region
    v_start_time := clock_timestamp();
    truncate table redshift_sync.AR_INVOICE_BASELINE_NonTLA_LP_Region; 
    insert into redshift_sync.AR_INVOICE_BASELINE_NonTLA_LP_Region (customername, ship_to_region, ship_to_country, item_number, transactional_curr_code, max_unit_amount, max_unit_acctd_amount, asp_usd, asp_lc, extended_price_usd, quantity_invoiced, avg_order_exchange_rate, price_type, avg_2024_rate, "GL Date", rn) select * from edw_prod_fin_dev.AR_INVOICE_BASELINE_NonTLA_LP_Region;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'AR_INVOICE_BASELINE_NonTLA_LP_Region', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);

    --12 AR_INVOICE_BASELINE_TLA_LP_Region
    v_start_time := clock_timestamp();
    truncate table redshift_sync.AR_INVOICE_BASELINE_TLA_LP_Region;
    insert into redshift_sync.AR_INVOICE_BASELINE_TLA_LP_Region (customername, ship_to_region, ship_to_country, item_number, transactional_curr_code, max_unit_amount, max_unit_acctd_amount, asp_usd, asp_lc, extended_price_usd, quantity_invoiced, avg_order_exchange_rate, price_type, avg_2024_rate, "GL Date", rn) select * from edw_prod_fin_dev.AR_INVOICE_BASELINE_TLA_LP_Region;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'AR_INVOICE_BASELINE_TLA_LP_Region', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);

    --13 AR_INVOICE_BASELINE_Region_Country
    v_start_time := clock_timestamp();
    truncate table redshift_sync.AR_INVOICE_BASELINE_Region_Country;
    insert into redshift_sync.AR_INVOICE_BASELINE_Region_Country(customername, bill_to_region, ship_to_country, item_number, transactional_curr_code, max_unit_amount, max_unit_acctd_amount, asp_usd, asp_lc, extended_price_usd, quantity_invoiced, avg_order_exchange_rate, price_type) select * from edw_prod_fin_dev.AR_INVOICE_BASELINE_Region_Country;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_2', 'FULL', 'AR_INVOICE_BASELINE_Region_Country', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);

    --14 AR_INVOICE_BASELINE_Region
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