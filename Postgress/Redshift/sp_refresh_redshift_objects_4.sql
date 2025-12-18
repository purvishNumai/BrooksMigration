-- DROP PROCEDURE edw_prod_dbo.sp_refresh_redshift_objects_4();

CREATE OR REPLACE PROCEDURE edw_prod_dbo.sp_refresh_redshift_objects_4()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_start_time TIMESTAMPTZ := clock_timestamp();
    v_rows BIGINT := 0;
BEGIN
    PERFORM set_config('datestyle', 'ISO, MDY', true);

    -- Log Start
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_4', 'START', 'START', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 1. Incremental Loads
    -- w_sales_orders_f
    v_start_time := clock_timestamp();
    MERGE INTO redshift_sync.w_sales_orders_f AS tgt
    USING edw_prod_dbo.w_sales_orders_f AS src
    ON tgt.integration_id = src.integration_id
    WHEN MATCHED THEN UPDATE SET
        line_id = src.line_id, order_id = src.order_id, org_id = src.org_id,
        invoice_to_org_id = src.invoice_to_org_id, ship_to_org_id = src.ship_to_org_id,
        sold_to_org_id = src.sold_to_org_id, ship_from_org_id = src.ship_from_org_id,
        item_id = src.item_id, salesrep_id = src.salesrep_id, order_date = src.order_date,
        schedule_date = src.schedule_date, request_date = src.request_date,
        product_code = src.product_code, product_code_node = src.product_code_node,
        qty_ordered = src.qty_ordered, qty_cancelled = src.qty_cancelled,
        qty_shipped = src.qty_shipped, qty_fulfilled = src.qty_fulfilled,
        qty_invoiced = src.qty_invoiced, unit_price = src.unit_price,
        unit_cost = src.unit_cost, exch_rate_usd = src.exch_rate_usd,
        created_date = src.created_date, updated_date = src.updated_date,
        source_system = src.source_system, payment_term_id = src.payment_term_id,
        inv_quantity_invoiced = src.inv_quantity_invoiced, inv_quantity_credited = src.inv_quantity_credited,
        inv_acctd_amount = src.inv_acctd_amount, unit_list_price = src.unit_list_price,
        legacy_order_qty = src.legacy_order_qty, converted_qty = src.converted_qty,
        inv_acctd_amount_lc = src.inv_acctd_amount_lc, virtual_organization_id = src.virtual_organization_id,
        virtual_item_id = src.virtual_item_id, order_line_id = src.order_line_id,
        split_from_line_id = src.split_from_line_id, unit_price_usd = src.unit_price_usd
    WHEN NOT MATCHED THEN INSERT (
        line_id, order_id, org_id, invoice_to_org_id, ship_to_org_id, sold_to_org_id,
        ship_from_org_id, item_id, salesrep_id, order_date, schedule_date, request_date,
        product_code, product_code_node, qty_ordered, qty_cancelled, qty_shipped,
        qty_fulfilled, qty_invoiced, unit_price, unit_cost, exch_rate_usd, integration_id,
        created_date, updated_date, source_system, payment_term_id, inv_quantity_invoiced,
        inv_quantity_credited, inv_acctd_amount, unit_list_price, legacy_order_qty,
        converted_qty, inv_acctd_amount_lc, virtual_organization_id, virtual_item_id,
        order_line_id, split_from_line_id, unit_price_usd
    ) VALUES (
        src.line_id, src.order_id, src.org_id, src.invoice_to_org_id, src.ship_to_org_id,
        src.sold_to_org_id, src.ship_from_org_id, src.item_id, src.salesrep_id, src.order_date,
        src.schedule_date, src.request_date, src.product_code, src.product_code_node,
        src.qty_ordered, src.qty_cancelled, src.qty_shipped, src.qty_fulfilled, src.qty_invoiced,
        src.unit_price, src.unit_cost, src.exch_rate_usd, src.integration_id, src.created_date,
        src.updated_date, src.source_system, src.payment_term_id, src.inv_quantity_invoiced,
        src.inv_quantity_credited, src.inv_acctd_amount, src.unit_list_price, src.legacy_order_qty,
        src.converted_qty, src.inv_acctd_amount_lc, src.virtual_organization_id, src.virtual_item_id,
        src.order_line_id, src.split_from_line_id, src.unit_price_usd
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_4', 'INCREMENTAL', 'w_sales_orders_f', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);  

    -- -- transactions
    -- v_start_time := clock_timestamp();
    -- MERGE INTO edw_prod.redshift_sync.transactions t
    -- USING edw_prod.stage_pgm.transactions s
    -- ON (t.line_id = s.line_id AND t.transaction_sort_order = s.transaction_sort_order)
    -- WHEN MATCHED THEN UPDATE SET
    --     inventory_item_id = s.inventory_item_id,
    --     customer_id = s.customer_id,
    --     ou = s.ou,
    --     hdr_org_id = s.hdr_org_id,
    --     sales_rep = s.sales_rep,
    --     lin_sales_rep = s.lin_sales_rep,
    --     lin_sales_region = s.lin_sales_region,
    --     bill_to_country = s.bill_to_country,
    --     order_number = s.order_number,
    --     line_number = s.line_number,
    --     shipment_number = s.shipment_number,
    --     order_line_shipment = s.order_line_shipment,
    --     transaction_type = s.transaction_type,
    --     order_type_name = s.order_type_name,
    --     lin_order_type_name = s.lin_order_type_name,
    --     booked_date = s.booked_date,
    --     booked_week = s.booked_week,
    --     time_period = s.time_period,
    --     time_period_week = s.time_period_week,
    --     schedule_ship_date = s.schedule_ship_date,
    --     new_product_class = s.new_product_class,
    --     product_class_description = s.product_class_description,
    --     quantity_ordered = s.quantity_ordered,
    --     quantity_change = s.quantity_change,
    --     running_quantity = s.running_quantity,
    --     usd_ext_prc_avg_rate = s.usd_ext_prc_avg_rate,
    --     running_usd_ext_prc_avg_rate = s.running_usd_ext_prc_avg_rate,
    --     in_month_change_count = s.in_month_change_count,
    --     finance_rule_fy17 = s.finance_rule_fy17,
    --     bssg_product_name = s.bssg_product_name,
    --     run_date = s.run_date
    -- WHEN NOT MATCHED THEN INSERT (
    --     inventory_item_id, customer_id, line_id, ou, hdr_org_id, sales_rep, lin_sales_rep, lin_sales_region,
    --     bill_to_country, order_number, line_number, shipment_number, order_line_shipment,
    --     transaction_type, transaction_sort_order, order_type_name, lin_order_type_name,
    --     booked_date, booked_week, time_period, time_period_week, schedule_ship_date,
    --     new_product_class, product_class_description, quantity_ordered, quantity_change,
    --     running_quantity, usd_ext_prc_avg_rate, running_usd_ext_prc_avg_rate,
    --     in_month_change_count, finance_rule_fy17, bssg_product_name, run_date
    -- ) VALUES (
    --     s.inventory_item_id, s.customer_id, s.line_id, s.ou, s.hdr_org_id, s.sales_rep, s.lin_sales_rep, s.lin_sales_region,
    --     s.bill_to_country, s.order_number, s.line_number, s.shipment_number, s.order_line_shipment,
    --     s.transaction_type, s.transaction_sort_order, s.order_type_name, s.lin_order_type_name,
    --     s.booked_date, s.booked_week, s.time_period, s.time_period_week, s.schedule_ship_date,
    --     s.new_product_class, s.product_class_description, s.quantity_ordered, s.quantity_change,
    --     s.running_quantity, s.usd_ext_prc_avg_rate, s.running_usd_ext_prc_avg_rate,
    --     s.in_month_change_count, s.finance_rule_fy17, s.bssg_product_name, s.run_date
    -- );
    -- GET DIAGNOSTICS v_rows = ROW_COUNT;
    -- CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_4', 'INCREMENTAL', 'transactions', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- -- pc_transactions
    -- v_start_time := clock_timestamp();
    -- MERGE INTO edw_prod.redshift_sync.pc_transactions t
    -- USING edw_prod.stage_pgm.pc_transactions s
    -- ON (t.line_id = s.line_id AND t.transaction_sort_order = s.transaction_sort_order)
    -- WHEN MATCHED THEN UPDATE SET
    --     inventory_item_id = s.inventory_item_id,
    --     customer_id = s.customer_id,
    --     ou = s.ou,
    --     hdr_org_id = s.hdr_org_id,
    --     sales_rep = s.sales_rep,
    --     lin_sales_rep = s.lin_sales_rep,
    --     lin_sales_region = s.lin_sales_region,
    --     bill_to_country = s.bill_to_country,
    --     order_number = s.order_number,
    --     line_number = s.line_number,
    --     shipment_number = s.shipment_number,
    --     order_line_shipment = s.order_line_shipment,
    --     transaction_type = s.transaction_type,
    --     order_type_name = s.order_type_name,
    --     lin_order_type_name = s.lin_order_type_name,
    --     booked_date = s.booked_date,
    --     booked_week = s.booked_week,
    --     time_period = s.time_period,
    --     time_period_week = s.time_period_week,
    --     schedule_ship_date = s.schedule_ship_date,
    --     new_product_class = s.new_product_class,
    --     product_class_description = s.product_class_description,
    --     quantity_ordered = s.quantity_ordered,
    --     quantity_change = s.quantity_change,
    --     running_quantity = s.running_quantity,
    --     usd_ext_prc_avg_rate = s.usd_ext_prc_avg_rate,
    --     running_usd_ext_prc_avg_rate = s.running_usd_ext_prc_avg_rate,
    --     in_month_change_count = s.in_month_change_count,
    --     finance_rule_fy17 = s.finance_rule_fy17,
    --     bssg_product_name = s.bssg_product_name,
    --     run_date = s.run_date,
    --     book_time_period = s.book_time_period,
    --     mgmt_customer = s.mgmt_customer,
    --     mgmt_region = s.mgmt_region,
    --     transaction_type_new = s.transaction_type_new,
    --     source_system = s.source_system,
    --     system_spare = s.system_spare,
    --     cust_po_number = s.cust_po_number
    -- WHEN NOT MATCHED THEN INSERT (
    --     inventory_item_id, customer_id, line_id, ou, hdr_org_id, sales_rep, lin_sales_rep, lin_sales_region,
    --     bill_to_country, order_number, line_number, shipment_number, order_line_shipment,
    --     transaction_type, transaction_sort_order, order_type_name, lin_order_type_name,
    --     booked_date, booked_week, time_period, time_period_week, schedule_ship_date,
    --     new_product_class, product_class_description, quantity_ordered, quantity_change,
    --     running_quantity, usd_ext_prc_avg_rate, running_usd_ext_prc_avg_rate,
    --     in_month_change_count, finance_rule_fy17, bssg_product_name, run_date,
    --     book_time_period, mgmt_customer, mgmt_region, transaction_type_new,
    --     source_system, system_spare, cust_po_number
    -- ) VALUES (
    --     s.inventory_item_id, s.customer_id, s.line_id, s.ou, s.hdr_org_id, s.sales_rep, s.lin_sales_rep, s.lin_sales_region,
    --     s.bill_to_country, s.order_number, s.line_number, s.shipment_number, s.order_line_shipment,
    --     s.transaction_type, s.transaction_sort_order, s.order_type_name, s.lin_order_type_name,
    --     s.booked_date, s.booked_week, s.time_period, s.time_period_week, s.schedule_ship_date,
    --     s.new_product_class, s.product_class_description, s.quantity_ordered, s.quantity_change,
    --     s.running_quantity, s.usd_ext_prc_avg_rate, s.running_usd_ext_prc_avg_rate,
    --     s.in_month_change_count, s.finance_rule_fy17, s.bssg_product_name, s.run_date,
    --     s.book_time_period, s.mgmt_customer, s.mgmt_region, s.transaction_type_new,
    --     s.source_system, s.system_spare, s.cust_po_number
    -- );
    -- GET DIAGNOSTICS v_rows = ROW_COUNT;
    -- CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_4', 'INCREMENTAL', 'pc_transactions ', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- -- pc_products
    -- v_start_time := clock_timestamp();
    -- MERGE INTO edw_prod.redshift_sync.pc_products t
    -- USING edw_prod.stage_pgm.pc_products s
    -- ON (t.inventory_item_id = s.inventory_item_id AND t.sales_product_class = s.sales_product_class AND t.source_system = s.source_system)
    -- WHEN MATCHED THEN UPDATE SET
    --     product_group = s.product_group,
    --     product_family = s.product_family,
    --     product_line = s.product_line,
    --     product_model = s.product_model,
    --     item_description = s.item_description,
    --     item_number = s.item_number,
    --     commodity_class = s.commodity_class,
    --     default_shipping_org = s.default_shipping_org
    -- WHEN NOT MATCHED THEN INSERT (
    --     inventory_item_id, product_group, product_family, product_line, product_model,
    --     sales_product_class, item_description, item_number, commodity_class, default_shipping_org, source_system
    -- ) VALUES (
    --     s.inventory_item_id, s.product_group, s.product_family, s.product_line, s.product_model,
    --     s.sales_product_class, s.item_description, s.item_number, s.commodity_class, s.default_shipping_org, s.source_system
    -- );
    -- GET DIAGNOSTICS v_rows = ROW_COUNT;
    -- CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_4', 'INCREMENTAL', 'pc_products ', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- -- pc_customers
    -- v_start_time := clock_timestamp();
    -- MERGE INTO edw_prod.redshift_sync.pc_customers t
    -- USING edw_prod.stage_pgm.pc_customers s
    -- ON (t.site_use_id = s.site_use_id)
    -- WHEN MATCHED THEN UPDATE SET
    --     management_region = s.management_region,
    --     top_level_customer_name = s.top_level_customer_name,
    --     customer_name_region_country = s.customer_name_region_country,
    --     customer_name = s.customer_name,
    --     semi_nonsemi = s.semi_nonsemi,
    --     customer_class_code = s.customer_class_code,
    --     primary_flag = s.primary_flag,
    --     status = s.status,
    --     region = s.region,
    --     sites_region = s.sites_region,
    --     country = s.country,
    --     territory_short_name = s.territory_short_name,
    --     source_system = s.source_system
    -- WHEN NOT MATCHED THEN INSERT (
    --     site_use_id, management_region, top_level_customer_name, customer_name_region_country,
    --     customer_name, semi_nonsemi, customer_class_code, primary_flag, status,
    --     region, sites_region, country, territory_short_name, source_system
    -- ) VALUES (
    --     s.site_use_id, s.management_region, s.top_level_customer_name, s.customer_name_region_country,
    --     s.customer_name, s.semi_nonsemi, s.customer_class_code, s.primary_flag, s.status,
    --     s.region, s.sites_region, s.country, s.territory_short_name, s.source_system
    -- );
    -- GET DIAGNOSTICS v_rows = ROW_COUNT;
    -- CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_4', 'INCREMENTAL', 'pc_customers ', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- -- customers
    -- v_start_time := clock_timestamp();
    -- MERGE INTO edw_prod.redshift_sync.customers t
    -- USING edw_prod.stage_pgm.customers s
    -- ON (t.site_use_id = s.site_use_id)
    -- WHEN MATCHED THEN UPDATE SET
    --     management_region = s.management_region,
    --     top_level_customer_name = s.top_level_customer_name,
    --     customer_name_region_country = s.customer_name_region_country,
    --     customer_name = s.customer_name,
    --     semi_nonsemi = s.semi_nonsemi,
    --     customer_class_code = s.customer_class_code,
    --     primary_flag = s.primary_flag,
    --     status = s.status,
    --     region = s.region,
    --     sites_region = s.sites_region,
    --     country = s.country,
    --     territory_short_name = s.territory_short_name
    -- WHEN NOT MATCHED THEN INSERT (
    --     site_use_id, management_region, top_level_customer_name, customer_name_region_country,
    --     customer_name, semi_nonsemi, customer_class_code, primary_flag, status,
    --     region, sites_region, country, territory_short_name
    -- ) VALUES (
    --     s.site_use_id, s.management_region, s.top_level_customer_name, s.customer_name_region_country,
    --     s.customer_name, s.semi_nonsemi, s.customer_class_code, s.primary_flag, s.status,
    --     s.region, s.sites_region, s.country, s.territory_short_name
    -- );
    -- GET DIAGNOSTICS v_rows = ROW_COUNT;
    -- CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_4', 'INCREMENTAL', 'customers ', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- -- w_organization_d
    -- v_start_time := clock_timestamp();
    -- MERGE INTO redshift_sync.w_organization_d AS tgt
    -- USING edw_prod_dbo.w_organization_d AS src
    -- ON tgt.integration_id = src.integration_id
    -- WHEN MATCHED THEN UPDATE SET
    --     org_code = src.org_code, org_name = src.org_name, org_type = src.org_type,
    --     region = src.region, src_org_id = src.src_org_id, created_date = src.created_date,
    --     updated_date = src.updated_date, source_system = src.source_system,
    --     currency_code = src.currency_code
    -- WHEN NOT MATCHED THEN INSERT (
    --     org_code, org_name, org_type, region, src_org_id, integration_id,
    --     created_date, updated_date, source_system, currency_code
    -- ) VALUES (
    --     src.org_code, src.org_name, src.org_type, src.region, src.src_org_id,
    --     src.integration_id, src.created_date, src.updated_date, src.source_system,
    --     src.currency_code
    -- );
    -- GET DIAGNOSTICS v_rows = ROW_COUNT;
    -- CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_4', 'INCREMENTAL', 'w_organization_d', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);


    -- 2. Full Loads
    -- FNC_BACKLOG_BILLING_MV
    v_start_time := clock_timestamp();
    Truncate table  redshift_sync.FNC_BACKLOG_BILLING_MV;
    insert into redshift_sync.FNC_BACKLOG_BILLING_MV
    ("Hdr Order Type Name","Order Number","Line Number","Shipment Number","Ordered Quantity","Order Exchange Rate","Transaction Curr Code","Cust PO Number","Request Date","Ordered Item","Scheduled Ship Date","Cancelled Quantity","Shipped Quantity","Unit Selling Price","Actual Ship Date","Line Order Type","Line Order Type Name","Ship From Func Currency","Curr Exchange Rate","Extended Price USD","Order Date","Booked Date","Cost of Sale Account",created_by_name,"Ship From Org Name","Ship From Org Code","Bill To Customer Parent","Bill To Customer Name","Ship To Customer Parent","Ship To Customer Name","Ship From Item Cost LC","Ship From Item Cost USD","Extended Price LC","Sales Rep","Sales Region","Hold Type","Unit Selling PriceLC","Product Class","Sales Account","GL Date","Invoice Curr Code","Quantity Credited","Quantity Invoiced","SOB Description","Invoice Period AVG Rate",category,"Category Name","Commodity Class","Item Description","Item Name","Product Family","Product Line","Product Model","Sourcing Segment","Label (L5)","Product Code & Label","Segment (L1)","Family Segment (L3)","Product/Segment (L2)","Family Group (L4)","Common vs System","Promise Date",ou,"Bill To Region","Ship To Region","Ship To Country","Bill To Class Code","Ship To Class Code","Invoice TRX Number","Ext Ship From Item Cost LC","Ext Ship From Item Cost USD","MGMT Customer","MGMT Region",data_type,source_system,line_id,"Bill To Country",order_status,line_status,fulfill_line_status,order_creation_date,line_creation_date,last_update_date,payment_terms,primary_market_segment,secondary_market_segment,fulfillment_line_number,line_source_number,"Invoice Line Number","Bill To Mgmt Continent","Ship To Mgmt Continent","Total_distribution_cost LC",mfg_frozen_cost_at_per_avg_in_usd,mfg_frozen_cost_at_planned_in_usd,ship_frozen_cost_at_per_avg_in_usd)
    select * from edw_prod_dbo.FNC_BACKLOG_BILLING_MV;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_4', 'FULL', 'FNC_BACKLOG_BILLING_MV', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- FNC_BOOKINGS_NEW_MV
    v_start_time := clock_timestamp();
    Truncate table redshift_sync.FNC_BOOKINGS_NEW_MV;   
    insert into redshift_sync.FNC_BOOKINGS_NEW_MV
    (hdr_order_type_name, order_number, line_number, shipment_number, ordered_quantity, order_exchange_rate, transaction_curr_code, cust_po_number, request_date, ordered_item, scheduled_ship_date, cancelled_quantity, shipped_quantity, unit_selling_price, actual_ship_date, line_order_type, line_order_type_name, ship_from_func_currency, curr_exchange_rate, extended_price_usd, order_date, booked_date, cost_of_sale_account, ship_from_org_name, ship_from_org_code, bill_to_customer_name, ship_to_customer_name, ship_from_item_cost_lc, ship_from_item_cost_usd, extended_price_lc, sales_rep, sales_region, hold_type, unit_selling_pricelc, product_class, sales_account, gl_date, invoice_curr_code, quantity_credited, quantity_invoiced, sob_description, invoice_period_avg_rate, category, category_name, commodity_class, item_description, item_name, product_family, product_line, product_model, sourcing_segment, label_l5, product_code_and_label, segment_l1, family_segment_l3, product_segment_l2, family_group_l4, common_vs_system, promise_date, ou, bill_to_region, ship_to_region, ship_to_country, transaction_type, bill_to_class_code, ship_to_class_code, invoice_trx_number, ext_ship_from_item_cost_lc, ext_ship_from_item_cost_usd, mgmt_customer, mgmt_region, data_type, source_system, snapshot_date, line_id, book_status, order_status, line_status, fulfill_line_status, order_creation_date, line_creation_date, last_update_date, primary_market_segment, secondary_market_segment, fulfillment_line_number, line_source_number)
    select * from  edw_prod_dbo.FNC_BOOKINGS_NEW_MV ;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_4', 'FULL', 'FNC_BOOKINGS_NEW_MV', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- fnc_booking_rpt_vw_mv
    v_start_time := clock_timestamp();
    truncate table redshift_sync.fnc_booking_rpt_vw_mv;
    INSERT INTO redshift_sync.fnc_booking_rpt_vw_mv
    ( "Hdr Order Type Name", "Order Number", "Line Number", "Shipment Number", "Ordered Quantity", "Order Exchange Rate", "Transaction Curr Code", "Cust PO Number", "Request Date", "Ordered Item", "Scheduled Ship Date", "Cancelled Quantity", "Shipped Quantity", "Unit Selling Price", "Actual Ship Date", "Line Order Type", "Line Order Type Name", "Ship From Func Currency", "Curr Exchange Rate", "Extended Price USD", "Order Date", "Booked Date", "Cost of Sale Account", "Ship From Org Name", "Ship From Org Code", "Bill To Customer Parent", "Bill To Customer Name", "Ship To Customer Parent", "Ship To Customer Name", "Ship From Item Cost LC", "Ship From Item Cost USD", "Extended Price LC", "Sales Rep", "Sales Region", "Hold Type", "Unit Selling PriceLC", "Product Class", "Sales Account", "GL Date", "Invoice Curr Code", "Quantity Credited", "Quantity Invoiced", "SOB Description", "Invoice Period AVG Rate", category, "Category Name", "Commodity Class", "Item Description", "Item Name", "Product Family", "Product Line", "Product Model", "Sourcing Segment", "Label (L5)", "Product Code & Label", "Segment (L1)", "Family Segment (L3)", "Product/Segment (L2)", "Family Group (L4)", "Common vs System", "Promise Date", ou, "Bill To Region", "Ship To Region", "Ship To Country", transaction_type, "Bill To Class Code", "Ship To Class Code", "Invoice TRX Number", "Ext Ship From Item Cost LC", "Ext Ship From Item Cost USD", "MGMT Customer", "MGMT Region", data_type, line_id) 
    Select * from stage_bi.fnc_booking_rpt_vw_mv;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_4', 'FULL', 'fnc_booking_rpt_vw_mv', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- -- pc_customer_rollup
    -- v_start_time := clock_timestamp();
    -- truncate table edw_prod.redshift_sync.pc_customer_rollup;
    -- INSERT INTO edw_prod.redshift_sync.pc_customer_rollup (
    --     management_region, top_level_customer_name, customer_name_region_country, semi_nonsemi, customer_class_code
    -- )
    -- SELECT
    --     management_region, top_level_customer_name, customer_name_region_country, semi_nonsemi, customer_class_code
    -- FROM edw_prod.stage_pgm.pc_customer_rollup;
    --     GET DIAGNOSTICS v_rows = ROW_COUNT;
    -- CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_4', 'FULL', 'pc_customer_rollup', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- customer_rollup
    -- v_start_time := clock_timestamp();
    -- truncate table edw_prod.redshift_sync.customer_rollup;
    -- INSERT INTO edw_prod.redshift_sync.customer_rollup (
    --     management_region, top_level_customer_name, customer_name_region_country, semi_nonsemi, customer_class_code
    -- )
    -- SELECT
    --     management_region, top_level_customer_name, customer_name_region_country, semi_nonsemi, customer_class_code
    -- FROM edw_prod.stage_pgm.customer_rollup;
    -- GET DIAGNOSTICS v_rows = ROW_COUNT;
    -- CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_4', 'FULL', 'customer_rollup', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- Log End
    v_start_time := clock_timestamp();
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_4', 'END', 'END', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);

EXCEPTION WHEN OTHERS THEN
    v_start_time := clock_timestamp();
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_4', 'UNKNOWN', 'GENERIC', v_start_time, clock_timestamp(), 0, 'FAILED', SQLERRM);
    RAISE;
END;
$procedure$
;