-- DROP PROCEDURE edw_prod_dbo.sp_refresh_redshift_objects_6();

CREATE OR REPLACE PROCEDURE edw_prod_dbo.sp_refresh_redshift_objects_6_backup2025dec17()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_start_time TIMESTAMPTZ := clock_timestamp();
    v_rows BIGINT := 0;
BEGIN
    PERFORM set_config('datestyle', 'ISO, MDY', true);
    
    -- Log Start
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_6', 'START', 'START', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    
    -- 5. w_inv_bal_targets_f
    v_start_time := clock_timestamp();
    truncate table  redshift_sync.w_inv_bal_targets_f ;
    insert into redshift_sync.w_inv_bal_targets_f 
    (as_of_date, organization_code, subinventory, "locator", subinventory_description, item, description, item_product_class, on_hand_quantity, currency, on_hand_value, unit_frzn_mfg_site_plan_usd, extend_frzn_mfg_site_plan_usd, unit_frzn_mfg_site_pe_usd, extend_frzn_mfg_site_pe_usd, item_status, buyer, planner_code, planning_type, make_buy, product_family, product_line, product_model, planned_rate, min_qty_sub, max_qty_sub, fixed_lot_mult_sub, min_qty_org, max_qty_org, fixed_order_qty_org, fixed_lot_mult_org, safety_stock_qty, subinv_product_class, _1_month_usage, _3_month_usage, _6_month_usage, _12_month_usage, _24_month_usage, _1_month_wo_usage, _3_month_wo_usage, _6_month_wo_usage, _12_month_wo_usage, _24_month_wo_usage, total_cum_lt, so_demand, iso_demand, wo_demand, mo_demand, fcst_demand, intransit_receipt, logistics_owner, inventory_type, inventory_class, kanban_type, sum_kanban_qty, no_of_cards, item_org_creation_date, dso, boru, user_defined_moh, unit_book_value, extended_book_value, item_commodity_segment1, item_commodity_segment2, item_commodity_segment3, subinv_location, "GROUP", effective_from, latest_revision, annual_turns, turns_cat, tsl_max_value, fill_rate, excess_qty_oh, load_date, source_system, v_create_date, v_update_date, v_src_sys)
    select * from edw_prod_dbo.w_inv_bal_targets_f;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_6', 'FULL', 'w_inv_bal_targets_f', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 6. cst_cost_item_summary
    v_start_time := clock_timestamp();
    truncate table redshift_sync.cst_cost_item_summary;
    insert into redshift_sync.cst_cost_item_summary(cost_org, cost_book, cost_scenario, inventory_org, item_number, item_description, cost_status, cost_source_based_on_rollup, cost_effective_start_date, cost_effective_end_date, make_or_buy, inventory_asset, item_unit_cost, material_cost, material_oh_cost, resource_cost, overhead_cost, osp_cost, product_class, product_family, product_line, product_model, sales_account, buyer, planner, item_type, inventory_item_status_code, primary_uom, default_shipping_org, source_org, cycle_count_enabled, customer_orders_enabled, internal_orders_enabled, company_code, wip_flag, user_item_type, functional_currency, conv_rate, item_unit_cost_usd, material_cost_usd, material_oh_cost_usd, resource_cost_usd, overhead_cost_usd, osp_cost_usd, last_total_cost, last_material_cost, last_material_oh_cost, last_resource_cost, last_overhead_cost, last_osp_cost, last_total_cost_usd, last_material_cost_usd, last_material_oh_cost_usd, last_resource_cost_usd, last_overhead_cost_usd, last_osp_cost_usd, last_std_cost_id, last_scenario_number, last_currency_code, original_ebs_dso, external_order_enabled, cost_catalog, source_commodity, load_date)
    select * from  oc_prod_dbo.cst_cost_item_summary;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_6', 'FULL', 'cst_cost_item_summary', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 7. dm_supply_plan_v
    v_start_time := clock_timestamp();
    truncate table redshift_sync.dm_supply_plan_v;  
    insert into redshift_sync.dm_supply_plan_v(snapshot_date, source_system, supply_demand, transaction_id, plan_name, org_code, source_org_code, "action", plan_order_type, planning_time_fence, demand_time_fence, fixed_days_supply, preprocessing_lead_time, fixed_lead_time, cumulative_total_lead_time, subinventory_code, safety_stock, wip_supply_type, planning_type, planner_code, buyer_name, sourcing_rule_name, firm_planned_type, make_buy, old_doc_date, suggested_doc_date, old_due_date, suggested_due_date, request_date, schedule_ship_date, qty_rate, item_number, description, inventory_planning_code, atp_rule_name, atp_lead_time, product_family, product_line, product_model, commodity_class, std_cst, std_cst_usd,
    material_cst, material_cst_usd, resource_cst, resource_cst_usd, overhead_cst, 
    overhead_cst_usd, outside_processing_cst, outside_processing_cst_usd, extended_cst, 
    extended_cst_usd, supplier_name, supplier_site_code, customer_name, customer_site,
    po_number, po_line_number, po_line_location_num, need_by_date, promised_date, so_order_number, so_line_num, so_order_currency, conversion_rate, so_unit_price, so_unit_price_usd)
    select * from edw_prod_dbo.dm_supply_plan_v;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_6', 'FULL', 'dm_supply_plan_v', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 8. inv_material_transaction_usage_v
    v_start_time := clock_timestamp();  
    truncate Table redshift_sync.inv_material_transaction_usage_v;
    INSERT INTO redshift_sync.inv_material_transaction_usage_v
    ( source_system, transaction_id, transaction_date, transaction_source_type_name, transaction_type_name, bu_name, organization_code, item_number, description, product_family, product_line, product_model, commodity_class, sourcing_segment, subinventory_code, item_standard_cost, item_standard_cost_usd, material_cost, material_cost_usd, resource_cost, resource_cost_usd, overhead_cost, overhead_cost_usd, outside_processing_cost, outside_processing_cost_usd, extended_cost, extended_cost_usd, usage_1_month, usage_3_month, usage_6_month, usage_12_month, usage_24_month, buyer_name, inventory_planning_code, make_buy, serial_number, transaction_quantity, transaction_uom, primary_quantity, transfer_organization_code, shipment_number, so_order_number, so_line_number, customer_name, wo_number, wo_type, wo_completion_date, creation_date, created_by)
    Select * from edw_prod_dbo.inv_material_transaction_usage_v;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_6', 'FULL', 'inv_material_transaction_usage_v', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

   
EXCEPTION WHEN OTHERS THEN
    v_start_time := clock_timestamp();
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_6', 'UNKNOWN', 'GENERIC', v_start_time, clock_timestamp(), 0, 'FAILED', SQLERRM);
    RAISE;
END;
$procedure$
;