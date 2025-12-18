
CREATE OR REPLACE PROCEDURE edw_prod_dbo.sp_refresh_redshift_objects_5_backup2025dec17()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_start_time TIMESTAMPTZ := clock_timestamp();
    v_rows BIGINT := 0;
BEGIN
    PERFORM set_config('datestyle', 'ISO, MDY', true);
    
    -- Log Start
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_5', 'START', 'START', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    ----------------------------------------------------------------
    -- INCREMENTAL LOAD SECTION
    ----------------------------------------------------------------

    -- 1. w_product_d (Incremental)
    v_start_time := clock_timestamp();
    MERGE INTO redshift_sync.w_product_d AS tgt
    USING edw_prod_dbo.w_product_d AS src
    ON tgt.integration_id = src.integration_id
    WHEN MATCHED THEN UPDATE SET
        inventory_item_id = src.inventory_item_id, organization_id = src.organization_id,
        item_number = src.item_number, item_description = src.item_description,
        organization_code = src.organization_code, organization_name = src.organization_name,
        default_ship_org = src.default_ship_org, internal_item = src.internal_item,
        product_family = src.product_family, product_line = src.product_line,
        product_model = src.product_model, commodity_class = src.commodity_class,
        category = src.category, sourcing_segment = src.sourcing_segment,
        country_of_origin = src.country_of_origin, eccn = src.eccn,
        copy_segment = src.copy_segment, created_date = src.created_date,
        updated_date = src.updated_date, source_system = src.source_system,
        where_used = src.where_used, category_name = src.category_name
    WHEN NOT MATCHED THEN INSERT (
        inventory_item_id, organization_id, item_number, item_description, organization_code,
        organization_name, default_ship_org, internal_item, product_family, product_line,
        product_model, commodity_class, category, sourcing_segment, country_of_origin,
        eccn, copy_segment, integration_id, created_date, updated_date, source_system,
        where_used, category_name
    ) VALUES (
        src.inventory_item_id, src.organization_id, src.item_number, src.item_description,
        src.organization_code, src.organization_name, src.default_ship_org, src.internal_item,
        src.product_family, src.product_line, src.product_model, src.commodity_class,
        src.category, src.sourcing_segment, src.country_of_origin, src.eccn, src.copy_segment,
        src.integration_id, src.created_date, src.updated_date, src.source_system,
        src.where_used, src.category_name
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_5', 'INCREMENTAL', 'w_product_d', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    ----------------------------------------------------------------
    -- FULL LOAD SECTION
    ----------------------------------------------------------------

    -- 2. dm_onhand_inv_v (Heavy)
    v_start_time := clock_timestamp();
    TRUNCATE TABLE  redshift_sync.dm_onhand_inv_v;
    INSERT INTO redshift_sync.dm_onhand_inv_v
    ( source_system, snapshot_date, on_hand_qty, inventory_item_id, organization_id, organization_code, item_number, description, buyer_id, buyer_name, planner_code, planner_name, item_type, inventory_item_status_code, inventory_planning_code, sales_account, cost_of_sales_account, expense_account, atp_flag, atp_rule_id, build_in_wip_flag, check_shortages_flag, customer_order_enabled_flag, customer_order_flag, internal_order_enabled_flag, internal_order_flag, inventory_asset_flag, inventory_item_flag, outside_operation_flag, purchasing_enabled_flag, returnable_flag, service_item_flag, serviceable_product_flag, shippable_item_flag, so_transactions_flag, item_standard_cost, standard_cost_currency, item_standard_cost_usd, lead_time_lot_size, fixed_lead_time, full_lead_time, postprocessing_lead_time, preprocessing_lead_time, cum_manufacturing_lead_time, cumulative_total_lead_time, variable_lead_time, fixed_days_supply, fixed_lot_multiplier, fixed_order_quantity, reservable_type, wip_supply_type, planning_make_buy_code, primary_uom_code, primary_unit_of_measure, unit_height, unit_length, unit_of_issue, unit_volume, unit_weight, unit_width, volume_uom_code, weight_uom_code, hts_code, country_of_origin, product_family, product_line, product_model, internal_item, eccn, sourcing_segment, category, commodity_class, copy_segment, org_code, org_name, org_type, region, src_org_id, subinventory_name, subinventory_description, atp_code, availability_type, inv_reservable_type, asset_inventory, disable_date, r_part_conversion, inventory_class, auto_replenish_nonstock, min_max_multi_bom, include_eo, customer_req_po, inventory_type, region_w_subinventory_d, include_mrb, logistics_owner)    
    Select * from edw_prod_dbo.dm_onhand_inv_v;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_5', 'FULL', 'dm_onhand_inv_v', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 3. inv_daily_shipping_rev_v1 (Heavy)
    v_start_time := clock_timestamp();
    truncate Table redshift_sync.inv_daily_shipping_rev_v1;
    INSERT INTO redshift_sync.inv_daily_shipping_rev_v1 ( confirmDate, orderNumber, orderType, org, wh, attention, customerPo, line, deliveryDetailId, delivery, subInventory, quantityShipped, orderCurrency, interimOrderCurrency, reportCurrency, unitPrice, interimUnitPrice, functionalExchangeRate, functionalExtPrice, internalPartNumber, orderedItemNumber, itemDescription, productFamily, productLine, productModel, commodityClass, category, sourcingSegment, plannerCode, status, shipMethod, freightTerms, shippingTerms, freightCosts, carrier, trackingNumber, additionalTracking, dateOrdered, dateRequested, datePlanned, dateScheduled, datePromised, customerNumber, customerName, siteNumber, siteName, location, city, state, country, itemCreationDate, serialNumbers, soldToFirstName, soldToLastName, soldToEmail, soldToPhone, shipToFirstName, shipToLastName, shipToPhone, billToFirstName, billToLastName, billToPhone, actualShipmentDate, itemProductClass, shipmentPriority, makeBuy, atpRule, leadTimeDays, leadTimeWeeks, planningType, serviceActivityCode, copyExact, itemStatus, pickSubInventory, otdCode, otdComments, plannerNotes, otdPartNumber, otdSupplier, groupName, grossWeight, weightUom, htsCode, countryOfOrigin, pickedDate, extPriceUsd, extPricePerAvgUsd, icPoNumber, icPoLine, ebsSoNumber, ebsSoLine, sourceSystem, orderLineStatus )
    select * from edw_prod_dbo.inv_daily_shipping_rev;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_5', 'FULL', 'inv_daily_shipping_rev_v1', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 4. item_org_attributes_v
    v_start_time := clock_timestamp();
    truncate table  redshift_sync.item_org_attributes_v;
    insert  into  redshift_sync.item_org_attributes_v
    (source_system, item_number, organization_code, description, inventory_item_id, organization_id, buyer_id, buyer_name, planner_code, planner_name, item_type, inventory_item_status_code, inventory_planning_code, sales_account, cost_of_sales_account, expense_account, atp_flag, atp_rule_id, build_in_wip_flag, check_shortages_flag, customer_order_enabled_flag, customer_order_flag, internal_order_enabled_flag, internal_order_flag, inventory_asset_flag, inventory_item_flag, outside_operation_flag, purchasing_enabled_flag, returnable_flag, service_item_flag, serviceable_product_flag, shippable_item_flag, so_transactions_flag, item_standard_cost, standard_cost_currency, item_standard_cost_usd, lead_time_lot_size, fixed_lead_time, full_lead_time, postprocessing_lead_time, preprocessing_lead_time, cum_manufacturing_lead_time, cumulative_total_lead_time, variable_lead_time, fixed_days_supply, fixed_lot_multiplier, fixed_order_quantity, reservable_type, wip_supply_type, planning_make_buy_code, primary_uom_code, primary_unit_of_measure, unit_height, unit_length, unit_of_issue, unit_volume, unit_weight, unit_width, volume_uom_code, weight_uom_code, hts_code, country_of_origin, product_family, product_line, product_model, eccn, sourcing_segment, category, commodity_class, copy_segment, atp_rule_name, material_cost, material_cost_usd, material_overhead_cost, material_overhead_cost_usd, resource_cost, resource_cost_usd, overhead_cost, overhead_cost_usd, outside_processing_cost, outside_processing_cost_usd, item_creation_date)
    select * from edw_prod_dbo.item_org_attributes_v;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_5', 'FULL', 'item_org_attributes_v', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);  

EXCEPTION WHEN OTHERS THEN
    v_start_time := clock_timestamp();
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_5', 'UNKNOWN', 'GENERIC', v_start_time, clock_timestamp(), 0, 'FAILED', SQLERRM);
    RAISE;
END;
$procedure$
;