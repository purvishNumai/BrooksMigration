CREATE OR REPLACE PROCEDURE edw_prod_dbo.generateocbookingsdata_part4()
 LANGUAGE plpgsql
AS $procedure$

DECLARE
    v_start_time   timestamp;
    v_end_time     timestamp;
    v_runtime_ms   numeric;
BEGIN

    -- Start Time
    v_start_time := clock_timestamp();
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part4 - Started at %', v_start_time;
    -------------------------------------------------------------------------
    -- INSERT missing negative ORG ids (kept duplicate as in MSSQL)
    -------------------------------------------------------------------------
    INSERT INTO edw_prod_dbo.w_organization_d (
        org_code, org_name, org_type, region, src_org_id, integration_id,
        created_date, updated_date, source_system
    )
    SELECT DISTINCT oc.org_code, oc.org_name, oc.org_type, oc.region,
           oc.src_org_id * -1,
           oc.integration_id || '-EBS-OC-REMAP',
           NOW(), NOW(), 'OC'
    FROM edw_prod_dbo.w_organization_d oc,
         edw_prod_dbo.w_sales_orders_f s
    WHERE NOT EXISTS (
              SELECT 1
              FROM edw_prod_dbo.w_organization_d o1
              WHERE s.source_system = o1.source_system
                AND o1.src_org_id = s.ship_from_org_id
          )
      AND s.ship_from_org_id * -1 = oc.src_org_id
      AND s.source_system = 'OC'
      AND oc.source_system = 'EBS';

    -------------------------------------------------------------------------
    -- INSERT missing ITEM-ORG rows (kept duplicate as in MSSQL)
    -------------------------------------------------------------------------
    INSERT INTO edw_prod_dbo.w_item_org_d (
        inventory_item_id, organization_id, organization_code, item_number, description,
        buyer_id, buyer_name, planner_code, planner_name, item_type, inventory_item_status_code,
        inventory_planning_code, sales_account, cost_of_sales_account, expense_account,
        atp_flag, atp_rule_id, build_in_wip_flag, check_shortages_flag, customer_order_enabled_flag,
        customer_order_flag, internal_order_enabled_flag, internal_order_flag, inventory_asset_flag,
        inventory_item_flag, outside_operation_flag, purchasing_enabled_flag, returnable_flag,
        service_item_flag, serviceable_product_flag, shippable_item_flag, so_transactions_flag,
        item_standard_cost, standard_cost_currency, item_standard_cost_usd, lead_time_lot_size,
        fixed_lead_time, full_lead_time, postprocessing_lead_time, preprocessing_lead_time,
        cum_manufacturing_lead_time, cumulative_total_lead_time, variable_lead_time,
        fixed_days_supply, fixed_lot_multiplier, fixed_order_quantity, reservable_type,
        wip_supply_type, planning_make_buy_code, primary_uom_code, primary_unit_of_measure,
        unit_height, unit_length, unit_of_issue, unit_volume, unit_weight, unit_width,
        volume_uom_code, weight_uom_code, hts_code, country_of_origin,
        integration_id, created_date, updated_date, source_system,
        product_family, product_line, product_model, internal_item,
        eccn, sourcing_segment, category, commodity_class, copy_segment, atp_rule_name,
        material_cost, material_cost_usd, material_overhead_cost, material_overhead_cost_usd,
        resource_cost, resource_cost_usd, overhead_cost, overhead_cost_usd,
        outside_processing_cost, outside_processing_cost_usd, default_shipping_org,
        item_creation_date
    )
    SELECT DISTINCT s.item_id, s.ship_from_org_id, inv.organization_code, inv.item_number, inv.description,
           inv.buyer_id, inv.buyer_name, inv.planner_code, inv.planner_name, inv.item_type, inv.inventory_item_status_code,
           inv.inventory_planning_code, inv.sales_account, inv.cost_of_sales_account, inv.expense_account,
           inv.atp_flag, inv.atp_rule_id, inv.build_in_wip_flag, inv.check_shortages_flag, inv.customer_order_enabled_flag,
           inv.customer_order_flag, inv.internal_order_enabled_flag, inv.internal_order_flag, inv.inventory_asset_flag,
           inv.inventory_item_flag, inv.outside_operation_flag, inv.purchasing_enabled_flag, inv.returnable_flag,
           inv.service_item_flag, inv.serviceable_product_flag, inv.shippable_item_flag, inv.so_transactions_flag,
           inv.item_standard_cost, inv.standard_cost_currency, inv.item_standard_cost_usd, inv.lead_time_lot_size,
           inv.fixed_lead_time, inv.full_lead_time, inv.postprocessing_lead_time, inv.preprocessing_lead_time,
           inv.cum_manufacturing_lead_time, inv.cumulative_total_lead_time, inv.variable_lead_time,
           inv.fixed_days_supply, inv.fixed_lot_multiplier, inv.fixed_order_quantity, inv.reservable_type,
           inv.wip_supply_type, inv.planning_make_buy_code, inv.primary_uom_code, inv.primary_unit_of_measure,
           inv.unit_height, inv.unit_length, inv.unit_of_issue, inv.unit_volume, inv.unit_weight, inv.unit_width,
           inv.volume_uom_code, inv.weight_uom_code, inv.hts_code, inv.country_of_origin,
           inv.integration_id || '-EBS-OC-REMAP', NOW(), NOW(), 'OC',
           inv.product_family, inv.product_line, inv.product_model, inv.internal_item,
           inv.eccn, inv.sourcing_segment, inv.category, inv.commodity_class, inv.copy_segment, inv.atp_rule_name,
           inv.material_cost, inv.material_cost_usd, inv.material_overhead_cost, inv.material_overhead_cost_usd,
           inv.resource_cost, inv.resource_cost_usd, inv.overhead_cost, inv.overhead_cost_usd,
           inv.outside_processing_cost, inv.outside_processing_cost_usd, inv.default_shipping_org * -1,
           inv.item_creation_date
    FROM edw_prod_dbo.w_sales_orders_f s,
         edw_prod_dbo.w_item_org_d inv
    WHERE NOT EXISTS (
              SELECT 1
              FROM edw_prod_dbo.w_item_org_d i
              WHERE s.source_system = i.source_system
                AND i.inventory_item_id = s.item_id
                AND i.organization_id = s.ship_from_org_id
          )
      AND s.item_id * -1 = inv.inventory_item_id
      AND s.ship_from_org_id * -1 = inv.organization_id
      AND s.source_system = 'OC'
      AND inv.source_system = 'EBS';

    -------------------------------------------------------------------------
    -- fix blank customer names on sites
    -------------------------------------------------------------------------
    UPDATE edw_prod_dbo.w_customer_site_d s
    SET customer_name = d.customer_name
    FROM edw_prod_dbo.w_customer_d d
    WHERE s.cust_account_id = d.cust_account_id
      AND s.source_system = d.source_system
      AND s.source_system = 'OC'
      AND s.customer_name IS NULL
      AND d.customer_name IS NOT NULL;

    -------------------------------------------------------------------------
    -- booked_date sync from doo_headers_all (type-safing casts)
    -------------------------------------------------------------------------
    UPDATE edw_prod_dbo.w_sales_orders_d d
    SET booked_date = h.headersubmitteddate::DATE
    FROM oc_prod_dbo.doo_headers_all h
    WHERE d.header_id = h.headerid::NUMERIC
      AND COALESCE(d.booked_date::date,'2000-01-01'::date)::date <> COALESCE(h.headersubmitteddate::date,'2000-01-01'::date)::date
      AND d.source_system = 'OC';

    -- Calc and Log Duration
    -- End time
    v_end_time := clock_timestamp();
    -- Duration in milliseconds
    v_runtime_ms := EXTRACT(EPOCH FROM (v_end_time - v_start_time)) * 1000;
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part4 - Completed. Start: %, End: %, Duration: % ms',v_start_time,v_end_time,v_runtime_ms;
          
END;
$procedure$
;