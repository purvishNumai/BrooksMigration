CREATE OR REPLACE PROCEDURE edw_prod_dbo.generateocbookingsdata_part5()
 LANGUAGE plpgsql
AS $procedure$

DECLARE
    v_start_time   timestamp;
    v_end_time     timestamp;
    v_runtime_ms   numeric;
BEGIN

    -- Start Time
    v_start_time := clock_timestamp();
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part5 - Started at %', v_start_time;
    
    -------------------------------------------------------------------------
    -- converted_list -> update OC booked_date from EBS min(booked_date)
    -------------------------------------------------------------------------
    WITH converted_list AS (
        SELECT MIN(booked_date) AS booked_date, order_number,
               COALESCE(d.customer_po,'~') AS customer_po
        FROM edw_prod_dbo.w_sales_orders_d d
        WHERE d.source_system = 'EBS'
          AND d.booked_date IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM edw_prod_dbo.w_sales_orders_d d1
              WHERE d1.source_system = 'OC'
                AND d1.order_source_reference = d.order_number
                AND COALESCE(d1.customer_po,'~') = COALESCE(d.customer_po,'~')
                AND d1.order_status NOT IN ('DOO_REFERENCE','DOO_DRAFT')
          )
        GROUP BY order_number, COALESCE(d.customer_po,'~')
    )
    UPDATE edw_prod_dbo.w_sales_orders_d s
    SET booked_date = c.booked_date
    FROM converted_list c
    WHERE s.order_source_reference = c.order_number
      AND COALESCE(s.customer_po,'~') = c.customer_po
      AND s.source_system = 'OC'
      AND s.order_status NOT IN ('DOO_REFERENCE','DOO_DRAFT');

     -------------------------------------------------------------------------
    -- repeat inserts (MSSQL had duplicates) - keep same behavior
    -------------------------------------------------------------------------
    INSERT INTO edw_prod_dbo.w_organization_d (
        org_code, org_name, org_type, region, src_org_id, integration_id,
        created_date, updated_date, source_system
    )
    SELECT DISTINCT
        oc.org_code,
        oc.org_name,
        oc.org_type,
        oc.region,
        oc.src_org_id * -1,
        oc.integration_id || '-EBS-OC-REMAP',
        NOW(),
        NOW(),
        'OC'
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


      INSERT INTO edw_prod_dbo.w_item_org_d (
        INVENTORY_ITEM_ID, ORGANIZATION_ID, ORGANIZATION_CODE, ITEM_NUMBER,
        DESCRIPTION, BUYER_ID, BUYER_NAME, PLANNER_CODE, PLANNER_NAME, ITEM_TYPE,
        INVENTORY_ITEM_STATUS_CODE, INVENTORY_PLANNING_CODE, SALES_ACCOUNT,
        COST_OF_SALES_ACCOUNT, EXPENSE_ACCOUNT, ATP_FLAG, ATP_RULE_ID,
        BUILD_IN_WIP_FLAG, CHECK_SHORTAGES_FLAG, CUSTOMER_ORDER_ENABLED_FLAG,
        CUSTOMER_ORDER_FLAG, INTERNAL_ORDER_ENABLED_FLAG, INTERNAL_ORDER_FLAG,
        INVENTORY_ASSET_FLAG, INVENTORY_ITEM_FLAG, OUTSIDE_OPERATION_FLAG,
        PURCHASING_ENABLED_FLAG, RETURNABLE_FLAG, SERVICE_ITEM_FLAG,
        SERVICEABLE_PRODUCT_FLAG, SHIPPABLE_ITEM_FLAG, SO_TRANSACTIONS_FLAG,
        ITEM_STANDARD_COST, STANDARD_COST_CURRENCY, ITEM_STANDARD_COST_USD,
        LEAD_TIME_LOT_SIZE, FIXED_LEAD_TIME, FULL_LEAD_TIME,
        POSTPROCESSING_LEAD_TIME, PREPROCESSING_LEAD_TIME,
        CUM_MANUFACTURING_LEAD_TIME, CUMULATIVE_TOTAL_LEAD_TIME,
        VARIABLE_LEAD_TIME, FIXED_DAYS_SUPPLY, FIXED_LOT_MULTIPLIER,
        FIXED_ORDER_QUANTITY, RESERVABLE_TYPE, WIP_SUPPLY_TYPE,
        PLANNING_MAKE_BUY_CODE, PRIMARY_UOM_CODE, PRIMARY_UNIT_OF_MEASURE,
        UNIT_HEIGHT, UNIT_LENGTH, UNIT_OF_ISSUE, UNIT_VOLUME, UNIT_WEIGHT,
        UNIT_WIDTH, VOLUME_UOM_CODE, WEIGHT_UOM_CODE, HTS_CODE,
        COUNTRY_OF_ORIGIN, INTEGRATION_ID, CREATED_DATE, UPDATED_DATE,
        SOURCE_SYSTEM, PRODUCT_FAMILY, PRODUCT_LINE, PRODUCT_MODEL,
        INTERNAL_ITEM, ECCN, SOURCING_SEGMENT, CATEGORY, COMMODITY_CLASS,
        COPY_SEGMENT, ATP_RULE_NAME, MATERIAL_COST, MATERIAL_COST_USD,
        MATERIAL_OVERHEAD_COST, MATERIAL_OVERHEAD_COST_USD, RESOURCE_COST,
        RESOURCE_COST_USD, OVERHEAD_COST, OVERHEAD_COST_USD,
        OUTSIDE_PROCESSING_COST, OUTSIDE_PROCESSING_COST_USD,
        DEFAULT_SHIPPING_ORG, ITEM_CREATION_DATE
    )
    SELECT DISTINCT
        s.ITEM_ID,
        s.SHIP_FROM_ORG_ID,
        inv.organization_code,
        inv.item_number,
        inv.description,
        inv.buyer_id,
        inv.buyer_name,
        inv.planner_code,
        inv.planner_name,
        inv.item_type,
        inv.inventory_item_status_code,
        inv.inventory_planning_code,
        inv.sales_account,
        inv.cost_of_sales_account,
        inv.expense_account,
        inv.atp_flag,
        inv.atp_rule_id,
        inv.build_in_wip_flag,
        inv.check_shortages_flag,
        inv.customer_order_enabled_flag,
        inv.customer_order_flag,
        inv.internal_order_enabled_flag,
        inv.internal_order_flag,
        inv.inventory_asset_flag,
        inv.inventory_item_flag,
        inv.outside_operation_flag,
        inv.purchasing_enabled_flag,
        inv.returnable_flag,
        inv.service_item_flag,
        inv.serviceable_product_flag,
        inv.shippable_item_flag,
        inv.so_transactions_flag,
        inv.item_standard_cost,
        inv.standard_cost_currency,
        inv.item_standard_cost_usd,
        inv.lead_time_lot_size,
        inv.fixed_lead_time,
        inv.full_lead_time,
        inv.postprocessing_lead_time,
        inv.preprocessing_lead_time,
        inv.cum_manufacturing_lead_time,
        inv.cumulative_total_lead_time,
        inv.variable_lead_time,
        inv.fixed_days_supply,
        inv.fixed_lot_multiplier,
        inv.fixed_order_quantity,
        inv.reservable_type,
        inv.wip_supply_type,
        inv.planning_make_buy_code,
        inv.primary_uom_code,
        inv.primary_unit_of_measure,
        inv.unit_height,
        inv.unit_length,
        inv.unit_of_issue,
        inv.unit_volume,
        inv.unit_weight,
        inv.unit_width,
        inv.volume_uom_code,
        inv.weight_uom_code,
        inv.hts_code,
        inv.country_of_origin,
        inv.integration_id || '-EBS-OC-REMAP',
        NOW(),
        NOW(),
        'OC',
        inv.product_family,
        inv.product_line,
        inv.product_model,
        inv.internal_item,
        inv.eccn,
        inv.sourcing_segment,
        inv.category,
        inv.commodity_class,
        inv.copy_segment,
        inv.atp_rule_name,
        inv.material_cost,
        inv.material_cost_usd,
        inv.material_overhead_cost,
        inv.material_overhead_cost_usd,
        inv.resource_cost,
        inv.resource_cost_usd,
        inv.overhead_cost,
        inv.overhead_cost_usd,
        inv.outside_processing_cost,
        inv.outside_processing_cost_usd,
        inv.default_shipping_org * -1,
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


    -- Calc and Log Duration
    -- End time
    v_end_time := clock_timestamp();
    -- Duration in milliseconds
    v_runtime_ms := EXTRACT(EPOCH FROM (v_end_time - v_start_time)) * 1000;
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part5 - Completed. Start: %, End: %, Duration: % ms',v_start_time,v_end_time,v_runtime_ms;
    
END;
$procedure$
;