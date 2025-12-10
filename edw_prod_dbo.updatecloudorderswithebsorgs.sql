-- DROP PROCEDURE edw_prod_dbo.updatecloudorderswithebsorgs();

CREATE OR REPLACE PROCEDURE edw_prod_dbo.updatecloudorderswithebsorgs()
 LANGUAGE plpgsql
AS $procedure$
BEGIN

    CALL edw_prod_dbo.edw_log_start_job('updatecloudorderswithebsorgs');

    WITH items AS (
        SELECT DISTINCT ebs_inv.inventory_item_id, ebs_inv.organization_id
        FROM edw_prod_dbo.w_sales_orders_v oc
        JOIN edw_prod_dbo.w_sales_orders_v ebs ON oc.ORDER_NUMBER = ebs.REF_ORDER_NUMBER
        JOIN edw_prod_dbo.w_item_org_d oc_inv ON oc.item_id = oc_inv.inventory_item_id
        JOIN edw_prod_dbo.w_item_org_d ebs_inv ON ebs.item_id = ebs_inv.inventory_item_id
        WHERE oc.source_system = 'OC'
          AND ebs.source_system = 'EBS'
          AND oc_inv.organization_code IN ('BIM', 'BAI_VIR', 'BAK_VIR')
          AND ebs.ship_from_org_id = ebs_inv.organization_id
          AND ebs_inv.item_number = oc_inv.item_number
          AND ebs_inv.source_system = ebs.source_system
          AND oc_inv.source_system = oc.source_system
          AND oc.line_number = 
               CAST(
            CASE
                WHEN POSITION('.' IN ebs.REF_ORDER_LINE) = 0 THEN
                    CASE
                        WHEN POSITION('-' IN ebs.REF_ORDER_LINE) = 0 THEN ebs.REF_ORDER_LINE
                        ELSE LEFT(ebs.REF_ORDER_LINE, POSITION('-' IN ebs.REF_ORDER_LINE) - 1)
                    END
                ELSE LEFT(ebs.REF_ORDER_LINE, POSITION('.' IN ebs.REF_ORDER_LINE) - 1)
            END
            AS bigint)
          AND COALESCE(CAST(oc.shipment_number AS VARCHAR(20)), '~') = 
              COALESCE(
                  CASE 
                      WHEN POSITION('.' IN ebs.REF_ORDER_LINE) = 0 OR POSITION('.' IN ebs.REF_ORDER_LINE) = LENGTH(ebs.REF_ORDER_LINE) THEN 
                          CASE 
                              WHEN POSITION('-' IN ebs.REF_ORDER_LINE) = 0 OR POSITION('-' IN ebs.REF_ORDER_LINE) = LENGTH(ebs.REF_ORDER_LINE) THEN COALESCE(CAST(oc.shipment_number AS VARCHAR(20)), '~')
                              ELSE RIGHT(ebs.REF_ORDER_LINE, LENGTH(ebs.REF_ORDER_LINE) - POSITION('-' IN ebs.REF_ORDER_LINE)) 
                          END
                      ELSE RIGHT(ebs.REF_ORDER_LINE, LENGTH(ebs.REF_ORDER_LINE) - POSITION('.' IN ebs.REF_ORDER_LINE)) 
                  END,
                  COALESCE(CAST(oc.shipment_number AS VARCHAR(20)), '~')
              )
          AND NOT EXISTS (
              SELECT 1 
              FROM edw_prod_dbo.w_item_org_d e_items
              WHERE e_items.inventory_item_id = ebs_inv.inventory_item_id * -1
                AND e_items.organization_id = ebs_inv.organization_id * -1
                AND e_items.source_system = 'OC'
          )
    )
    INSERT INTO edw_prod_dbo.w_item_org_d (
        INVENTORY_ITEM_ID, ORGANIZATION_ID, ORGANIZATION_CODE, ITEM_NUMBER, DESCRIPTION, BUYER_ID, BUYER_NAME, PLANNER_CODE, PLANNER_NAME, ITEM_TYPE, 
        INVENTORY_ITEM_STATUS_CODE, INVENTORY_PLANNING_CODE, SALES_ACCOUNT, COST_OF_SALES_ACCOUNT, EXPENSE_ACCOUNT, ATP_FLAG, ATP_RULE_ID, 
        BUILD_IN_WIP_FLAG, CHECK_SHORTAGES_FLAG, CUSTOMER_ORDER_ENABLED_FLAG, CUSTOMER_ORDER_FLAG, INTERNAL_ORDER_ENABLED_FLAG, 
        INTERNAL_ORDER_FLAG, INVENTORY_ASSET_FLAG, INVENTORY_ITEM_FLAG, OUTSIDE_OPERATION_FLAG, PURCHASING_ENABLED_FLAG, RETURNABLE_FLAG, 
        SERVICE_ITEM_FLAG, SERVICEABLE_PRODUCT_FLAG, SHIPPABLE_ITEM_FLAG, SO_TRANSACTIONS_FLAG, ITEM_STANDARD_COST, STANDARD_COST_CURRENCY, 
        ITEM_STANDARD_COST_USD, LEAD_TIME_LOT_SIZE, FIXED_LEAD_TIME, FULL_LEAD_TIME, POSTPROCESSING_LEAD_TIME, PREPROCESSING_LEAD_TIME, 
        CUM_MANUFACTURING_LEAD_TIME, CUMULATIVE_TOTAL_LEAD_TIME, VARIABLE_LEAD_TIME, FIXED_DAYS_SUPPLY, FIXED_LOT_MULTIPLIER, 
        FIXED_ORDER_QUANTITY, RESERVABLE_TYPE, WIP_SUPPLY_TYPE, PLANNING_MAKE_BUY_CODE, PRIMARY_UOM_CODE, PRIMARY_UNIT_OF_MEASURE, 
        UNIT_HEIGHT, UNIT_LENGTH, UNIT_OF_ISSUE, UNIT_VOLUME, UNIT_WEIGHT, UNIT_WIDTH, VOLUME_UOM_CODE, WEIGHT_UOM_CODE, HTS_CODE, 
        COUNTRY_OF_ORIGIN, INTEGRATION_ID, CREATED_DATE, UPDATED_DATE, SOURCE_SYSTEM, PRODUCT_FAMILY, PRODUCT_LINE, PRODUCT_MODEL, 
        INTERNAL_ITEM, ECCN, SOURCING_SEGMENT, CATEGORY, COMMODITY_CLASS, COPY_SEGMENT, ATP_RULE_NAME, MATERIAL_COST, MATERIAL_COST_USD, 
        MATERIAL_OVERHEAD_COST, MATERIAL_OVERHEAD_COST_USD, RESOURCE_COST, RESOURCE_COST_USD, OVERHEAD_COST, OVERHEAD_COST_USD, 
        OUTSIDE_PROCESSING_COST, OUTSIDE_PROCESSING_COST_USD, DEFAULT_SHIPPING_ORG, ITEM_CREATION_DATE
    )
    SELECT 
        ebs_item.INVENTORY_ITEM_ID * -1, 
        ebs_item.ORGANIZATION_ID * -1, 
        ORGANIZATION_CODE, ITEM_NUMBER, DESCRIPTION, BUYER_ID, BUYER_NAME, PLANNER_CODE, PLANNER_NAME, ITEM_TYPE, 
        INVENTORY_ITEM_STATUS_CODE, INVENTORY_PLANNING_CODE, SALES_ACCOUNT, COST_OF_SALES_ACCOUNT, EXPENSE_ACCOUNT, 
        ATP_FLAG, ATP_RULE_ID, BUILD_IN_WIP_FLAG, CHECK_SHORTAGES_FLAG, CUSTOMER_ORDER_ENABLED_FLAG, CUSTOMER_ORDER_FLAG, 
        INTERNAL_ORDER_ENABLED_FLAG, INTERNAL_ORDER_FLAG, INVENTORY_ASSET_FLAG, INVENTORY_ITEM_FLAG, OUTSIDE_OPERATION_FLAG, 
        PURCHASING_ENABLED_FLAG, RETURNABLE_FLAG, SERVICE_ITEM_FLAG, SERVICEABLE_PRODUCT_FLAG, SHIPPABLE_ITEM_FLAG, 
        SO_TRANSACTIONS_FLAG, ITEM_STANDARD_COST, STANDARD_COST_CURRENCY, ITEM_STANDARD_COST_USD, LEAD_TIME_LOT_SIZE, 
        FIXED_LEAD_TIME, FULL_LEAD_TIME, POSTPROCESSING_LEAD_TIME, PREPROCESSING_LEAD_TIME, CUM_MANUFACTURING_LEAD_TIME, 
        CUMULATIVE_TOTAL_LEAD_TIME, VARIABLE_LEAD_TIME, FIXED_DAYS_SUPPLY, FIXED_LOT_MULTIPLIER, FIXED_ORDER_QUANTITY, 
        RESERVABLE_TYPE, WIP_SUPPLY_TYPE, PLANNING_MAKE_BUY_CODE, PRIMARY_UOM_CODE, PRIMARY_UNIT_OF_MEASURE, 
        UNIT_HEIGHT, UNIT_LENGTH, UNIT_OF_ISSUE, UNIT_VOLUME, UNIT_WEIGHT, UNIT_WIDTH, VOLUME_UOM_CODE, WEIGHT_UOM_CODE, 
        HTS_CODE, COUNTRY_OF_ORIGIN, INTEGRATION_ID || '-EBS-OC-REMAP', CURRENT_TIMESTAMP AS CREATED_DATE, 
        CURRENT_TIMESTAMP AS UPDATED_DATE, 'OC' AS SOURCE_SYSTEM, PRODUCT_FAMILY, PRODUCT_LINE, PRODUCT_MODEL, 
        INTERNAL_ITEM, ECCN, SOURCING_SEGMENT, CATEGORY, COMMODITY_CLASS, COPY_SEGMENT, ATP_RULE_NAME, MATERIAL_COST, 
        MATERIAL_COST_USD, MATERIAL_OVERHEAD_COST, MATERIAL_OVERHEAD_COST_USD, RESOURCE_COST, RESOURCE_COST_USD, 
        OVERHEAD_COST, OVERHEAD_COST_USD, OUTSIDE_PROCESSING_COST, OUTSIDE_PROCESSING_COST_USD, DEFAULT_SHIPPING_ORG * -1, 
        ITEM_CREATION_DATE	
    FROM edw_prod_dbo.w_item_org_d ebs_item
    JOIN items oc ON ebs_item.inventory_item_id = oc.inventory_item_id			
    AND ebs_item.organization_id = oc.organization_id
    AND ebs_item.source_system = 'EBS';

    WITH Orgs AS (
        SELECT DISTINCT ebs_inv.organization_id
        FROM edw_prod_dbo.w_sales_orders_v oc
        JOIN edw_prod_dbo.w_sales_orders_v ebs ON oc.ORDER_NUMBER = ebs.REF_ORDER_NUMBER
        JOIN edw_prod_dbo.w_item_org_d oc_inv ON oc.item_id = oc_inv.inventory_item_id
        JOIN edw_prod_dbo.w_item_org_d ebs_inv ON ebs.item_id = ebs_inv.inventory_item_id
        WHERE oc.source_system = 'OC'
          AND ebs.source_system = 'EBS'
          AND oc_inv.organization_code IN ('BIM', 'BAI_VIR', 'BAK_VIR')
          AND ebs.ship_from_org_id = ebs_inv.organization_id
          AND ebs_inv.item_number = oc_inv.item_number
          AND ebs_inv.source_system = ebs.source_system
          AND oc_inv.source_system = oc.source_system
          AND oc.line_number = 
               CAST(
            CASE
                WHEN POSITION('.' IN ebs.REF_ORDER_LINE) = 0 THEN
                    CASE
                        WHEN POSITION('-' IN ebs.REF_ORDER_LINE) = 0 THEN ebs.REF_ORDER_LINE
                        ELSE LEFT(ebs.REF_ORDER_LINE, POSITION('-' IN ebs.REF_ORDER_LINE) - 1)
                    END
                ELSE LEFT(ebs.REF_ORDER_LINE, POSITION('.' IN ebs.REF_ORDER_LINE) - 1)
            END
            AS bigint)
          AND COALESCE(CAST(oc.shipment_number AS VARCHAR(20)), '~') = 
              COALESCE(
                  CASE 
                      WHEN POSITION('.' IN ebs.REF_ORDER_LINE) = 0 OR POSITION('.' IN ebs.REF_ORDER_LINE) = LENGTH(ebs.REF_ORDER_LINE) THEN 
                          CASE 
                              WHEN POSITION('-' IN ebs.REF_ORDER_LINE) = 0 OR POSITION('-' IN ebs.REF_ORDER_LINE) = LENGTH(ebs.REF_ORDER_LINE) THEN COALESCE(CAST(oc.shipment_number AS VARCHAR(20)), '~')
                              ELSE RIGHT(ebs.REF_ORDER_LINE, LENGTH(ebs.REF_ORDER_LINE) - POSITION('-' IN ebs.REF_ORDER_LINE)) 
                          END
                      ELSE RIGHT(ebs.REF_ORDER_LINE, LENGTH(ebs.REF_ORDER_LINE) - POSITION('.' IN ebs.REF_ORDER_LINE)) 
                  END,
                  COALESCE(CAST(oc.shipment_number AS VARCHAR(20)), '~')
              )
          AND NOT EXISTS (
              SELECT 1 
              FROM edw_prod_dbo.W_ORGANIZATION_D e_orgs
              WHERE e_orgs.SRC_ORG_ID = ebs_inv.organization_id * -1
                AND e_orgs.source_system = 'OC'
          )
    )
    INSERT INTO edw_prod_dbo.W_ORGANIZATION_D (
        ORG_CODE, ORG_NAME, ORG_TYPE, REGION, SRC_ORG_ID, INTEGRATION_ID, CREATED_DATE, UPDATED_DATE, SOURCE_SYSTEM
    )
    SELECT 
        oc.ORG_CODE, oc.ORG_NAME, oc.ORG_TYPE, oc.REGION, oc.SRC_ORG_ID * -1, 
        oc.INTEGRATION_ID || '-EBS-OC-REMAP', CURRENT_TIMESTAMP AS CREATED_DATE, 
        CURRENT_TIMESTAMP AS UPDATED_DATE, 'OC'
    FROM edw_prod_dbo.W_ORGANIZATION_D oc
    JOIN Orgs orgs ON oc.SRC_ORG_ID = orgs.organization_id
    WHERE oc.source_system = 'EBS';

    WITH ORDERS AS (
        SELECT DISTINCT 
            oc_org.org_code, 
            oc_inv.organization_code,  
            ebs.item_id * -1 AS new_inventory_item_id, 
            ebs.ship_from_org_id * -1 AS new_ship_from_org_id,  
            oc.line_id, 
            oc.SHIP_FROM_ORG_ID, 
            oc.ITEM_ID
        FROM edw_prod_dbo.w_sales_orders_v oc
        JOIN edw_prod_dbo.w_sales_orders_v ebs ON oc.ORDER_NUMBER = ebs.REF_ORDER_NUMBER
        JOIN edw_prod_dbo.w_item_org_d oc_inv ON oc.item_id = oc_inv.inventory_item_id
        JOIN edw_prod_dbo.w_item_org_d ebs_inv ON ebs.item_id = ebs_inv.inventory_item_id
        JOIN edw_prod_dbo.W_ORGANIZATION_D oc_org ON oc.ship_from_org_id = oc_org.SRC_ORG_ID
        WHERE oc.source_system = 'OC'
          AND ebs.source_system = 'EBS'
          AND oc_inv.organization_code IN ('BIM', 'BAI_VIR', 'BAK_VIR')
          AND ebs.ship_from_org_id = ebs_inv.organization_id
          AND ebs_inv.item_number = oc_inv.item_number
          AND ebs_inv.source_system = ebs.source_system
          AND oc_inv.source_system = oc.source_system
          AND oc.line_number = 
              CAST(
            CASE
                WHEN POSITION('.' IN ebs.REF_ORDER_LINE) = 0 THEN
                    CASE
                        WHEN POSITION('-' IN ebs.REF_ORDER_LINE) = 0 THEN ebs.REF_ORDER_LINE
                        ELSE LEFT(ebs.REF_ORDER_LINE, POSITION('-' IN ebs.REF_ORDER_LINE) - 1)
                    END
                ELSE LEFT(ebs.REF_ORDER_LINE, POSITION('.' IN ebs.REF_ORDER_LINE) - 1)
            END
            AS bigint)
          AND COALESCE(CAST(oc.shipment_number AS VARCHAR(20)), '~') = 
              COALESCE(
                  CASE 
                      WHEN POSITION('.' IN ebs.REF_ORDER_LINE) = 0 OR POSITION('.' IN ebs.REF_ORDER_LINE) = LENGTH(ebs.REF_ORDER_LINE) THEN 
                          CASE 
                              WHEN POSITION('-' IN ebs.REF_ORDER_LINE) = 0 OR POSITION('-' IN ebs.REF_ORDER_LINE) = LENGTH(ebs.REF_ORDER_LINE) THEN COALESCE(CAST(oc.shipment_number AS VARCHAR(20)), '~')
                              ELSE RIGHT(ebs.REF_ORDER_LINE, LENGTH(ebs.REF_ORDER_LINE) - POSITION('-' IN ebs.REF_ORDER_LINE)) 
                          END
                      ELSE RIGHT(ebs.REF_ORDER_LINE, LENGTH(ebs.REF_ORDER_LINE) - POSITION('.' IN ebs.REF_ORDER_LINE)) 
                  END,
                  COALESCE(CAST(oc.shipment_number AS VARCHAR(20)), '~')
              )
		AND oc.ship_from_org_id = 0
    )
	
   /* UPDATE v
    SET 
        v.ship_from_org_id = o.new_ship_from_org_id,
        v.item_id = o.new_inventory_item_id,
        v.virtual_organization_id = o.SHIP_FROM_ORG_ID,
        v.virtual_item_id = o.ITEM_ID
    FROM ORDERS o
    JOIN edw_prod_dbo.W_SALES_ORDERS_V v ON o.line_id = v.line_id 
    WHERE v.SOURCE_system = 'OC'
      AND (v.ship_from_org_id <> o.new_ship_from_org_id OR v.item_id <> o.new_inventory_item_id);
*/-- commented on 19-08-2025 New Approach is used reason View Cant be Updated in Postgres 

UPDATE edw_prod_dbo.w_sales_orders_f AS v
SET
    ship_from_org_id        = o.new_ship_from_org_id,
    item_id                 = o.new_inventory_item_id,
    virtual_organization_id = o.ship_from_org_id,
    virtual_item_id         = o.item_id
FROM orders AS o
WHERE v.source_system = 'OC'
  AND o.line_id = v.line_id
  AND (
        v.ship_from_org_id IS DISTINCT FROM o.new_ship_from_org_id
     OR v.item_id          IS DISTINCT FROM o.new_inventory_item_id
  );
    -- Update product table
    WITH missing_prod AS (
        SELECT DISTINCT o.inventory_item_id, 300000003579286 AS organization_id 
        FROM edw_prod_dbo.w_product_d p
        JOIN edw_prod_dbo.w_item_org_d o ON p.inventory_item_id = o.INVENTORY_ITEM_ID * -1
        WHERE p.source_system = 'EBS'
          AND o.source_system = 'OC'
          AND o.inventory_item_id < 0
          AND NOT EXISTS (
              SELECT 1 
              FROM edw_prod_dbo.w_product_d d1 
              WHERE d1.inventory_item_id = o.inventory_item_id 
                AND d1.organization_id = 300000003579286
          )
    )
    INSERT INTO edw_prod_dbo.w_product_d (
        inventory_item_id, organization_id, ITEM_NUMBER, ITEM_DESCRIPTION, ORGANIZATION_CODE, ORGANIZATION_NAME, 
        DEFAULT_SHIP_ORG, INTERNAL_ITEM, PRODUCT_FAMILY, PRODUCT_LINE, PRODUCT_MODEL, COMMODITY_CLASS, 
        CATEGORY, SOURCING_SEGMENT, COUNTRY_OF_ORIGIN, eccn, COPY_SEGMENT, integration_id, created_date, 
        updated_date, source_system, where_used, category_name
    )
    SELECT 
        m.inventory_item_id, 
        m.organization_id, 
        ITEM_NUMBER, ITEM_DESCRIPTION, ORGANIZATION_CODE, ORGANIZATION_NAME, 
        DEFAULT_SHIP_ORG, INTERNAL_ITEM, PRODUCT_FAMILY, PRODUCT_LINE, PRODUCT_MODEL, 
        COMMODITY_CLASS, CATEGORY, SOURCING_SEGMENT, COUNTRY_OF_ORIGIN, eccn, 
        COPY_SEGMENT, integration_id || '.' || CAST(m.inventory_item_id AS VARCHAR(300)) || '.-300000003579286' AS integration_id, 
        CURRENT_TIMESTAMP AS created_date, 
        CURRENT_TIMESTAMP AS updated_date, 
        'OC' AS source_system, 
        where_used, 
        category_name
    FROM edw_prod_dbo.w_product_d d
    JOIN missing_prod m ON d.inventory_item_id = m.inventory_item_id * -1;

    CALL edw_prod_dbo.edw_log_update_job('updatecloudorderswithebsorgs', 'C', 'Complete'); 

END;
$procedure$
;