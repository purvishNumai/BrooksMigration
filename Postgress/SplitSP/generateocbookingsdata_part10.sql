CREATE OR REPLACE PROCEDURE edw_prod_dbo.generateocbookingsdata_part10()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_start_time timestamp;
    v_runtime interval;
BEGIN
    
    v_start_time := clock_timestamp();
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part10 - Started at %', v_start_time;

    -------------------------------------------------------------------------
    -- update fnc_bookings_mv metadata from sales_orders_d
    -------------------------------------------------------------------------
    UPDATE edw_prod_dbo.fnc_bookings_mv f
    SET order_created_by   = s.order_created_by,
        line_created_by    = s.line_created_by,
        last_updated_by    = s.last_update_by,
        line_creation_date = CASE WHEN f.line_creation_date IS NOT NULL THEN f.line_creation_date ELSE s.line_creation_date END,
        order_creation_date= CASE WHEN f.order_creation_date IS NOT NULL THEN f.order_creation_date ELSE s.order_creation_date END,
        last_update_date   = CASE WHEN f.last_update_date IS NOT NULL THEN f.last_update_date ELSE s.last_update_date END
    FROM edw_prod_dbo.w_sales_orders_d s
    WHERE f.line_id = s.line_id
      AND f.source_system = s.source_system;

    -------------------------------------------------------------------------
    -- update fnc_backlog_billing_mv metadata from sales_orders_d
    -------------------------------------------------------------------------
    UPDATE edw_prod_dbo.fnc_backlog_billing_mv f
    SET line_creation_date = CASE WHEN f.line_creation_date IS NOT NULL THEN f.line_creation_date ELSE s.line_creation_date END,
        order_creation_date= CASE WHEN f.order_creation_date IS NOT NULL THEN f.order_creation_date ELSE s.order_creation_date END,
        last_update_date   = CASE WHEN f.last_update_date IS NOT NULL THEN f.last_update_date ELSE s.last_update_date END
    FROM edw_prod_dbo.w_sales_orders_d s
    WHERE f.line_id = s.line_id
      AND f.source_system = s.source_system;

    -------------------------------------------------------------------------
    -- update Product Family/Line/Model for backlog/bookings (fix logic bug to reference m fields)
    -------------------------------------------------------------------------
    UPDATE edw_prod_dbo.fnc_backlog_billing_mv m
    SET "Product Family" = REPLACE(i.product_family, '_', ' '),
        "Product Line"   = REPLACE(i.product_line, '_', ' '),
        "Product Model"  = REPLACE(i.product_model, '_', ' ')
    FROM edw_prod_dbo.w_sales_orders_f f
    JOIN edw_prod_dbo.w_item_org_d i
      ON i.inventory_item_id = f.item_id
      AND i.organization_id = f.ship_from_org_id
    WHERE m.line_id = f.line_id
      AND i.product_family IS NOT NULL
      AND m."Product Family" IS NULL
      AND m.source_system = i.source_system
      AND f.source_system = i.source_system;

    UPDATE edw_prod_dbo.fnc_bookings_mv m
    SET "Product Family" = REPLACE(i.product_family, '_', ' '),
        "Product Line"   = REPLACE(i.product_line, '_', ' '),
        "Product Model"  = REPLACE(i.product_model, '_', ' ')
    FROM edw_prod_dbo.w_sales_orders_f f
    JOIN edw_prod_dbo.w_item_org_d i
      ON i.inventory_item_id = f.item_id
      AND i.organization_id = f.ship_from_org_id
    WHERE m.line_id = f.line_id
      AND i.product_family IS NOT NULL
      AND m."Product Family" IS NULL
      AND m.source_system = i.source_system
      AND f.source_system = i.source_system;

    -- same updates without org check (kept duplicates per MSSQL)
    UPDATE edw_prod_dbo.fnc_backlog_billing_mv m
    SET "Product Family" = REPLACE(i.product_family, '_', ' '),
        "Product Line"   = REPLACE(i.product_line, '_', ' '),
        "Product Model"  = REPLACE(i.product_model, '_', ' ')
    FROM edw_prod_dbo.w_sales_orders_f f
    JOIN edw_prod_dbo.w_item_org_d i
      ON i.inventory_item_id = f.item_id
    WHERE m.line_id = f.line_id
      AND i.product_family IS NOT NULL
      AND m."Product Family" IS NULL
      AND m.source_system = i.source_system
      AND f.source_system = i.source_system;

    UPDATE edw_prod_dbo.fnc_bookings_mv m
    SET "Product Family" = REPLACE(i.product_family, '_', ' '),
        "Product Line"   = REPLACE(i.product_line, '_', ' '),
        "Product Model"  = REPLACE(i.product_model, '_', ' ')
    FROM edw_prod_dbo.w_sales_orders_f f
    JOIN edw_prod_dbo.w_item_org_d i
      ON i.inventory_item_id = f.item_id
    WHERE m.line_id = f.line_id
      AND i.product_family IS NOT NULL
      AND m."Product Family" IS NULL
      AND m.source_system = i.source_system
      AND f.source_system = i.source_system;


    -------------------------------------------------------------------------
    -- update Order Date for bookings/backlog when invoiced
    -------------------------------------------------------------------------
    UPDATE edw_prod_dbo.fnc_bookings_mv f
    SET "Order Date" = COALESCE(s.inv_gl_date, s.inv_trx_date)
    FROM edw_prod_dbo.w_sales_orders_d s
    WHERE f.line_id = s.line_id
      AND f.source_system = s.source_system
      AND f."Order Date" IS NULL
      AND s.life_cycle_status = 'Invoiced'
      AND COALESCE(s.inv_gl_date, s.inv_trx_date) IS NOT NULL;

    UPDATE edw_prod_dbo.fnc_backlog_billing_mv f
    SET "Order Date" = COALESCE(s.inv_gl_date, s.inv_trx_date)
    FROM edw_prod_dbo.w_sales_orders_d s
    WHERE f.line_id = s.line_id
      AND f.source_system = s.source_system
      AND f."Order Date" IS NULL
      AND s.life_cycle_status = 'Invoiced'
      AND COALESCE(s.inv_gl_date, s.inv_trx_date) IS NOT NULL;

    -------------------------------------------------------------------------
    -- fix backlog dates to first of current month for non-invoiced
    -------------------------------------------------------------------------
    UPDATE edw_prod_dbo.fnc_backlog_billing_mv
    SET "Order Date" = date_trunc('month', current_date)
    WHERE "Line Order Type" <> 'Invoiced'
      AND "Order Date" < date_trunc('month', current_date);

    -- Calc and Log Duration
    v_runtime := clock_timestamp() - v_start_time;
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part10 - Completed. Duration %', v_runtime;
      
END;
$procedure$
;