CREATE OR REPLACE PROCEDURE edw_prod_dbo.generateocbookingsdata_part6()
 LANGUAGE plpgsql
AS $procedure$

DECLARE
    v_start_time timestamp;
    v_runtime interval;
BEGIN

    v_start_time := clock_timestamp();
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part6 - Started at %', v_start_time;

    -------------------------------------------------------------------------
    -- INSERT ORDER LINES THAT DO NOT EXIST (INITIAL)
    -------------------------------------------------------------------------
    INSERT INTO edw_prod_dbo.w_booking_trxn (
        order_id, line_id, org_id, invoice_to_org_id, ship_to_org_id,
        sold_to_org_id, ship_from_org_id, inventory_item_id,
        bkg_change_date, transaction_type, qty_ordered, qty_shipped,
        qty_cancelled, qty_fulfilled, qty_invoiced, unit_cost,
        unit_selling_price, extended_price, net_qty_ordered,
        net_qty_shipped, net_qty_cancelled, net_qty_fulfilled,
        net_qty_invoiced, net_unit_cost, net_unit_selling_price,
        net_extended_price, integration_id, created_date,
        updated_date, source_system, snapshot_date, status
    )
    SELECT
        source.order_id,
        source.line_id,
        source.org_id,
        source.invoice_to_org_id,
        source.ship_to_org_id,
        source.sold_to_org_id,
        source.ship_from_org_id,
        source.item_id,
        NOW(),
        'INITIAL-oc',
        COALESCE(source.qty_ordered, 0),
        COALESCE(source.qty_shipped, 0),
        COALESCE(source.qty_cancelled, 0),
        COALESCE(source.qty_fulfilled, 0),
        COALESCE(source.qty_invoiced, 0),
        COALESCE(source.unit_cost, 0),
        COALESCE(source.unit_price, 0),
        COALESCE(source.qty_ordered, 0) * COALESCE(source.unit_price, 0),
        COALESCE(source.qty_ordered, 0),
        COALESCE(source.qty_shipped, 0),
        COALESCE(source.qty_cancelled, 0),
        COALESCE(source.qty_fulfilled, 0),
        COALESCE(source.qty_invoiced, 0),
        COALESCE(source.unit_cost, 0),
        COALESCE(source.unit_price, 0),
        COALESCE(source.qty_ordered, 0) * COALESCE(source.unit_price, 0),
        source.integration_id::VARCHAR || '.' || gen_random_uuid()::VARCHAR,
        source.created_date,
        source.updated_date,
        'OC',
        NOW(),
        'Booked'
    FROM edw_prod_dbo.w_sales_orders_f source,
         edw_prod_dbo.w_sales_orders_d d
    WHERE NOT EXISTS (
              SELECT 1
              FROM edw_prod_dbo.w_booking_trxn f
              WHERE f.line_id = source.line_id
                AND f.source_system = source.source_system
          )
      AND d.line_id = source.line_id
      AND d.order_status NOT IN ('DOO_REFERENCE','DOO_DRAFT')
      AND d.fulfill_line_status <> 'DRAFT'
      AND d.source_system = source.source_system
      AND source.source_system = 'OC';

    -------------------------------------------------------------------------
    -- INSERT CHANGE01 rows (differences vs last snapshot)
    -------------------------------------------------------------------------
    INSERT INTO edw_prod_dbo.w_booking_trxn (
        order_id, line_id, org_id, invoice_to_org_id, ship_to_org_id,
        sold_to_org_id, ship_from_org_id, inventory_item_id, bkg_change_date,
        transaction_type, qty_ordered, qty_shipped, qty_cancelled,
        qty_fulfilled, qty_invoiced, unit_cost, unit_selling_price,
        extended_price, net_qty_ordered, net_qty_shipped, net_qty_cancelled,
        net_qty_fulfilled, net_qty_invoiced, net_unit_cost,
        net_unit_selling_price, net_extended_price, integration_id,
        created_date, updated_date, source_system, snapshot_date, status
    )
    SELECT
        source.order_id,
        source.line_id,
        source.org_id,
        source.invoice_to_org_id,
        source.ship_to_org_id,
        source.sold_to_org_id,
        source.ship_from_org_id,
        source.item_id,
        NOW(),
        'CHANGE01',
        COALESCE(source.qty_ordered,0) - COALESCE(f.qty_ordered,0),
        COALESCE(source.qty_shipped,0) - COALESCE(f.qty_shipped,0),
        COALESCE(source.qty_cancelled,0) - COALESCE(f.qty_cancelled,0),
        COALESCE(source.qty_fulfilled,0) - COALESCE(f.qty_fulfilled,0),
        COALESCE(source.qty_invoiced,0) - COALESCE(f.qty_invoiced,0),
        COALESCE(source.unit_cost,0) - COALESCE(f.unit_cost,0),
        COALESCE(source.unit_price,0) - COALESCE(f.unit_selling_price,0),
        (COALESCE(source.qty_ordered,0) * COALESCE(source.unit_price,0)) - f.extended_price,
        COALESCE(source.qty_ordered,0),
        COALESCE(source.qty_shipped,0),
        COALESCE(source.qty_cancelled,0),
        COALESCE(source.qty_fulfilled,0),
        COALESCE(source.qty_invoiced,0),
        COALESCE(source.unit_cost,0),
        COALESCE(source.unit_price,0),
        COALESCE(source.qty_ordered,0) * COALESCE(source.unit_price,0),
        source.integration_id || '.' || gen_random_uuid(),
        source.created_date,
        source.updated_date,
        'OC',
        NOW(),
        CASE WHEN d.fulfill_line_status = 'CANCELED' THEN 'Cancelled' ELSE 'Booked' END
    FROM edw_prod_dbo.w_sales_orders_f source
    JOIN edw_prod_dbo.w_booking_trxn f ON f.line_id = source.line_id
                         AND source.source_system = f.source_system
    JOIN edw_prod_dbo.w_sales_orders_d d ON d.line_id = source.line_id
                            AND d.source_system = source.source_system
    WHERE (COALESCE(f.net_qty_cancelled,0) <> COALESCE(source.qty_cancelled,0)
           OR COALESCE(f.net_qty_ordered,0) <> COALESCE(source.qty_ordered,0)
           OR COALESCE(f.net_unit_selling_price,0) <> COALESCE(source.unit_price,0))
      AND source.source_system = 'OC'
      AND f.snapshot_date = (
            SELECT max(d1.snapshot_date)
            FROM edw_prod_dbo.w_booking_trxn d1
            WHERE d1.line_id = source.line_id
        )
      AND COALESCE(d.fulfill_line_status,'Why Would I be NULL?') NOT IN ('DRAFT')
      AND d.order_status NOT IN ('DOO_REFERENCE','DOO_DRAFT','CANCELED')
      AND d.life_cycle_status <> 'Cancelled';

    -------------------------------------------------------------------------
    -- INSERT CANCEL Change rows (from INITIAL rows when lifecycle is Cancelled)
    -------------------------------------------------------------------------
    INSERT INTO edw_prod_dbo.w_booking_trxn (
        order_id, line_id, org_id, invoice_to_org_id, ship_to_org_id,
        sold_to_org_id, ship_from_org_id, inventory_item_id, bkg_change_date,
        transaction_type, qty_ordered, qty_shipped, qty_cancelled,
        qty_fulfilled, qty_invoiced, unit_cost, unit_selling_price,
        extended_price, net_qty_ordered, net_qty_shipped, net_qty_cancelled,
        net_qty_fulfilled, net_qty_invoiced, net_unit_cost,
        net_unit_selling_price, net_extended_price, integration_id,
        created_date, updated_date, source_system, snapshot_date,
        booked_date, status
    )
    SELECT
        x.order_id,
        x.line_id,
        x.org_id,
        x.invoice_to_org_id,
        x.ship_to_org_id,
        x.sold_to_org_id,
        x.ship_from_org_id,
        x.inventory_item_id,
        x.bkg_change_date,
        'Change',
        x.qty_ordered * -1,
        x.qty_shipped * -1,
        x.qty_ordered,
        x.qty_fulfilled,
        x.qty_invoiced,
        x.unit_cost * -1,
        x.unit_selling_price * -1,
        x.extended_price * -1,
        x.net_qty_ordered * -1,
        x.net_qty_shipped * -1,
        x.net_qty_ordered * -1,
        0,
        0,
        0,
        0,
        x.net_extended_price * -1,
        x.integration_id || '.' || gen_random_uuid(),
        NOW(),
        NOW(),
        x.source_system,
        NOW(),
        x.booked_date,
        'Cancelled'
    FROM edw_prod_dbo.w_booking_trxn x
    JOIN edw_prod_dbo.w_sales_orders_d d ON d.line_id = x.line_id
                            AND x.source_system = d.source_system
    WHERE d.life_cycle_status = 'Cancelled'
      AND x.transaction_type LIKE 'INITIAL%'
      AND COALESCE(x.status,'Booked') = 'Booked'
      AND x.source_system = 'OC';

    -------------------------------------------------------------------------
    -- mark existing initial transactions as Cancelled
    -------------------------------------------------------------------------
    UPDATE edw_prod_dbo.w_booking_trxn x
    SET status = 'Cancelled'
    FROM edw_prod_dbo.w_sales_orders_d d
    WHERE d.line_id = x.line_id
      AND d.life_cycle_status = 'Cancelled'
      AND x.transaction_type LIKE 'INITIAL%'
      AND COALESCE(x.status,'Booked') = 'Booked'
      AND x.source_system = d.source_system
      AND x.source_system = 'OC';

    -------------------------------------------------------------------------
    -- sync booked_date in booking_trxn from sales_orders_d
    -------------------------------------------------------------------------
    UPDATE edw_prod_dbo.w_booking_trxn b
    SET booked_date = d.booked_date
    FROM edw_prod_dbo.w_sales_orders_d d
    WHERE b.line_id = d.line_id
      --AND COALESCE(b.booked_date, NOW()) <> COALESCE(d.order_creation_date, NOW() - INTERVAL '1 day')
	  AND COALESCE(b.booked_date::date, CURRENT_DATE)
        <> COALESCE(d.order_creation_date::date, CURRENT_DATE - 1)
      AND d.booked_date IS NOT NULL
      AND b.source_system = d.source_system;

    -- Calc and Log Duration
    v_runtime := clock_timestamp() - v_start_time;
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part6 - Completed. Duration %', v_runtime;
      
END;
$procedure$
;