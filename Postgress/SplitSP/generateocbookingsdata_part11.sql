CREATE OR REPLACE PROCEDURE edw_prod_dbo.generateocbookingsdata_part11()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_start_time   timestamp;
    v_end_time     timestamp;
    v_runtime_ms   numeric;
BEGIN
    
    -- Start Time
    v_start_time := clock_timestamp();
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part11 - Started at %', v_start_time;

    -------------------------------------------------------------------------
    -- main logic: update backlog + bookings with EBS ship from costs
    -------------------------------------------------------------------------

    -------------------------------------------------------------------------
    -- step 1: build temp table of item/org + order_date + line_id
    -------------------------------------------------------------------------
    -- step 2: update backlog costs
    -- step 3: update bookings costs
    -------------------------------------------------------------------------
-------------------------------------------------------------------------
-- temp table to hold item/org + order_date + line_id (mimic @table variable)
-------------------------------------------------------------------------
CREATE TEMP TABLE cost_temp1 (
    inventory_item_id bigint,
    organization_id bigint,
    order_date date,
    line_id bigint
);
 
INSERT INTO cost_temp1 (inventory_item_id, organization_id, order_date, line_id)
SELECT
  ebs_inv.inventory_item_id,
  ebs_inv.organization_id,
  m1.order_date,
  oc.line_id
FROM
/* OC orders: only keys, filtered inside */
(
  SELECT
    customer_po,
    line_id::bigint  AS line_id,
    item_id::bigint  AS item_id,
    source_system
  FROM edw_prod_dbo.w_sales_orders_v
  WHERE source_system = 'OC'
) oc
 
/* EBS orders: only keys, filtered inside */
JOIN (
  SELECT
    customer_po,
    item_id::bigint          AS item_id,
    ship_from_org_id::bigint AS ship_from_org_id,
    source_system
  FROM edw_prod_dbo.w_sales_orders_v
  WHERE source_system = 'EBS'
) ebs
  ON ebs.customer_po = oc.customer_po
/* OC item/org: restricted to BIM + match source_system */
JOIN (
  SELECT
    inventory_item_id,
    item_number,
    source_system
  FROM edw_prod_dbo.w_item_org_d
  WHERE organization_code = 'BIM'
) oc_inv
  ON oc.item_id       = oc_inv.inventory_item_id
AND oc.source_system = oc_inv.source_system
/* EBS item/org: only what we use */
JOIN (
  SELECT
    inventory_item_id,
    organization_id,
    item_number,
    source_system
  FROM edw_prod_dbo.w_item_org_d
) ebs_inv
  ON ebs.item_id          = ebs_inv.inventory_item_id
AND ebs.source_system    = ebs_inv.source_system
And ebs.ship_from_org_id = ebs_inv.organization_id
AND ebs_inv.item_number  = oc_inv.item_number
 
/* Backlog: typed once; only needed cols */
JOIN (
  SELECT
    line_id::bigint    AS line_id,
    "Order Date"::date AS order_date
  FROM edw_prod_dbo.fnc_backlog_billing_mv
) m1
  ON m1.line_id = oc.line_id
/* Eligibility: SAME logic as your EXISTS, set-based & deduped */
JOIN (
  SELECT DISTINCT
    dfla.fulfilllinelineid::bigint AS line_id
  FROM oc_prod_dbo.doo_fulfill_lines_all       dfla
  JOIN oc_prod_dbo.doo_lines_all               dla
    ON dla.lineid = dfla.fulfilllinelineid
  JOIN oc_prod_dbo.doo_headers_all             dha
    ON dha.headerid = dla.lineheaderid
  JOIN oc_prod_dbo.msc_xref_mapping            mxm1
    ON mxm1.entity_name = 'SUPPLIERS'
   AND mxm1.target_value = dfla.fulfilllinesupplierid
  JOIN oc_prod_dbo.msc_xref_mapping            mxm2
    ON mxm2.entity_name = 'SUPPLIER_SITES'
   AND mxm2.target_value = dfla.fulfilllinesuppliersiteid
  JOIN oc_prod_dbo.poz_suppliers               psv
    ON psv.vendorid = mxm1.source_value
  JOIN oc_prod_dbo.poz_supplier_sites_all_m    pssam
    ON pssam.vendorsiteid = mxm2.source_value
  JOIN oc_prod_dbo.msc_trading_partner_sites   mtps
    ON mtps.partner_id      = dfla.fulfilllinesupplierid
   AND mtps.partner_site_id = dfla.fulfilllinesuppliersiteid
  WHERE dha.headersubmittedflag = 'Y'
) elig
  ON elig.line_id = oc.line_id;
/**
SELECT ebs_inv.inventory_item_id,
       ebs_inv.organization_id,
       m1."Order Date",
       m1.line_id
FROM edw_prod_dbo.w_sales_orders_v oc
JOIN edw_prod_dbo.w_sales_orders_v ebs ON oc.customer_po = ebs.customer_po
JOIN edw_prod_dbo.w_item_org_d oc_inv ON oc.item_id::BIGINT = oc_inv.inventory_item_id
JOIN edw_prod_dbo.w_item_org_d ebs_inv ON ebs.item_id = ebs_inv.inventory_item_id
JOIN edw_prod_dbo.fnc_backlog_billing_mv m1 ON m1.line_id::BIGINT = oc.line_id
WHERE oc.source_system = 'OC'
  AND ebs.source_system = 'EBS'
  AND oc_inv.organization_code = 'BIM'
  AND ebs.ship_from_org_id::BIGINT = ebs_inv.organization_id
  AND ebs_inv.item_number = oc_inv.item_number
  AND ebs_inv.source_system = ebs.source_system
  AND CAST(oc_inv.source_system AS BIGINT) = CAST(oc.source_system AS BIGINT)
  AND EXISTS (
        SELECT 1
        FROM oc_prod_dbo.msc_trading_partner_sites mtps
        JOIN oc_prod_dbo.doo_fulfill_lines_all dfla ON dfla.fulfilllinelineid::NUMERIC = oc.line_id
        JOIN oc_prod_dbo.doo_lines_all dla ON dla.lineid = dfla.fulfilllinelineid
        JOIN oc_prod_dbo.doo_headers_all dha ON dla.lineheaderid = dha.headerid
        JOIN oc_prod_dbo.msc_xref_mapping mxm1 ON mxm1.entity_name = 'SUPPLIERS' AND mxm1.target_value = dfla.fulfilllinesupplierid
        JOIN oc_prod_dbo.msc_xref_mapping mxm2 ON mxm2.entity_name = 'SUPPLIER_SITES' AND mxm2.target_value = dfla.fulfilllinesuppliersiteid
        JOIN oc_prod_dbo.poz_suppliers psv ON psv.vendorid = mxm1.source_value
        JOIN oc_prod_dbo.poz_supplier_sites_all_m pssam ON pssam.vendorsiteid = mxm2.source_value
        WHERE dha.headersubmittedflag = 'Y'
          AND dfla.fulfilllinesupplierid = mtps.partner_id
          AND dfla.fulfilllinesuppliersiteid = mtps.partner_site_id
  );
*/

    -- Update backlog costs
    UPDATE edw_prod_dbo.fnc_backlog_billing_mv m1
    SET "Ship From Item Cost LC" = edw_prod_dbo.edw_getcostfrozencostperavglc(e.inventory_item_id::BIGINT, e.organization_id::BIGINT, e.order_date::date, 'EBS'),
        "Ship From Item Cost USD" = edw_prod_dbo.edw_getcostfrozencostperavgusd(e.inventory_item_id::BIGINT, e.organization_id::BIGINT, e.order_date::date, 'EBS'),
        "Ext Ship From Item Cost LC" = edw_prod_dbo.edw_getcostfrozencostperavglc(e.inventory_item_id::BIGINT, e.organization_id::BIGINT, e.order_date::date, 'EBS') * m1."Ordered Quantity",
        "Ext Ship From Item Cost USD" = edw_prod_dbo.edw_getcostfrozencostperavgusd(e.inventory_item_id::BIGINT, e.organization_id::BIGINT, e.order_date::date, 'EBS') * m1."Ordered Quantity"
    FROM cost_temp1 e
    WHERE e.line_id = m1.line_id;

    TRUNCATE cost_temp1;

    -- Insert into temp for bookings

    INSERT INTO cost_temp1 (inventory_item_id, organization_id, order_date, line_id)
SELECT
  ebs_inv.inventory_item_id,
  ebs_inv.organization_id,
  m1.order_date,
  oc.line_id
FROM
/* OC orders (keys only) */
(
  SELECT
    customer_po,
    line_id::bigint AS line_id,
    item_id::bigint AS item_id,
    source_system
  FROM edw_prod_dbo.w_sales_orders_v
  WHERE source_system = 'OC'
) oc

/* EBS orders (keys only) */
JOIN (
  SELECT
    customer_po,
    item_id::bigint          AS item_id,
    ship_from_org_id::bigint AS ship_from_org_id,
    source_system
  FROM edw_prod_dbo.w_sales_orders_v
  WHERE source_system = 'EBS'
) ebs
  ON ebs.customer_po = oc.customer_po

/* OC item org (only BIM, only used cols) */
JOIN (
  SELECT
    inventory_item_id,
    item_number,
    source_system
  FROM edw_prod_dbo.w_item_org_d
  WHERE organization_code = 'BIM'
) oc_inv
  ON oc.item_id       = oc_inv.inventory_item_id
 AND oc.source_system = oc_inv.source_system

/* EBS item org (only required cols) */
JOIN (
  SELECT
    inventory_item_id,
    organization_id,
    item_number,
    source_system
  FROM edw_prod_dbo.w_item_org_d
) ebs_inv
  ON ebs.item_id          = ebs_inv.inventory_item_id
 AND ebs.source_system    = ebs_inv.source_system
 AND ebs.ship_from_org_id = ebs_inv.organization_id
 AND ebs_inv.item_number  = oc_inv.item_number

/* Bookings: slimmed to key + date */
JOIN (
  SELECT
    line_id::bigint    AS line_id,
    "Order Date"::date AS order_date
  FROM edw_prod_dbo.fnc_bookings_mv
) m1
  ON m1.line_id = oc.line_id

/* Eligibility JOIN (replaces EXISTS) */
JOIN (
  SELECT DISTINCT
    dfla.fulfilllinelineid::bigint AS line_id
  FROM oc_prod_dbo.doo_fulfill_lines_all       dfla
  JOIN oc_prod_dbo.doo_lines_all               dla
    ON dla.lineid = dfla.fulfilllinelineid
  JOIN oc_prod_dbo.doo_headers_all             dha
    ON dha.headerid = dla.lineheaderid
  JOIN oc_prod_dbo.msc_xref_mapping            mxm1
    ON mxm1.entity_name = 'SUPPLIERS'
   AND mxm1.target_value = dfla.fulfilllinesupplierid
  JOIN oc_prod_dbo.msc_xref_mapping            mxm2
    ON mxm2.entity_name = 'SUPPLIER_SITES'
   AND mxm2.target_value = dfla.fulfilllinesuppliersiteid
  JOIN oc_prod_dbo.poz_suppliers               psv
    ON psv.vendorid = mxm1.source_value
  JOIN oc_prod_dbo.poz_supplier_sites_all_m    pssam
    ON pssam.vendorsiteid = mxm2.source_value
  JOIN oc_prod_dbo.msc_trading_partner_sites   mtps
    ON mtps.partner_id      = dfla.fulfilllinesupplierid
   AND mtps.partner_site_id = dfla.fulfilllinesuppliersiteid
  WHERE dha.headersubmittedflag = 'Y'
) elig
  ON elig.line_id = oc.line_id;


    -- INSERT INTO cost_temp1 (inventory_item_id, organization_id, order_date, line_id)
    -- SELECT ebs_inv.inventory_item_id,
    --        ebs_inv.organization_id,
    --        m1."Order Date",
    --        m1.line_id
    -- FROM edw_prod_dbo.w_sales_orders_v oc
    -- JOIN edw_prod_dbo.w_sales_orders_v ebs ON oc.customer_po = ebs.customer_po
    -- JOIN edw_prod_dbo.w_item_org_d oc_inv ON oc.item_id::BIGINT = oc_inv.inventory_item_id
    -- JOIN edw_prod_dbo.w_item_org_d ebs_inv ON ebs.item_id = ebs_inv.inventory_item_id
    -- JOIN edw_prod_dbo.fnc_bookings_mv m1 ON m1.line_id::BIGINT = oc.line_id
    -- WHERE oc.source_system = 'OC'
    --   AND ebs.source_system = 'EBS'
    --   AND oc_inv.organization_code = 'BIM'
    --   AND ebs.ship_from_org_id = ebs_inv.organization_id
    --   AND ebs_inv.item_number = oc_inv.item_number
    --   AND ebs_inv.source_system = ebs.source_system
    --   AND oc_inv.source_system::text = oc.source_system::text
    --   AND EXISTS (
    --         SELECT 1
    --         FROM oc_prod_dbo.msc_trading_partner_sites mtps
    --         JOIN oc_prod_dbo.doo_fulfill_lines_all dfla ON dfla.fulfilllinelineid::NUMERIC = oc.line_id
    --         JOIN oc_prod_dbo.doo_lines_all dla ON dla.lineid = dfla.fulfilllinelineid
    --         JOIN oc_prod_dbo.doo_headers_all dha ON dla.lineheaderid = dha.headerid
    --         JOIN oc_prod_dbo.msc_xref_mapping mxm1 ON mxm1.entity_name = 'SUPPLIERS' AND mxm1.target_value = dfla.fulfilllinesupplierid
    --         JOIN oc_prod_dbo.msc_xref_mapping mxm2 ON mxm2.entity_name = 'SUPPLIER_SITES' AND mxm2.target_value = dfla.fulfilllinesuppliersiteid
    --         JOIN oc_prod_dbo.poz_suppliers psv ON psv.vendorid = mxm1.source_value
    --         JOIN oc_prod_dbo.poz_supplier_sites_all_m pssam ON pssam.vendorsiteid = mxm2.source_value
    --         WHERE dha.headersubmittedflag = 'Y'
    --           AND dfla.fulfilllinesupplierid = mtps.partner_id
    --           AND dfla.fulfilllinesuppliersiteid = mtps.partner_site_id
    --   );

    -- Update booking costs
    UPDATE edw_prod_dbo.fnc_bookings_mv m1
    SET "Ship From Item Cost LC" = edw_prod_dbo.edw_getcostfrozencostperavglc(e.inventory_item_id::BIGINT, e.organization_id::BIGINT, m1."Order Date"::date, 'EBS'),
        "Ship From Item Cost USD" = edw_prod_dbo.edw_getcostfrozencostperavgusd(e.inventory_item_id::BIGINT, e.organization_id::BIGINT, m1."Order Date"::date, 'EBS'),
        "Ext Ship From Item Cost LC" = edw_prod_dbo.edw_getcostfrozencostperavglc(e.inventory_item_id::BIGINT, e.organization_id::BIGINT, m1."Order Date"::date, 'EBS') * m1."Ordered Quantity",
        "Ext Ship From Item Cost USD" = edw_prod_dbo.edw_getcostfrozencostperavgusd(e.inventory_item_id::BIGINT, e.organization_id::BIGINT, m1."Order Date"::date, 'EBS') * m1."Ordered Quantity"
    FROM cost_temp1 e
    WHERE e.line_id = m1.line_id;

    -- Set actual costs for invoiced records
   WITH acts AS (
        SELECT f.trx_source_line_id,
               f.inventory_item_id,
               f.organization_id,
               SUM(f.transaction_value * f.transaction_quantity) AS act_cost
        FROM edw_prod_dbo.w_material_trx_sl_f f
        GROUP BY f.trx_source_line_id, f.inventory_item_id, f.organization_id
    )
    UPDATE edw_prod_dbo.fnc_backlog_billing_mv m
    SET "Ship From Item Cost LC"  = ROUND(edw_prod_dbo.edw_getcostfrozencostperavglc(f.inventory_item_id::BIGINT, f.organization_id::BIGINT, m."Order Date"::date, 'EBS')::numeric, 2),
        "Ship From Item Cost USD" = ROUND(edw_prod_dbo.edw_getcostfrozencostperavgusd(f.inventory_item_id::BIGINT, f.organization_id::BIGINT, m."Order Date"::date, 'EBS')::numeric, 2),
        "Ext Ship From Item Cost LC" = ROUND(edw_prod_dbo.edw_getcostfrozencostperavglc(f.inventory_item_id::BIGINT, f.organization_id::BIGINT, m."Order Date"::date, 'EBS')::numeric * COALESCE(m."Quantity Invoiced", m."Ordered Quantity")::numeric, 2),
        "Ext Ship From Item Cost USD" = ROUND(edw_prod_dbo.edw_getcostfrozencostperavgusd(f.inventory_item_id::BIGINT, f.organization_id::BIGINT, m."Order Date"::date, 'EBS')::numeric * COALESCE(m."Quantity Invoiced", m."Ordered Quantity")::numeric, 2)
    FROM acts f
    WHERE m.line_id = f.trx_source_line_id;

    -------------------------------------------------------------------------
    -- finish: call update GLSO and end job
    -------------------------------------------------------------------------
    CALL edw_prod_dbo.updateglsodata();
    CALL edw_prod_dbo.edw_log_update_job('GenerateOCBookingsData','C','Complete');

    -- clean up temp
    DROP TABLE IF EXISTS cost_temp1;

    -- Calc and Log Duration
    -- End time
    v_end_time := clock_timestamp();
    -- Duration in milliseconds
    v_runtime_ms := EXTRACT(EPOCH FROM (v_end_time - v_start_time)) * 1000;
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part11 - Completed. Start: %, End: %, Duration: % ms',v_start_time,v_end_time,v_runtime_ms;
END;
$procedure$
;