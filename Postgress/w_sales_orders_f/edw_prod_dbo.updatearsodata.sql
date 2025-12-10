-- DROP PROCEDURE edw_prod_dbo.updatearsodata();

CREATE OR REPLACE PROCEDURE edw_prod_dbo.updatearsodata()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
   --start log
  Call edw_prod_dbo.edw_log_start_job('UpdateARSOData');

  /* ========== Currency backfill from OC daily rates (SL) ========== */
  UPDATE edw_prod_dbo.w_ar_invoice_sl_f w
  SET exchange_rate = gdr.dailyrateconversionrate::numeric,
      invoice_currency_code = gdr.dailyratefromcurrency
  FROM oc_prod_dbo.gl_ledgers                 gl
  JOIN oc_prod_dbo.ra_cust_trx_line_gl_dist_all gld
    ON gld.racusttrxlinegldistsetofbooksid = gl.ledgerledgerid
  JOIN oc_prod_dbo.ra_customer_trx_all       r
    ON gld.racusttrxlinegldistcustomertrxid = r.racustomertrxcustomertrxid
  JOIN oc_prod_dbo.gl_daily_conversion_types gdct
    ON gdct.dailyconversiontypeconversiontype = gdct.dailyconversiontypeconversiontype
  JOIN oc_prod_dbo.gl_daily_rates            gdr
    ON gdr.dailyratefromcurrency = gl.ledgercurrencycode
   AND gdr.dailyrateconversiontype = gdct.dailyconversiontypeconversiontype
   AND gdr.dailyratetocurrency = 'USD'
   AND gld.racusttrxlinegldistgldate = gdr.dailyrateconversiondate
  WHERE r.racustomertrxinvoicecurrencycode <> 'USD'
    AND r.racustomertrxexchangerate IS NULL
    AND w.customer_trx_line_id = gld.racusttrxlinegldistcustomertrxlineid::BIGINT
    AND w.exchange_rate IS NULL;

  /* ========== Currency backfill from OC daily rates (Header) ========== */
  UPDATE edw_prod_dbo.w_ar_invoice_f w
  SET exchange_rate = gdr.dailyrateconversionrate::numeric,
      invoice_currency_code = gdr.dailyratefromcurrency
  FROM oc_prod_dbo.gl_ledgers                 gl
  JOIN oc_prod_dbo.ra_cust_trx_line_gl_dist_all gld
    ON gld.racusttrxlinegldistsetofbooksid = gl.ledgerledgerid
  JOIN oc_prod_dbo.ra_customer_trx_all       r
    ON gld.racusttrxlinegldistcustomertrxid = r.racustomertrxcustomertrxid
  JOIN oc_prod_dbo.gl_daily_conversion_types gdct
    ON gdct.dailyconversiontypeconversiontype = gdct.dailyconversiontypeconversiontype
  JOIN oc_prod_dbo.gl_daily_rates            gdr
    ON gdr.dailyratefromcurrency = gl.ledgercurrencycode
   AND gdr.dailyrateconversiontype = gdct.dailyconversiontypeconversiontype
   AND gdr.dailyratetocurrency = 'USD'
   AND gld.racusttrxlinegldistgldate = gdr.dailyrateconversiondate
  WHERE r.racustomertrxinvoicecurrencycode <> 'USD'
    AND r.racustomertrxexchangerate IS NULL
    AND w.customer_trx_line_id = gld.racusttrxlinegldistcustomertrxlineid::BIGINT
    AND w.exchange_rate IS NULL;

  /* ========== Sync unit prices from AR back to SO (OC only) ========== */
  UPDATE edw_prod_dbo.w_sales_orders_f sf
  SET unit_price = a.unit_selling_price
  FROM edw_prod_dbo.w_ar_invoice_f a
  WHERE sf.line_id = a.so_line_id
    AND a.source_system = sf.source_system
    AND a.source_system = 'OC'
    AND COALESCE(sf.unit_price,0) <> COALESCE(a.unit_selling_price,0);

  UPDATE edw_prod_dbo.w_sales_orders_f sf
  SET unit_price_usd = ROUND((a.unit_selling_price * a.exchange_rate)::numeric, 2)
  FROM edw_prod_dbo.w_ar_invoice_f a
  WHERE sf.line_id = a.so_line_id
    AND a.source_system = sf.source_system
    AND a.source_system = 'OC'
    AND COALESCE(sf.unit_price_usd,0) <> ROUND((COALESCE(a.unit_selling_price,0) * COALESCE(a.exchange_rate,1))::numeric, 2);

  /* ========== Fix OC ledgers on OU dim ========== */
  UPDATE edw_prod_dbo.w_oper_unit_d d
  SET ledger_id = bu.primary_ledger_id::BIGINT
  FROM oc_prod_dbo.fun_all_business_units_v bu
  JOIN oc_prod_dbo.inv_org_parameters iop
    ON bu.bu_id = iop.businessunitid
  WHERE iop.itemdefinitionorgid ::BIGINT= d.org_id
    AND d.ledger_id IS NULL
    AND d.source_system = 'OC';

  /* ========== Fix DSO currency from item org ==========
     Part 1: pull from dso row that matches default shipping org */
 UPDATE edw_prod_dbo.w_item_org_d orig
SET dso_currency_code = dso_data.standard_cost_currency
FROM edw_prod_dbo.w_item_org_d dso_data
WHERE orig.inventory_item_id = dso_data.inventory_item_id
  AND dso_data.organization_id = orig.default_shipping_org
  AND dso_data.source_system = orig.source_system
  AND orig.default_shipping_org IS NOT NULL
  AND COALESCE(orig.dso_currency_code,'!') <> COALESCE(dso_data.standard_cost_currency,'!');

  /* Part 2: fill dso currency via ledger mapping */
 UPDATE edw_prod_dbo.w_item_org_d orig
SET dso_currency_code = l.currency_code
FROM oc_prod_dbo.inv_org_parameters p
JOIN oc_prod_dbo.fun_all_business_units_v bu
  ON bu.bu_id = p.businessunitid
JOIN edw_prod_dbo.w_gl_ledger_d l
  ON l.ledger_id = bu.primary_ledger_id::BIGINT
WHERE orig.default_shipping_org IS NOT NULL
  AND orig.dso_currency_code IS NULL
  AND orig.source_system = 'OC'
  AND p.organizationid ::BIGINT= orig.default_shipping_org
  AND l.currency_code <> COALESCE(orig.dso_currency_code,'~');


  /* Part 3: normalize standard_cost_currency across orgs (OC) */
UPDATE edw_prod_dbo.w_item_org_d c
SET standard_cost_currency = p.standard_cost_currency
FROM (
    SELECT organization_id, MAX(standard_cost_currency) AS standard_cost_currency
    FROM edw_prod_dbo.w_item_org_d
    WHERE standard_cost_currency IS NOT NULL
      AND organization_id > 0
      AND source_system = 'OC'
    GROUP BY organization_id
) p
WHERE c.source_system = 'OC'
  AND c.organization_id > 0
  AND c.organization_id = p.organization_id
  AND COALESCE(c.standard_cost_currency,'!') <> COALESCE(p.standard_cost_currency,'!');

  /* ========== Backfill org_id on AR SL from AR header ========== */
  UPDATE edw_prod_dbo.w_ar_invoice_sl_f s
  SET org_id = i.org_id
  FROM edw_prod_dbo.w_ar_invoice_f i
  WHERE s.org_id IS NULL
    AND i.org_id IS NOT NULL
    AND s.customer_trx_line_id = i.customer_trx_line_id;

  /* ========== Map EBS/OC line ids ==========
     (Assuming REF.* is now ref_dbo.*) */
  UPDATE edw_prod_dbo.w_sales_orders_d s
  SET ebs_line_id = m.ebs_line_id
  FROM edw_prod_ref.ebs_oc_line_map m
  WHERE m.cloud_line_id = s.order_line_id
    AND COALESCE(m.ebs_line_id,0) <> COALESCE(s.ebs_line_id,0);

  /* ========== Ship-to site from invoice lines (SL) ========== */
 UPDATE edw_prod_dbo.w_ar_invoice_sl_f sl
SET ship_to_site_use_id = csd.site_use_id
FROM oc_prod_dbo.ra_customer_trx_lines_all r
JOIN oc_prod_dbo.hz_party_site_uses psu
  ON r.racustomertrxlineshiptopartysiteuseid = psu.party_site_use_id
JOIN edw_prod_dbo.w_customer_site_d csd
  ON csd.party_site_id = psu.party_site_id::BIGINT
 AND psu.site_use_type = 'SHIP_TO'
 AND psu.site_use_type = csd.site_use_code
WHERE sl.source_system = 'OC'
  AND sl.source_system = csd.source_system
  AND r.racustomertrxlinecustomertrxlineid ::BIGINT= sl.customer_trx_line_id
  AND psu.party_site_use_id IS NOT NULL
  AND csd.site_use_id <> COALESCE(sl.ship_to_site_use_id, 1);



  /* ========== Ship-to site from invoice header (SL) ========== */
 UPDATE edw_prod_dbo.w_ar_invoice_sl_f sl
SET ship_to_site_use_id = csd.site_use_id
FROM oc_prod_dbo.ra_customer_trx_all r
JOIN oc_prod_dbo.hz_party_site_uses psu
  ON r.racustomertrxshiptopartysiteuseid = psu.party_site_use_id
JOIN edw_prod_dbo.w_customer_site_d csd
  ON csd.party_site_id = psu.party_site_id::BIGINT
 AND psu.site_use_type = 'SHIP_TO'
 AND psu.site_use_type = csd.site_use_code
WHERE sl.customer_trx_id = r.racustomertrxcustomertrxid::BIGINT
  AND sl.source_system = csd.source_system
  AND sl.source_system = 'OC'
  AND psu.party_site_use_id IS NOT NULL
  AND csd.site_use_id <> COALESCE(sl.ship_to_site_use_id, 1);

  /* ========== Ship-to customer on SL ========== */
  UPDATE edw_prod_dbo.w_ar_invoice_sl_f a
SET ship_to_customer_id = c.cust_account_id
FROM edw_prod_dbo.w_customer_site_d csd
JOIN edw_prod_dbo.w_customer_d c
  ON c.customer_account = csd.customer_account
 AND c.source_system = csd.source_system
WHERE a.source_system = csd.source_system
  AND a.ship_to_site_use_id = csd.site_use_id
  AND a.source_system = c.source_system
  AND a.source_system = 'OC'
  AND COALESCE(a.ship_to_customer_id, -1) <> c.cust_account_id;


  /* ========== Ship-to site from invoice lines (Header table) ========== */
  UPDATE edw_prod_dbo.w_ar_invoice_f sl
SET ship_to_site_use_id = csd.site_use_id
FROM oc_prod_dbo.ra_customer_trx_lines_all r
JOIN oc_prod_dbo.hz_party_site_uses psu
  ON r.racustomertrxlineshiptopartysiteuseid = psu.party_site_use_id
JOIN edw_prod_dbo.w_customer_site_d csd
  ON csd.party_site_id = psu.party_site_id::BIGINT
 AND psu.site_use_type = 'SHIP_TO'
 AND psu.site_use_type = csd.site_use_code
WHERE sl.source_system = 'OC'
  AND r.racustomertrxlinecustomertrxlineid ::BIGINT= sl.customer_trx_line_id
  AND sl.source_system = csd.source_system         -- moved here from JOIN
  AND psu.party_site_use_id IS NOT NULL
  AND csd.site_use_id <> COALESCE(sl.ship_to_site_use_id, 1);



  /* ========== Ship-to site from invoice header (Header table) ========== */
  UPDATE edw_prod_dbo.w_ar_invoice_f sl
SET ship_to_site_use_id = csd.site_use_id
FROM oc_prod_dbo.ra_customer_trx_all r
JOIN oc_prod_dbo.hz_party_site_uses psu
  ON r.racustomertrxshiptopartysiteuseid = psu.party_site_use_id
JOIN edw_prod_dbo.w_customer_site_d csd
  ON csd.party_site_id = psu.party_site_id::BIGINT
 AND psu.site_use_type = 'SHIP_TO'
 AND psu.site_use_type = csd.site_use_code
WHERE sl.source_system = 'OC'
  AND sl.source_system = csd.source_system
  AND r.racustomertrxcustomertrxid ::BIGINT= sl.customer_trx_id
  AND psu.party_site_use_id IS NOT NULL
  AND csd.site_use_id <> COALESCE(sl.ship_to_site_use_id, 1);


  /* ========== Ship-to customer on header table ========== */
 UPDATE edw_prod_dbo.w_ar_invoice_f a
SET ship_to_customer_id = c.cust_account_id
FROM edw_prod_dbo.w_customer_site_d csd
JOIN edw_prod_dbo.w_customer_d c
  ON c.customer_account = csd.customer_account
 AND c.source_system = csd.source_system
WHERE a.source_system = 'OC'
  AND a.source_system = csd.source_system
  AND a.ship_to_site_use_id = csd.site_use_id
  AND COALESCE(a.ship_to_customer_id, -1) <> c.cust_account_id;


  /* ========== Default ship_from_org_id for OC if missing ========== */
  UPDATE edw_prod_dbo.w_sales_orders_f
  SET ship_from_org_id = 300000003579286
  WHERE COALESCE(ship_from_org_id,0) = 0
    AND source_system = 'OC'
    AND item_id IS NOT NULL;

  /* ========== Build AR totals (TEMP TABLE replaces @table var) ========== */
  CREATE TEMP TABLE ar_trx_totals (
    so_line_id            bigint,
    trx_number            varchar(90),
    ar_acctd_amount       numeric,
    exch_rate             numeric,
    gl_date               date,
    trx_date              date,
    exch_date             date,
    invoice_currency_code varchar(30),
    customer_trx_line_id  bigint,
    quantity_ordered      numeric,
    quantity_credited     numeric,
    quantity_invoiced     numeric
  );

  INSERT INTO ar_trx_totals (so_line_id, ar_acctd_amount, exch_rate, gl_date, trx_number, exch_date, invoice_currency_code, trx_date, customer_trx_line_id)
  SELECT a.so_line_id,
         SUM(a.ar_acctd_amount)::numeric AS ar_acctd_amount,
         MAX(a.exchange_rate)::numeric    AS exch_rate,
         MAX(a.gl_date)::date             AS gl_date,
         MAX(a.trx_number)                AS trx_number,
         MAX(a.exchange_date)::date       AS exch_date,
         a.invoice_currency_code,
         MAX(a.trx_date)::date,
         a.customer_trx_line_id
  FROM edw_prod_dbo.w_ar_invoice_f   arbase
  JOIN edw_prod_dbo.w_ar_invoice_sl_f a
    ON a.customer_trx_line_id = arbase.customer_trx_line_id
  JOIN edw_prod_dbo.w_gl_account_d    g
    ON a.code_combination_id = g.code_combination_id
   AND g.source_system = a.source_system
   AND g.intercompany_code = '0000'
  WHERE a.source_system = 'OC'
    AND a.so_line_id IS NOT NULL
    AND a.line_type_code = 'LINE'
  GROUP BY a.so_line_id, a.invoice_currency_code, a.customer_trx_line_id;

  INSERT INTO ar_trx_totals (so_line_id, ar_acctd_amount, exch_rate, trx_number, exch_date, invoice_currency_code, trx_date, customer_trx_line_id)
  SELECT a.so_line_id,
         SUM(a.amount)::numeric AS ar_acctd_amount,
         MAX(a.exchange_rate)::numeric,
         MAX(a.trx_number),
         MAX(a.exchange_date)::date,
         a.invoice_currency_code,
         MAX(a.trx_date)::date,
         a.customer_trx_line_id
  FROM edw_prod_dbo.w_ar_invoice_f a
  WHERE a.source_system = 'OC'
    AND a.so_line_id IS NOT NULL
    AND a.line_type_code = 'LINE'
    AND NOT EXISTS (
      SELECT 1 FROM ar_trx_totals t1
      WHERE t1.customer_trx_line_id = a.customer_trx_line_id
    )
  GROUP BY a.so_line_id, a.invoice_currency_code, a.customer_trx_line_id;

  UPDATE ar_trx_totals t
  SET quantity_ordered  = COALESCE(a.quantity_ordered,0),
      quantity_credited = COALESCE(a.quantity_credited,0),
      quantity_invoiced = COALESCE(a.quantity_invoiced,0)
  FROM edw_prod_dbo.w_ar_invoice_f a
  WHERE t.customer_trx_line_id = a.customer_trx_line_id
    AND t.so_line_id = a.so_line_id;

  UPDATE edw_prod_dbo.w_sales_orders_d o
  SET inv_gl_date        = a.gl_date,
      inv_trx_number     = a.trx_number,
      inv_exch_date      = a.exch_date,
      inv_exch_rate      = COALESCE(a.exch_rate,1),
      inv_currency_code  = a.invoice_currency_code
  FROM ar_trx_totals a
  WHERE a.so_line_id = o.line_id
    AND o.source_system = 'OC';

  UPDATE edw_prod_dbo.w_sales_orders_f o
  SET inv_quantity_invoiced = a.quantity_invoiced,
      inv_quantity_credited = a.quantity_credited,
      inv_acctd_amount      = a.ar_acctd_amount * COALESCE(a.exch_rate,1),
      inv_acctd_amount_lc   = a.ar_acctd_amount,
      exch_rate_usd         = COALESCE(a.exch_rate,1),
      unit_price_usd        = ROUND((o.unit_price * COALESCE(a.exch_rate,1))::numeric, 2)
  FROM ar_trx_totals a
  WHERE a.so_line_id = o.line_id
    AND o.source_system = 'OC';

  /* ========== Build temp rates (TEMP TABLE replaces @tmp_rates) ========== */
  CREATE TEMP TABLE tmp_rates (
    conversion_rate numeric,
    from_currency   varchar(10),
    source_system   varchar(20)
  ) ;

  INSERT INTO tmp_rates (conversion_rate, from_currency, source_system)
  SELECT conversion_rate::numeric, from_currency, source_system
  FROM edw_prod_dbo.w_curr_exch_rates_d
  WHERE to_currency = 'USD'
    AND conversion_type = 'Corporate'
    AND conversion_date = CURRENT_DATE;

  -- ensure USD self-rate for OC
  INSERT INTO tmp_rates (conversion_rate, from_currency, source_system)
  SELECT 1, 'USD', 'OC'
  WHERE NOT EXISTS (
    SELECT 1 FROM tmp_rates r1 WHERE r1.from_currency = 'USD' AND r1.source_system = 'OC'
  );

  -- ensure USD self-rate for EBS
  INSERT INTO tmp_rates (conversion_rate, from_currency, source_system)
  SELECT 1, 'USD', 'EBS'
  WHERE NOT EXISTS (
    SELECT 1 FROM tmp_rates r1 WHERE r1.from_currency = 'USD' AND r1.source_system = 'EBS'
  );

  /* ========== Unit price USD fill using tmp_rates (no target refs in JOIN) ========== */
  UPDATE edw_prod_dbo.w_sales_orders_f
SET unit_price_usd = ROUND(
        (wso.unit_price * COALESCE(r.conversion_rate, wso.exch_rate_usd))::numeric, 2
      )
FROM edw_prod_dbo.w_sales_orders_f wso
JOIN edw_prod_dbo.w_sales_orders_d d
  ON wso.line_id = d.line_id
 AND wso.source_system = d.source_system
LEFT JOIN tmp_rates r
  ON r.from_currency = COALESCE(
        d.order_currency,
        edw_prod_dbo.edw_getoucurrency(wso.org_id::bigint, wso.source_system)
     )
 AND r.source_system = wso.source_system
WHERE edw_prod_dbo.w_sales_orders_f.line_id = wso.line_id
  AND edw_prod_dbo.w_sales_orders_f.source_system = wso.source_system
  AND d.order_status NOT LIKE 'DOO%'
  AND COALESCE(edw_prod_dbo.w_sales_orders_f.unit_price_usd,0)
      <> ROUND((wso.unit_price * COALESCE(r.conversion_rate,1))::numeric, 2);

  /* ========== Fix non-converted USD where exch_rate available ========== */
  UPDATE edw_prod_dbo.w_sales_orders_f f
  SET unit_price_usd = ROUND((unit_price * exch_rate_usd)::numeric, 2)
  WHERE COALESCE(exch_rate_usd,0) <> 0
    AND unit_price = unit_price_usd
    AND exch_rate_usd <> 1;

  /* ========== OC path: overwrite with rate when needed (JOIN-safe) ========== */
 UPDATE edw_prod_dbo.w_sales_orders_f
SET unit_price_usd = ROUND((so.unit_price * COALESCE(r.conversion_rate, 1))::numeric, 2),
    exch_rate_usd  = COALESCE(r.conversion_rate, so.exch_rate_usd)
FROM edw_prod_dbo.w_sales_orders_f so
JOIN edw_prod_dbo.w_sales_orders_d d
  ON so.line_id = d.line_id
 AND so.source_system = d.source_system
LEFT JOIN tmp_rates r
  ON r.from_currency = COALESCE(
         d.order_currency,
         edw_prod_dbo.edw_getoucurrency(so.org_id::bigint, so.source_system)::text
     )
 AND r.source_system = so.source_system
WHERE edw_prod_dbo.w_sales_orders_f.line_id = so.line_id
  AND edw_prod_dbo.w_sales_orders_f.source_system = so.source_system
  AND so.unit_price = so.unit_price_usd
  AND so.exch_rate_usd <> 1
  AND so.source_system = 'OC'
  AND so.org_id <> 300000003544714
  AND d.order_status NOT LIKE 'DOO%'
  AND so.unit_price <> 0
  AND ROUND((COALESCE(r.conversion_rate,1) * so.unit_price)::numeric, 2) 
        <> COALESCE(so.unit_price_usd,0);

  -- end log
  call edw_prod_dbo.edw_log_update_job('UpdateARSOData','C','Complete');
DROP TABLE IF EXISTS ar_trx_totals;
DROP TABLE IF EXISTS tmp_rates;

END;
$procedure$
;