-- edw_prod_dbo.bbb_om_table1_oc_invoc_v source

CREATE OR REPLACE VIEW edw_prod_dbo.bbb_om_table1_oc_invoc_v
AS SELECT concat(w_ar_trx_type_d.ar_trx_type_desc, ' - ', w_oper_unit_d.ou_code) AS "HDR Order Type name",
    w_ar_invoice_f.so_line_id,
    w_sales_orders_d.order_number AS "Order Number",
    w_sales_orders_d.line_number AS "Line Number",
    w_ar_invoice_f.invoice_line_number AS "Invoice_Line Number",
    w_sales_orders_d.shipment_number AS "Shipment Number",
    COALESCE(w_ar_invoice_f.quantity_ordered, w_ar_invoice_f.quantity_invoiced) AS "Ordered Quantity",
    w_ar_invoice_f.exchange_rate AS "Order Exchange Rate",
    w_ar_invoice_f.invoice_currency_code AS "Transaction Curr Code",
    w_sales_orders_d.customer_po AS "Cust PO Number",
    w_sales_orders_d.request_date AS "Request Date",
    COALESCE(w_sales_orders_d.ordered_item, w_product_d.item_number) AS "Ordered Item",
        CASE
            WHEN w_sales_orders_d.line_type_name::text ~~ '%Only%'::text THEN COALESCE(w_sales_orders_d.schedule_date, w_sales_orders_d.request_date)
            ELSE w_sales_orders_d.schedule_date
        END AS "Scheduled Ship Date",
    w_ar_invoice_f.quantity_credited AS "Cancelled Quantity",
    w_ar_invoice_f.quantity_invoiced AS "Shipped Quantity",
        CASE
            WHEN w_ar_invoice_f.invoice_currency_code::text = 'USD'::text THEN w_ar_invoice_f.unit_selling_price
            ELSE w_ar_invoice_f.unit_selling_price * w_ar_invoice_f.exchange_rate
        END AS "Unit Selling Price",
    w_sales_orders_d.actual_ship_date AS "Actual Ship Date",
    'Invoiced'::text AS "Line Order Type",
    COALESCE(w_sales_orders_d.line_type_name, w_ar_invoice_f.line_type_code) AS "Line Order Type Name",
    item_org_attributes.standard_cost_currency AS "Ship From Func Currency",
        CASE
            WHEN w_ar_invoice_f.invoice_currency_code::text = 'USD'::text THEN 1::double precision
            ELSE edw_prod_dbo.edw_getperavgrateusd(gl_data.gl_date::date, w_ar_invoice_f.invoice_currency_code, 'OC'::character varying, 1::double precision)
        END AS "Curr Exchange Rate",
    w_ar_invoice_f.unit_selling_price AS "Unit Selling Price LC",
        CASE w_ar_invoice_f.invoice_currency_code
            WHEN 'USD'::text THEN w_ar_invoice_f.amount
            ELSE w_ar_invoice_f.amount * edw_prod_dbo.edw_getperavgrateusd(gl_data.gl_date::date, w_ar_invoice_f.invoice_currency_code, 'OC'::character varying, 1::double precision)
        END AS "Extended Price USD",
    gl_data.acctdamount,
    COALESCE(gl_data.gl_date::date::timestamp without time zone, w_ar_invoice_f.trx_date) AS "Order Date",
    w_sales_orders_d.booked_date AS "Booked Date",
    replace(COALESCE(w_sales_orders_d.cost_of_sale_account, item_org_attributes.cost_of_sales_account)::text, '.'::text, '-'::text) AS "Cost of Sale Account",
    w_sales_orders_d.order_created_by AS created_by_name,
    COALESCE(w_organization_d.org_name, sales_orders_f.ship_from_org_id::character varying) AS "Ship From Org Name",
    w_organization_d.org_code AS "Ship From Org Code",
    COALESCE(w_customer_site_bt.customer_name, w_customer_site_bt.site_name) AS "Bill To Customer Parent",
    COALESCE(w_customer_site_bt.customer_name, w_customer_site_bt.site_name) AS "Bill To Customer Name",
    COALESCE(w_customer_site_st.customer_name, w_customer_site_st.site_name) AS "Ship To Customer Parent",
    COALESCE(w_customer_site_st.customer_name, w_customer_site_st.site_name) AS "Ship To Customer Name",
        CASE
            WHEN COALESCE(cost_look.include_cost, 'Y'::character varying)::text = 'N'::text OR upper(w_ar_trx_type_d.ar_trx_type_desc::text) ~~ '%ONLY%'::text OR upper(w_sales_orders_d.line_type_name::text) ~~ '%ONLY%'::text THEN 0::double precision
            ELSE
            CASE
                WHEN COALESCE(sales_orders_f.virtual_organization_id, '300000003579286'::bigint) = ANY (ARRAY[0::bigint, '300000003579286'::bigint]) THEN edw_prod_dbo.edw_gettotalactualcostlc(sales_orders_f.item_id::bigint, sales_orders_f.ship_from_org_id::bigint, gl_data.gl_date::date, 'OC'::character varying)
                ELSE edw_prod_dbo.edw_gettotalactualcostlc(COALESCE(sales_orders_f.virtual_item_id::numeric, sales_orders_f.item_id)::bigint, COALESCE(sales_orders_f.virtual_organization_id, sales_orders_f.ship_from_org_id::bigint), gl_data.gl_date::date, 'OC'::character varying)
            END
        END AS "Ship From Item Cost LC",
        CASE
            WHEN COALESCE(cost_look.include_cost, 'Y'::character varying)::text = 'N'::text OR upper(w_ar_trx_type_d.ar_trx_type_desc::text) ~~ '%ONLY%'::text OR upper(w_sales_orders_d.line_type_name::text) ~~ '%ONLY%'::text THEN 0::double precision
            ELSE
            CASE
                WHEN COALESCE(sales_orders_f.virtual_organization_id, '300000003579286'::bigint) = ANY (ARRAY[0::bigint, '300000003579286'::bigint]) THEN edw_prod_dbo.edw_gettotalactualcostusd(sales_orders_f.item_id::bigint, sales_orders_f.ship_from_org_id::bigint, gl_data.gl_date::date, w_organization_d.currency_code::character varying, 'OC'::character varying)
                ELSE edw_prod_dbo.edw_gettotalactualcostusd(sales_orders_f.virtual_item_id, sales_orders_f.virtual_organization_id, gl_data.gl_date::date, w_organization_d.currency_code::character varying, 'OC'::character varying)
            END
        END AS "Ship From Item Cost USD",
    w_ar_invoice_f.amount AS "Extended Price LC",
    sales_rep.partyname AS "Sales Rep",
    ''::text AS "Sales Region",
    COALESCE("substring"(slf.sales_account::text, 6, 3), sales_orders_f.product_code_node::text) AS "Product Class",
    replace(COALESCE(slf.sales_account, w_sales_orders_d.sales_account)::text, '.'::text, '-'::text) AS "Sales Account",
    COALESCE(gl_data.gl_date::date::timestamp without time zone, w_ar_invoice_f.trx_date) AS "GL Date",
    w_ar_invoice_f.invoice_currency_code AS "Invoice Curr Code",
    sales_orders_f.inv_quantity_credited AS "Quantity Credited",
    COALESCE(sales_orders_f.inv_quantity_invoiced, 0::double precision) AS "Quantity Invoiced",
    operating_units.ou_name AS "SOB Description",
    edw_prod_dbo.edw_getperavgrateusd(COALESCE(gl_data.gl_date::date::timestamp with time zone, now())::date, w_ar_invoice_f.invoice_currency_code, 'OC'::character varying, 1::double precision) AS "Invoice Period AVG Rate",
    w_product_d.category AS "Category",
    (((w_product_d.product_family::text || '.'::text) || item_org_attributes.product_line::text) || '.'::text) || item_org_attributes.product_model::text AS "Category Name",
    w_product_d.commodity_class AS "Commodity Class",
    w_product_d.item_description AS "Item Description",
    w_product_d.item_number AS "Item Name",
    replace(w_product_d.product_family::text, '_'::text, ' '::text) AS "Product Family",
    replace(w_product_d.product_line::text, '_'::text, ' '::text) AS "Product Line",
    replace(w_product_d.product_model::text, '_'::text, ' '::text) AS "Product Model",
    w_product_d.sourcing_segment AS "Sourcing Segment",
    "substring"(financial_hierarchy."LEVEL05_NAME", 7, 100) AS "Label (L5)",
    financial_hierarchy."LEVEL05_NAME" AS "Product Code & Label",
    financial_hierarchy.level01_name AS "Segment (L1)",
    financial_hierarchy.level03_name AS "Family Segment (L3)",
    financial_hierarchy.level02_name AS "Product/Segment (L2)",
    financial_hierarchy.level04_name AS "Family Group (L4)",
        CASE
            WHEN upper(item_org_attributes.description::text) ~~ 'NRC%'::text THEN 'NRC'::text
            ELSE
            CASE
                WHEN upper(item_org_attributes.product_model::text) ~~ '%COMMON%'::text THEN 'Spare'::text
                ELSE 'System'::text
            END
        END AS "Common vs System",
    w_sales_orders_d.promise_date AS "Promise Date",
    operating_units.ou_code AS "OU",
    COALESCE(btreg.region, w_customer_site_bt.region) AS "Bill To Region",
    COALESCE(streg.region, w_customer_site_st.region) AS "Ship To Region",
    w_customer_site_bt.country AS "Bill To Country",
    w_customer_site_st.country AS "Ship To Country",
    w_customer_site_bt.customer_class_code AS "Bill To Class Code",
    w_customer_site_st.customer_class_code AS "Ship To Class Code",
    btreg."Mgmt Continent" AS "Bill To Mgmt Continent",
    streg."Mgmt Continent" AS "Ship To Mgmt Continent",
    w_ar_invoice_f.trx_number AS "Invoice TRX Number",
        CASE
            WHEN COALESCE(cost_look.include_cost, 'Y'::character varying)::text = 'N'::text OR upper(w_ar_trx_type_d.ar_trx_type_desc::text) ~~ '%ONLY%'::text OR upper(w_sales_orders_d.line_type_name::text) ~~ '%ONLY%'::text THEN 0::double precision
            ELSE
            CASE
                WHEN COALESCE(sales_orders_f.virtual_organization_id, '300000003579286'::bigint) = ANY (ARRAY[0::bigint, '300000003579286'::bigint]) THEN edw_prod_dbo.edw_gettotalactualcostlc(sales_orders_f.item_id::bigint, sales_orders_f.ship_from_org_id::bigint, gl_data.gl_date::date, 'OC'::character varying) * COALESCE(sales_orders_f.qty_ordered, 0::double precision)
                ELSE edw_prod_dbo.edw_gettotalactualcostlc(COALESCE(sales_orders_f.virtual_item_id::numeric, sales_orders_f.item_id)::bigint, COALESCE(sales_orders_f.virtual_organization_id::numeric, sales_orders_f.ship_from_org_id)::bigint, gl_data.gl_date::date, 'OC'::character varying) * COALESCE(sales_orders_f.qty_ordered, 0::double precision)
            END
        END AS "Ext Ship From Item Cost LC",
        CASE
            WHEN COALESCE(cost_look.include_cost, 'Y'::character varying)::text = 'N'::text OR upper(w_ar_trx_type_d.ar_trx_type_desc::text) ~~ '%ONLY%'::text OR upper(w_sales_orders_d.line_type_name::text) ~~ '%ONLY%'::text THEN 0::double precision
            ELSE
            CASE
                WHEN COALESCE(sales_orders_f.virtual_organization_id, '300000003579286'::bigint) = ANY (ARRAY[0::bigint, '300000003579286'::bigint]) THEN edw_prod_dbo.edw_gettotalactualcostusd(sales_orders_f.item_id::bigint, sales_orders_f.ship_from_org_id::bigint, gl_data.gl_date::date, w_organization_d.currency_code::character varying, 'OC'::character varying) * COALESCE(sales_orders_f.qty_ordered, 0::double precision)
                ELSE edw_prod_dbo.edw_gettotalactualcostusd(sales_orders_f.item_id::bigint, sales_orders_f.ship_from_org_id::bigint, gl_data.gl_date::date, w_organization_d.currency_code::character varying, 'OC'::character varying) * COALESCE(sales_orders_f.qty_ordered, 0::double precision)
            END
        END AS "Ext Ship From Item Cost USD",
    TRIM(BOTH FROM COALESCE(COALESCE(mc.mgmt_parent, mo.top_level_customer_name), w_customer_site_bt.customer_name)) AS "MGMT Customer",
    TRIM(BOTH FROM replace(COALESCE(COALESCE(COALESCE(mc.mgmt_region, mo.management_region), w_customer_site_st.region), w_customer_site_bt.region)::text, 'ASIA-'::text, ''::text)) AS "MGMT Region",
    'BackBill-1'::text AS "Data_Type",
    'OC Invoice'::text AS source_system,
    sales_orders_f.line_id,
    ar_terms.ar_term_desc AS payment_terms,
    w_sales_orders_d.order_status,
    w_sales_orders_d.line_status,
    w_sales_orders_d.fulfill_line_status,
    w_sales_orders_d.order_creation_date,
    w_sales_orders_d.line_creation_date,
    w_sales_orders_d.last_update_date,
    COALESCE(w_customer_site_st.primary_market_segment, w_customer_site_bt.primary_market_segment) AS primary_market_segment,
    COALESCE(w_customer_site_st.secondary_market_segment, w_customer_site_bt.secondary_market_segment) AS secondary_market_segment,
    w_ar_invoice_f.exchange_date AS "INV EXCHANGE_DATE",
    w_ar_invoice_f.exchange_rate AS "INV EXCHANGE_RATE",
    w_ar_invoice_f.invoice_currency_code AS "INV INVOICE_CURRENCY_CODE",
    w_ar_invoice_f.unit_standard_price AS "INV UNIT_STANDARD_PRICE",
    w_ar_invoice_f.unit_selling_price AS "INV UNIT_SELLING_PRICE",
    w_ar_invoice_f.quantity_ordered AS "INV QUANTITY_ORDERED",
    w_ar_invoice_f.quantity_credited AS "INV QUANTITY_CREDITED",
    w_ar_invoice_f.quantity_invoiced AS "INV QUANTITY_INVOICED",
    w_ar_invoice_f.quantity_invoiced * w_ar_invoice_f.unit_selling_price AS inv_invoice_amount,
    w_sales_orders_d.fulfillment_line_number,
    w_sales_orders_d.line_source_number,
    COALESCE(gl_data.gl_date::date::timestamp without time zone, w_ar_invoice_f.trx_date) AS gl_date,
    gl_data.gl_posted_date,
    w_ar_invoice_f.customer_trx_line_id,
    sales_orders_f.ship_to_org_id,
    w_ar_invoice_f.bill_to_site_use_id,
    sales_orders_f.salesrep_id,
    pcm.new_product_class,
    sales_orders_f.product_code_node,
    "left"(replace(w_ar_trx_type_d.ar_trx_type_desc::text, 'Repairs'::text, 'Repair'::text), 15) AS "Order Type Desc",
    "left"(pcm.order_type::text, 15) AS pcm_order_type,
    w_sales_orders_d.life_cycle_status,
    w_sales_orders_d.sales_account AS "Sales Account Ord",
    sales_orders_f.ship_from_org_id,
    sales_orders_f.item_id,
    w_ar_invoice_f.invoice_currency_code,
    sales_orders_f.ship_from_org_id AS sfo,
    sales_orders_f.org_id,
    edw_prod_dbo.edw_getoucurrency(sales_orders_f.org_id::bigint, 'OC'::character varying) AS org_cd,
    w_oper_unit_d.ou_name,
    edw_prod_dbo.edw_gettotalactualcostusd(sales_orders_f.item_id::bigint, sales_orders_f.ship_from_org_id::bigint, gl_data.gl_date::date, w_organization_d.currency_code, 'OC'::character varying) AS totalactualcostusd,
    c.total_distribution_cost_lc,
    c.mfg_frozen_cost_at_per_avg_in_usd,
    c.mfg_frozen_cost_at_planned_in_usd,
    c.ship_frozen_cost_at_per_avg_in_usd
   FROM edw_prod_dbo.w_ar_invoice_f w_ar_invoice_f
     LEFT JOIN edw_prod_dbo.bbb_get_ar_invoice_gl_v gl_data ON gl_data.racusttrxlinegldistcustomertrxlineid::numeric = w_ar_invoice_f.customer_trx_line_id
     LEFT JOIN edw_prod_dbo.w_oper_unit_d w_oper_unit_d ON w_ar_invoice_f.org_id = w_oper_unit_d.org_id AND w_ar_invoice_f.source_system::text = w_oper_unit_d.source_system::text
     LEFT JOIN edw_prod_dbo.w_ar_trx_type_d w_ar_trx_type_d ON w_ar_invoice_f.cust_trx_type_id = w_ar_trx_type_d.cust_trx_type_id AND w_ar_invoice_f.source_system::text = w_ar_trx_type_d.source_system::text
     LEFT JOIN edw_prod_dbo.w_sales_orders_d w_sales_orders_d ON w_ar_invoice_f.so_line_id = w_sales_orders_d.line_id AND w_ar_invoice_f.source_system::text = w_sales_orders_d.source_system::text
     LEFT JOIN edw_prod_dbo.w_sales_orders_d ebs_invoice ON ebs_invoice.line_id = w_sales_orders_d.line_id AND ebs_invoice.source_system::text = 'EBS'::text
     LEFT JOIN edw_prod_dbo.w_sales_orders_f sales_orders_f ON sales_orders_f.line_id = w_sales_orders_d.line_id AND sales_orders_f.source_system::text = w_sales_orders_d.source_system::text
     LEFT JOIN edw_prod_dbo.w_organization_d w_organization_d ON sales_orders_f.ship_from_org_id = w_organization_d.src_org_id AND w_organization_d.source_system::text = 'OC'::text
     LEFT JOIN edw_prod_dbo.w_product_d w_product_d ON w_ar_invoice_f.inventory_item_id = w_product_d.inventory_item_id AND w_product_d.source_system::text = 'OC'::text
     LEFT JOIN edw_prod_dbo.w_customer_site_d w_customer_site_bt ON w_ar_invoice_f.bill_to_site_use_id = w_customer_site_bt.site_use_id AND w_customer_site_bt.site_use_code::text = 'BILL_TO'::text AND w_customer_site_bt.source_system::text = 'OC'::text
     LEFT JOIN edw_prod_dbo.w_customer_site_d w_customer_site_st ON COALESCE(w_ar_invoice_f.ship_to_site_use_id, sales_orders_f.ship_to_org_id) = w_customer_site_st.site_use_id AND w_customer_site_st.site_use_code::text = 'SHIP_TO'::text AND w_customer_site_st.source_system::text = 'OC'::text
     LEFT JOIN edw_prod_dbo.w_item_org_d item_org_attributes ON w_ar_invoice_f.inventory_item_id = item_org_attributes.inventory_item_id AND item_org_attributes.organization_code::text = 'BIM'::text AND item_org_attributes.source_system::text = 'OC'::text
     LEFT JOIN edw_prod_dbo."BBB_Get_GL_Code_Invoice_SL_F_V2" slf ON slf.invoice_line_number = w_ar_invoice_f.invoice_line_number AND slf.trx_number::text = w_ar_invoice_f.trx_number::text
     LEFT JOIN oc_prod_dbo.jtf_rs_salesreps sales_rep ON sales_orders_f.salesrep_id = sales_rep.resourceid::numeric AND sales_rep.resourcesalesreppeostatus::text = 'A'::text
     LEFT JOIN edw_prod_dbo.w_oper_unit_d operating_units ON w_ar_invoice_f.org_id = operating_units.org_id AND operating_units.source_system::text = 'OC'::text
     LEFT JOIN edw_prod_dbo."W_PRODUCT_DH_V" financial_hierarchy ON financial_hierarchy.node::text = COALESCE("substring"(slf.sales_account::text, 6, 3), "substring"(w_sales_orders_d.sales_account::text, 6, 3))
     LEFT JOIN edw_prod_dbo.mgmt_cust_pn_lookup mc ON mc."Bill To Customer Name"::text = w_customer_site_bt.customer_name::text AND mc."Brooks PN"::text = item_org_attributes.item_number::text AND mc."Customer PN"::text = w_sales_orders_d.ordered_item::text
     LEFT JOIN edw_prod_dbo.w_ar_terms_d ar_terms ON sales_orders_f.payment_term_id = ar_terms.terms_id AND sales_orders_f.source_system::text = ar_terms.source_system::text
     LEFT JOIN edw_prod_dbo.management_override_new mo ON mo.customer_name::text = w_customer_site_bt.customer_name::text
     LEFT JOIN stage_ref.bi_organization_names org ON org.organization_code::character varying(3)::text = w_organization_d.org_code::character varying(3)::text
     LEFT JOIN stage_ref.product_class_map pcm ON "left"(pcm.order_type::text, 15) = "left"(replace(w_ar_trx_type_d.ar_trx_type_desc::text, 'Repairs'::text, 'Repair'::text), 15) AND pcm.current_product_class = sales_orders_f.product_code_node::double precision
     LEFT JOIN edw_prod_dbo.country_region streg ON streg."Country Code"::text = w_customer_site_st.country::text
     LEFT JOIN edw_prod_dbo.country_region btreg ON btreg."Country Code"::text = w_customer_site_bt.country::text
     LEFT JOIN edw_prod_dbo.bbb_include_cost_lookup cost_look ON cost_look.lookup_value::text = (("left"(w_ar_trx_type_d.ar_trx_type_desc::text, 10) || '-'::text) || "left"(w_sales_orders_d.line_type_name::text, 10))
     LEFT JOIN ( SELECT c_1.customer_trx_line_id,
            sum(c_1.total_distribution_cost) AS total_distribution_cost_lc,
            sum(c_1.mfg_frozen_cost_at_per_avg_in_usd) AS mfg_frozen_cost_at_per_avg_in_usd,
            sum(c_1.mfg_frozen_cost_at_planned_in_usd) AS mfg_frozen_cost_at_planned_in_usd,
            0 AS ship_frozen_cost_at_per_avg_in_usd
           FROM edw_prod_dbo.w_ar_combined_cost c_1
          WHERE c_1.source_system::text = 'OC'::text AND "right"(c_1.reference_account::text, 4) = '0000'::text
          GROUP BY c_1.customer_trx_line_id) c ON w_ar_invoice_f.customer_trx_line_id = c.customer_trx_line_id::numeric
  WHERE 1 = 1 AND w_ar_invoice_f.source_system::text <> 'EBS'::text AND w_ar_invoice_f.trx_date > '2024-07-31 00:00:00'::timestamp without time zone AND (w_ar_invoice_f.trx_date > '2024-07-31 00:00:00'::timestamp without time zone AND ("left"(replace(COALESCE(slf.sales_account, w_sales_orders_d.sales_account)::text, '.'::text, '-'::text), 4) = ANY (ARRAY['0100'::text, '0235'::text, '0240'::text, '0219'::text])) OR w_ar_invoice_f.trx_date > '2025-10-08 00:00:00'::timestamp without time zone AND ("left"(replace(COALESCE(slf.sales_account, w_sales_orders_d.sales_account)::text, '.'::text, '-'::text), 4) <> ALL (ARRAY['0100'::text, '0235'::text, '0240'::text, '0219'::text]))) AND w_ar_invoice_f.line_type_code::text <> 'TAX'::text AND w_ar_trx_type_d.ar_trx_type_desc::text !~~ 'Subscription%'::text;