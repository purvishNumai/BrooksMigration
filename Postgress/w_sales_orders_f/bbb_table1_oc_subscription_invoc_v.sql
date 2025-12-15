-- edw_prod_dbo.bbb_table1_oc_subscription_invoc_v source

CREATE OR REPLACE VIEW edw_prod_dbo.bbb_table1_oc_subscription_invoc_v
AS SELECT ("W_AR_TRX_TYPE_D".ar_trx_type_desc::text || ' - '::text) || "W_OPER_UNIT_D".ou_code::text AS "HDR Order Type name",
    "W_AR_INVOICE_F".so_line_id,
    "W_SALES_ORDERS_D".order_number AS "Order Number",
    "W_SALES_ORDERS_D".line_number AS "Line Number",
    "W_AR_INVOICE_F".invoice_line_number AS "Invoice_Line Number",
    "W_SALES_ORDERS_D".shipment_number AS "Shipment Number",
    COALESCE("W_AR_INVOICE_F".quantity_ordered, "W_AR_INVOICE_F".quantity_invoiced) AS "Ordered Quantity",
    "W_AR_INVOICE_F".exchange_rate AS "Order Exchange Rate",
    "W_AR_INVOICE_F".invoice_currency_code AS "Transaction Curr Code",
    "W_SALES_ORDERS_D".customer_po AS "Cust PO Number",
    "W_SALES_ORDERS_D".request_date AS "Request Date",
    COALESCE("W_SALES_ORDERS_D".ordered_item, "W_PRODUCT_D".item_number) AS "Ordered Item",
        CASE
            WHEN "W_SALES_ORDERS_D".line_type_name::text ~~ '%Only%'::text THEN COALESCE("W_SALES_ORDERS_D".schedule_date, "W_SALES_ORDERS_D".request_date)
            ELSE "W_SALES_ORDERS_D".schedule_date
        END AS "Scheduled Ship Date",
    "W_AR_INVOICE_F".quantity_credited AS "Cancelled Quantity",
    "W_AR_INVOICE_F".quantity_invoiced AS "Shipped Quantity",
        CASE
            WHEN "W_AR_INVOICE_F".invoice_currency_code::text = 'USD'::text THEN "W_AR_INVOICE_F".unit_selling_price
            ELSE "W_AR_INVOICE_F".unit_selling_price * "W_AR_INVOICE_F".exchange_rate
        END AS "Unit Selling Price",
    "W_SALES_ORDERS_D".actual_ship_date AS "Actual Ship Date",
    'Invoiced'::text AS "Line Order Type",
    COALESCE("W_SALES_ORDERS_D".line_type_name, "W_AR_INVOICE_F".line_type_code) AS "Line Order Type Name",
    "Item-Org Attributes".standard_cost_currency AS "Ship From Func Currency",
        CASE
            WHEN "W_AR_INVOICE_F".invoice_currency_code::text = 'USD'::text THEN 1::double precision
            ELSE edw_prod_dbo.edw_getperavgrateusd("W_AR_INVOICE_F".trx_date::date, "W_AR_INVOICE_F".invoice_currency_code, 'OC'::character varying, 1::double precision)
        END AS "Curr Exchange Rate",
    "W_AR_INVOICE_F".unit_selling_price AS "Unit Selling Price LC",
        CASE
            WHEN "W_AR_INVOICE_F".invoice_currency_code::text = 'USD'::text THEN "W_AR_INVOICE_F".amount
            ELSE "W_AR_INVOICE_F".amount * edw_prod_dbo.edw_getperavgrateusd("W_AR_INVOICE_F".trx_date::date, "W_AR_INVOICE_F".invoice_currency_code, 'OC'::character varying, 1::double precision)
        END AS "Extended Price USD",
    "GL_DATA".acctdamount,
    COALESCE("GL_DATA".gl_date::timestamp without time zone, "W_AR_INVOICE_F".trx_date)::date AS "Order Date",
    "W_SALES_ORDERS_D".booked_date AS "Booked Date",
    replace(COALESCE("W_SALES_ORDERS_D".cost_of_sale_account, "Item-Org Attributes".cost_of_sales_account)::text, '.'::text, '-'::text) AS "Cost of Sale Account",
    "W_SALES_ORDERS_D".order_created_by AS created_by_name,
    COALESCE("W_ORGANIZATION_D".org_name, "Sales Orders-F".ship_from_org_id::character varying) AS "Ship From Org Name",
    "W_ORGANIZATION_D".org_code AS "Ship From Org Code",
    COALESCE("W_CUSTOMER_SITE_BT".customer_name, "W_CUSTOMER_SITE_BT".site_name) AS "Bill To Customer Parent",
    COALESCE("W_CUSTOMER_SITE_BT".customer_name, "W_CUSTOMER_SITE_BT".site_name) AS "Bill To Customer Name",
    COALESCE("W_CUSTOMER_SITE_ST".customer_name, "W_CUSTOMER_SITE_ST".site_name) AS "Ship To Customer Parent",
    COALESCE("W_CUSTOMER_SITE_ST".customer_name, "W_CUSTOMER_SITE_ST".site_name) AS "Ship To Customer Name",
        CASE
            WHEN upper("W_SALES_ORDERS_D".line_type_name::text) ~~ '%ONLY%'::text THEN 0::double precision
            ELSE edw_prod_dbo.edw_gettotalactualcostlc("Sales Orders-F".item_id::bigint, "Sales Orders-F".ship_from_org_id::bigint, "GL_DATA".gl_date::date, 'OC'::character varying)
        END AS "Ship From Item Cost LC",
        CASE
            WHEN upper("W_SALES_ORDERS_D".line_type_name::text) ~~ '%ONLY%'::text THEN 0::double precision
            ELSE edw_prod_dbo.edw_gettotalactualcostusd("Sales Orders-F".item_id::bigint, "Sales Orders-F".ship_from_org_id::bigint, "GL_DATA".gl_date::date, "W_ORGANIZATION_D".currency_code, 'OC'::character varying)
        END AS "Ship From Item Cost USD",
    "W_AR_INVOICE_F".amount AS "Extended Price LC",
    "Sales Rep".partyname AS "Sales Rep",
    ''::text AS "Sales Region",
    COALESCE(SUBSTRING(slf.sales_account FROM 6 FOR 3), SUBSTRING("Sales Orders-F".product_code_node FROM 6 FOR 3)) AS "Product Class",
    replace(COALESCE(slf.sales_account, "W_SALES_ORDERS_D".sales_account)::text, '.'::text, '-'::text) AS "Sales Account",
    COALESCE("GL_DATA".gl_date::timestamp without time zone, "W_AR_INVOICE_F".trx_date::date::timestamp without time zone) AS "GL Date",
    "W_AR_INVOICE_F".invoice_currency_code AS "Invoice Curr Code",
    "Sales Orders-F".inv_quantity_credited AS "Quantity Credited",
    COALESCE("Sales Orders-F".inv_quantity_invoiced, 0::double precision) AS "Quantity Invoiced",
    "Operating Units".ou_name AS "SOB Description",
    edw_prod_dbo.edw_getperavgrateusd(COALESCE("W_AR_INVOICE_F".trx_date::date, CURRENT_DATE), "W_AR_INVOICE_F".invoice_currency_code::character varying, 'OC'::character varying, 1::double precision) AS "Invoice Period AVG Rate",
    "W_PRODUCT_D".category AS "Category",
    ((("W_PRODUCT_D".product_family::text || '.'::text) || "Item-Org Attributes".product_line::text) || '.'::text) || "Item-Org Attributes".product_model::text AS "Category Name",
    "W_PRODUCT_D".commodity_class AS "Commodity Class",
    "W_PRODUCT_D".item_description AS "Item Description",
    "W_PRODUCT_D".item_number AS "Item Name",
    replace("W_PRODUCT_D".product_family::text, '_'::text, ' '::text) AS "Product Family",
    replace("W_PRODUCT_D".product_line::text, '_'::text, ' '::text) AS "Product Line",
    replace("W_PRODUCT_D".product_model::text, '_'::text, ' '::text) AS "Product Model",
    "W_PRODUCT_D".sourcing_segment AS "Sourcing Segment",
    SUBSTRING("Financial Hierarchy"."LEVEL05_NAME" FROM 7 FOR 100) AS "Label (L5)",
    "Financial Hierarchy"."LEVEL05_NAME" AS "Product Code & Label",
    "Financial Hierarchy".level01_name AS "Segment (L1)",
    "Financial Hierarchy".level03_name AS "Family Segment (L3)",
    "Financial Hierarchy".level02_name AS "Product/Segment (L2)",
    "Financial Hierarchy".level04_name AS "Family Group (L4)",
        CASE
            WHEN upper("Item-Org Attributes".product_model::text) ~~ '%COMMON%'::text THEN 'Spare'::text
            ELSE 'System'::text
        END AS "Common vs System",
    "W_SALES_ORDERS_D".promise_date AS "Promise Date",
    "Operating Units".ou_code AS "OU",
    COALESCE(btreg.region, "W_CUSTOMER_SITE_BT".region) AS "Bill To Region",
    COALESCE(streg.region, "W_CUSTOMER_SITE_ST".region) AS "Ship To Region",
    "W_CUSTOMER_SITE_BT".country AS "Bill To Country",
    "W_CUSTOMER_SITE_ST".country AS "Ship To Country",
    "W_CUSTOMER_SITE_ST".customer_class_code AS "Bill To Class Code",
    "W_CUSTOMER_SITE_BT".customer_class_code AS "Ship To Class Code",
    "W_AR_INVOICE_F".trx_number AS "Invoice TRX Number",
    edw_prod_dbo.edw_gettotalactualcostlc("Sales Orders-F".item_id::bigint, "Sales Orders-F".ship_from_org_id::bigint, "GL_DATA".gl_date::date, 'OC'::character varying) * "Sales Orders-F".inv_quantity_invoiced AS "Ext Ship From Item Cost LC",
    edw_prod_dbo.edw_gettotalactualcostusd("Sales Orders-F".item_id::bigint, "Sales Orders-F".ship_from_org_id::bigint, "GL_DATA".gl_date::date, "W_ORGANIZATION_D".currency_code, 'OC'::character varying) * "Sales Orders-F".inv_quantity_invoiced AS "Ext Ship From Item Cost USD",
    COALESCE(COALESCE(mc.mgmt_parent, mo.top_level_customer_name), "W_CUSTOMER_SITE_BT".customer_name) AS "MGMT Customer",
    TRIM(BOTH FROM replace(COALESCE(COALESCE(mc.mgmt_region, mo.management_region), "W_CUSTOMER_SITE_BT".region)::text, 'ASIA-'::text, ''::text)) AS "MGMT Region",
    'BackBill-1'::text AS "Data_Type",
    'OC Invoice'::text AS source_system,
    "Sales Orders-F".line_id,
    "AR_TERMS".ar_term_desc AS payment_terms,
    "W_SALES_ORDERS_D".order_status,
    "W_SALES_ORDERS_D".line_status,
    "W_SALES_ORDERS_D".fulfill_line_status,
    "W_SALES_ORDERS_D".order_creation_date,
    "W_SALES_ORDERS_D".line_creation_date,
    "W_SALES_ORDERS_D".last_update_date,
    COALESCE("W_CUSTOMER_SITE_ST".primary_market_segment, "W_CUSTOMER_SITE_BT".primary_market_segment) AS primary_market_segment,
    COALESCE("W_CUSTOMER_SITE_ST".secondary_market_segment, "W_CUSTOMER_SITE_BT".secondary_market_segment) AS secondary_market_segment,
    "W_AR_INVOICE_F".exchange_date AS "INV EXCHANGE_DATE",
    "W_AR_INVOICE_F".exchange_rate AS "INV EXCHANGE_RATE",
    "W_AR_INVOICE_F".invoice_currency_code AS "INV INVOICE_CURRENCY_CODE",
    "W_AR_INVOICE_F".unit_standard_price AS "INV UNIT_STANDARD_PRICE",
    "W_AR_INVOICE_F".unit_selling_price AS "INV UNIT_SELLING_PRICE",
    "W_AR_INVOICE_F".quantity_ordered AS "INV QUANTITY_ORDERED",
    "W_AR_INVOICE_F".quantity_credited AS "INV QUANTITY_CREDITED",
    "W_AR_INVOICE_F".quantity_invoiced AS "INV QUANTITY_INVOICED",
    "W_AR_INVOICE_F".quantity_invoiced * "W_AR_INVOICE_F".unit_selling_price AS inv_invoice_amount,
    "W_SALES_ORDERS_D".fulfillment_line_number,
    "W_SALES_ORDERS_D".line_source_number,
    COALESCE("GL_DATA".gl_date::timestamp without time zone, "W_AR_INVOICE_F".trx_date)::date AS gl_date,
    "GL_DATA".gl_posted_date::date AS gl_posted_date,
    "W_AR_INVOICE_F".customer_trx_line_id,
    "Sales Orders-F".ship_to_org_id,
    "W_AR_INVOICE_F".bill_to_site_use_id,
    "Sales Orders-F".salesrep_id,
    "PCM".new_product_class,
    "Sales Orders-F".product_code_node,
    "left"(replace("W_AR_TRX_TYPE_D".ar_trx_type_desc::text, 'Repairs'::text, 'Repair'::text), 15) AS "Order Type Desc",
    "left"("PCM".order_type::text, 15) AS pcm_order_type,
    "W_SALES_ORDERS_D".life_cycle_status,
    "W_SALES_ORDERS_D".sales_account AS "Sales Account Ord",
    cost.mfg_frozen_cost_at_per_avg_in_usd,
    "Sales Orders-F".ship_from_org_id,
    "Sales Orders-F".item_id,
    "W_AR_INVOICE_F".invoice_currency_code,
    "Sales Orders-F".ship_from_org_id AS sfo,
    "Sales Orders-F".org_id,
    edw_prod_dbo.edw_getoucurrency("Sales Orders-F".org_id::bigint, 'oc'::character varying) AS org_cd,
    "W_OPER_UNIT_D".ou_name,
    edw_prod_dbo.edw_gettotalactualcostusd("Sales Orders-F".item_id::bigint, "Sales Orders-F".ship_from_org_id::bigint, "GL_DATA".gl_date::date, "W_ORGANIZATION_D".currency_code::character varying, 'OC'::character varying) AS totalactualcostusd,
    cost.total_distribution_cost,
    btreg."Mgmt Continent" AS "Bill To Mgmt Continent",
    streg."Mgmt Continent" AS "Ship To Mgmt Continent"
   FROM edw_prod_dbo.w_ar_invoice_f "W_AR_INVOICE_F"
     LEFT JOIN edw_prod_dbo.bbb_get_ar_invoice_gl_v "GL_DATA" ON "GL_DATA".racusttrxlinegldistcustomertrxlineid::bigint::numeric = "W_AR_INVOICE_F".customer_trx_line_id
     LEFT JOIN edw_prod_dbo.w_oper_unit_d "W_OPER_UNIT_D" ON "W_AR_INVOICE_F".org_id = "W_OPER_UNIT_D".org_id AND "W_AR_INVOICE_F".source_system::text = "W_OPER_UNIT_D".source_system::text
     LEFT JOIN edw_prod_dbo.w_ar_trx_type_d "W_AR_TRX_TYPE_D" ON "W_AR_INVOICE_F".cust_trx_type_id = "W_AR_TRX_TYPE_D".cust_trx_type_id AND "W_AR_INVOICE_F".source_system::text = "W_AR_TRX_TYPE_D".source_system::text
     LEFT JOIN edw_prod_dbo.w_sales_orders_d "W_SALES_ORDERS_D" ON "W_AR_INVOICE_F".so_line_id = "W_SALES_ORDERS_D".line_id AND "W_AR_INVOICE_F".source_system::text = "W_SALES_ORDERS_D".source_system::text
     LEFT JOIN edw_prod_dbo.w_sales_orders_d "EBS Invoice" ON "EBS Invoice".line_id = "W_SALES_ORDERS_D".line_id AND "EBS Invoice".source_system::text = 'EBS'::text
     LEFT JOIN edw_prod_dbo.w_sales_orders_f "Sales Orders-F" ON "Sales Orders-F".line_id = "W_SALES_ORDERS_D".line_id AND "Sales Orders-F".source_system::text = "W_SALES_ORDERS_D".source_system::text
     LEFT JOIN edw_prod_dbo.w_organization_d "W_ORGANIZATION_D" ON "Sales Orders-F".ship_from_org_id = "W_ORGANIZATION_D".src_org_id AND "W_ORGANIZATION_D".source_system::text = 'OC'::text
     LEFT JOIN edw_prod_dbo.w_product_d "W_PRODUCT_D" ON "W_AR_INVOICE_F".inventory_item_id = "W_PRODUCT_D".inventory_item_id AND "W_PRODUCT_D".source_system::text = 'OC'::text
     LEFT JOIN edw_prod_dbo.w_customer_site_d "W_CUSTOMER_SITE_BT" ON "W_AR_INVOICE_F".bill_to_site_use_id = "W_CUSTOMER_SITE_BT".site_use_id AND "W_CUSTOMER_SITE_BT".site_use_code::text = 'BILL_TO'::text AND "W_CUSTOMER_SITE_BT".source_system::text = 'OC'::text
     LEFT JOIN edw_prod_dbo.w_customer_site_d "W_CUSTOMER_SITE_ST" ON COALESCE("W_AR_INVOICE_F".ship_to_site_use_id, "Sales Orders-F".ship_to_org_id) = "W_CUSTOMER_SITE_ST".site_use_id AND "W_CUSTOMER_SITE_ST".site_use_code::text = 'SHIP_TO'::text AND "W_CUSTOMER_SITE_ST".source_system::text = 'OC'::text
     LEFT JOIN edw_prod_dbo.w_item_org_d "Item-Org Attributes" ON "W_AR_INVOICE_F".inventory_item_id = "Item-Org Attributes".inventory_item_id AND "Item-Org Attributes".organization_code::text = 'BIM'::text AND "Item-Org Attributes".source_system::text = 'OC'::text
     LEFT JOIN edw_prod_fin_dev.bbb_get_gl_code_subscription_sl_f_v2 slf ON slf.invoice_line_number = "W_AR_INVOICE_F".invoice_line_number AND slf.trx_number::text = "W_AR_INVOICE_F".trx_number::text
     LEFT JOIN oc_prod_dbo.jtf_rs_salesreps "Sales Rep" ON "Sales Orders-F".salesrep_id = "Sales Rep".resourceid::bigint::numeric AND "Sales Rep".resourcesalesreppeostatus::text = 'A'::text
     LEFT JOIN edw_prod_dbo.w_oper_unit_d "Operating Units" ON "W_AR_INVOICE_F".org_id = "Operating Units".org_id AND "Operating Units".source_system::text = 'OC'::text
     LEFT JOIN edw_prod_dbo."W_PRODUCT_DH_V" "Financial Hierarchy" ON "Financial Hierarchy".node::text = COALESCE("substring"(slf.sales_account::text, 6, 3), "substring"("W_SALES_ORDERS_D".sales_account::text, 6, 3))
     LEFT JOIN edw_prod_dbo.mgmt_cust_pn_lookup mc ON mc."Bill To Customer Name"::text = "W_CUSTOMER_SITE_ST".customer_name::text AND mc."Brooks PN"::text = "Item-Org Attributes".item_number::text AND mc."Customer PN"::text = "W_SALES_ORDERS_D".ordered_item::text
     LEFT JOIN edw_prod_dbo.w_ar_terms_d "AR_TERMS" ON "Sales Orders-F".payment_term_id = "AR_TERMS".terms_id AND "Sales Orders-F".source_system::text = "AR_TERMS".source_system::text
     LEFT JOIN edw_prod_dbo.management_override_new mo ON mo.top_level_customer_name::text = "W_CUSTOMER_SITE_ST".customer_name::text
     LEFT JOIN stage_ref.bi_organization_names org ON org.organization_code::character varying(3)::text = "W_ORGANIZATION_D".org_code::character varying(3)::text
     LEFT JOIN edw_prod_ref.product_class_map "PCM" ON "left"("PCM".order_type::text, 15) = "left"(replace("W_AR_TRX_TYPE_D".ar_trx_type_desc::text, 'Repairs'::text, 'Repair'::text), 15) AND "PCM".current_product_class = "Sales Orders-F".product_code_node::double precision
     LEFT JOIN edw_prod_fin_dev.country_region streg ON streg."Country Code"::text = "W_CUSTOMER_SITE_ST".country::text
     LEFT JOIN edw_prod_fin_dev.country_region btreg ON btreg."Country Code"::text = "W_CUSTOMER_SITE_BT".country::text
     LEFT JOIN edw_prod_dbo.w_ar_combined_cost cost ON cost.so_line_id = "Sales Orders-F".line_id::double precision AND "W_AR_INVOICE_F".customer_trx_line_id = cost.customer_trx_line_id::numeric AND cost.source_system::text = 'OC'::text
  WHERE 1 = 1 AND "W_AR_INVOICE_F".source_system::text <> 'EBS'::text AND "W_AR_INVOICE_F".trx_date > '2024-07-31'::date AND "W_AR_INVOICE_F".line_type_code::text <> 'TAX'::text AND "W_AR_TRX_TYPE_D".ar_trx_type_desc::text ~~ 'Subscription%'::text;