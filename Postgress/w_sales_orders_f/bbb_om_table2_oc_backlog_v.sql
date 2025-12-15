-- edw_prod_dbo.bbb_om_table2_oc_backlog_v source

CREATE OR REPLACE VIEW edw_prod_dbo.bbb_om_table2_oc_backlog_v
AS SELECT "Order Attributes".order_header_type AS "Hdr Order Type Name",
    "Order Attributes".order_number AS "Order Number",
    "Order Attributes".line_number AS "Line Number",
    "Order Attributes".shipment_number AS "Shipment Number",
        CASE
            WHEN (COALESCE("Sales Orders-F".inv_quantity_invoiced, 0::double precision) + COALESCE("Sales Orders-F".inv_quantity_credited, 0::double precision)) <> 0::double precision THEN COALESCE("Sales Orders-F".inv_quantity_invoiced, 0::double precision) + COALESCE("Sales Orders-F".inv_quantity_credited, 0::double precision)
            ELSE "Sales Orders-F".qty_ordered
        END AS "Ordered Quantity",
    "Sales Orders-F".exch_rate_usd AS "Order Exchange Rate",
    COALESCE(COALESCE("Order Attributes".bbb_currency_code, edw_prod_dbo.edw_getoucurrency("Sales Orders-F".org_id::bigint, "Sales Orders-F".source_system)), "Order Attributes".order_currency) AS "Transaction Curr Code",
    "Order Attributes".customer_po AS "Cust PO Number",
    "Order Attributes".request_date AS "Request Date",
    "Order Attributes".ordered_item AS "Ordered Item",
        CASE
            WHEN "Order Attributes".line_type_name::text ~~ '%Only%'::text THEN COALESCE("Order Attributes".schedule_date, "Order Attributes".request_date)
            ELSE "Order Attributes".schedule_date
        END AS "Scheduled Ship Date",
    "Sales Orders-F".qty_cancelled AS "Cancelled Quantity",
    "Sales Orders-F".qty_shipped AS "Shipped Quantity",
    "Sales Orders-F".unit_price_usd AS "Unit Selling Price",
    "Order Attributes".actual_ship_date AS "Actual Ship Date",
    "Order Attributes".life_cycle_status AS "Line Order Type",
    "Order Attributes".line_type_name AS "Line Order Type Name",
    "Item-Org Attributes".standard_cost_currency AS "Ship From Func Currency",
    COALESCE(edw_prod_dbo.edw_getdailyrateusd(CURRENT_DATE, edw_prod_dbo.edw_getordtypecurrency("Order Attributes".order_header_type, "Order Attributes".source_system), "Sales Orders-F".source_system, "Sales Orders-F".exch_rate_usd), 1::double precision) AS "Curr Exchange Rate",
    round(("Sales Orders-F".unit_price_usd * COALESCE("Sales Orders-F".qty_ordered, 1::double precision))::numeric, 2) AS "Extended Price USD",
        CASE "left"("Order Attributes".life_cycle_status::text, 4)
            WHEN 'Bill'::text THEN "Order Attributes".bill_only_ship_date::timestamp with time zone
            WHEN 'Late'::text THEN date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)
            WHEN 'SNI '::text THEN
            CASE
                WHEN COALESCE("Order Attributes".schedule_date, "Order Attributes".request_date) < date_trunc('month'::text, CURRENT_DATE::timestamp with time zone) THEN date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)
                ELSE COALESCE("Order Attributes".schedule_date, "Order Attributes".request_date, CURRENT_DATE)::timestamp with time zone
            END
            WHEN 'Cred'::text THEN
            CASE
                WHEN date_trunc('month'::text, CURRENT_DATE::timestamp with time zone) > COALESCE("Order Attributes".schedule_date, "Order Attributes".actual_ship_date, '2020-01-01'::date) THEN date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)
                ELSE COALESCE("Order Attributes".schedule_date, "Order Attributes".actual_ship_date)::timestamp with time zone
            END
            WHEN 'Unsc'::text THEN (date_trunc('quarter'::text, CURRENT_DATE::timestamp with time zone) + '3 mons'::interval - '1 day'::interval)::date::timestamp with time zone
            ELSE
            CASE
                WHEN COALESCE("Order Attributes".schedule_date, "Order Attributes".request_date) < date_trunc('month'::text, CURRENT_DATE::timestamp with time zone) THEN date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)
                ELSE COALESCE("Order Attributes".schedule_date, "Order Attributes".request_date, CURRENT_DATE)::timestamp with time zone
            END
        END AS "Order Date",
    "Order Attributes".booked_date AS "Booked Date",
    replace("Order Attributes".cost_of_sale_account::text, '.'::text, '-'::text) AS "Cost of Sale Account",
    "Order Attributes".order_created_by AS created_by_name,
    "Ship From Org".org_name AS "Ship From Org Name",
    "Ship From Org".org_code AS "Ship From Org Code",
    "Bill To Customer".customer_name AS "Bill To Customer Parent",
    "Bill To Customer".customer_name AS "Bill To Customer Name",
    "Ship To Customer".customer_name AS "Ship To Customer Parent",
    "Ship To Customer".customer_name AS "Ship To Customer Name",
        CASE
            WHEN COALESCE("Cost Look".include_cost, 'Y'::character varying)::text = 'N'::text OR upper("Order Attributes".line_type_name::text) ~~ '%ONLY%'::text THEN 0::double precision
            ELSE
            CASE
                WHEN COALESCE("Sales Orders-F".virtual_organization_id, '300000003579286'::bigint) = ANY (ARRAY[0::bigint, '300000003579286'::bigint]) THEN edw_prod_dbo.edw_gettotalactualcostlc("Sales Orders-F".item_id::bigint, "Sales Orders-F".ship_from_org_id::bigint, CURRENT_DATE, 'OC'::character varying)
                ELSE edw_prod_dbo.edw_gettotalactualcostlc(COALESCE("Sales Orders-F".virtual_item_id::numeric, "Sales Orders-F".item_id)::bigint, COALESCE("Sales Orders-F".virtual_organization_id::numeric, "Sales Orders-F".ship_from_org_id)::bigint, CURRENT_DATE, 'OC'::character varying)
            END
        END AS "Ship From Item Cost LC",
        CASE
            WHEN COALESCE("Cost Look".include_cost, 'Y'::character varying)::text = 'N'::text OR upper("Order Attributes".line_type_name::text) ~~ '%ONLY%'::text THEN 0::double precision
            ELSE
            CASE
                WHEN COALESCE("Sales Orders-F".virtual_organization_id, '300000003579286'::bigint) = ANY (ARRAY[0::bigint, '300000003579286'::bigint]) THEN edw_prod_dbo.edw_gettotalactualcostusd("Sales Orders-F".item_id::bigint, "Sales Orders-F".ship_from_org_id::bigint, CURRENT_DATE, "Ship From Org".currency_code, 'OC'::character varying)
                ELSE edw_prod_dbo.edw_gettotalactualcostusd(COALESCE("Sales Orders-F".virtual_item_id::numeric, "Sales Orders-F".item_id)::bigint, COALESCE("Sales Orders-F".virtual_organization_id::numeric, "Sales Orders-F".ship_from_org_id)::bigint, CURRENT_DATE, "Ship From Org".currency_code, 'OC'::character varying)
            END
        END AS "Ship From Item Cost USD",
    "Sales Orders-F".unit_price * COALESCE("Sales Orders-F".qty_ordered, 1::double precision) AS "Extended Price LC",
    "Sales Rep".full_name AS "Sales Rep",
    "Sales Rep".region AS "Sales Region",
    bh.name AS "Hold Type",
    "Sales Orders-F".unit_price AS "Unit Selling PriceLC",
    COALESCE("Sales Orders-F".product_code_node, "substring"("Order Attributes".sales_account::text, 6, 3)::character varying) AS "Product Class",
    replace("Order Attributes".sales_account::text, '.'::text, '-'::text) AS "Sales Account",
    ''::text AS "GL Date",
    "Order Attributes".inv_currency_code AS "Invoice Curr Code",
    "Sales Orders-F".inv_quantity_credited AS "Quantity Credited",
    abs(COALESCE("Sales Orders-F".inv_quantity_invoiced, 0::double precision) + COALESCE("Sales Orders-F".inv_quantity_credited, 0::double precision)) AS "Quantity Invoiced",
    "Operating Units".ou_name AS "SOB Description",
    edw_prod_dbo.edw_getperavgrateusd(CURRENT_DATE, COALESCE("Order Attributes".bbb_currency_code, edw_prod_dbo.edw_getoucurrency("Sales Orders-F".org_id::bigint, "Sales Orders-F".source_system)), "Sales Orders-F".source_system, COALESCE(NULLIF("Order Attributes".inv_exch_rate, 0::double precision), "Sales Orders-F".exch_rate_usd)) AS "Invoice Period AVG Rate",
    COALESCE("Item-Org Attributes".category, "Products".category) AS "Category",
    (((COALESCE("Item-Org Attributes".product_family, "Products".product_family)::text || '.'::text) || COALESCE("Item-Org Attributes".product_line, "Products".product_line)::text) || '.'::text) || COALESCE("Item-Org Attributes".product_model, "Products".product_model)::text AS "Category Name",
    COALESCE("Item-Org Attributes".commodity_class, "Products".commodity_class) AS "Commodity Class",
    COALESCE("Item-Org Attributes".description, "Products".item_description) AS "Item Description",
    COALESCE("Item-Org Attributes".item_number, "Products".item_number) AS "Item Name",
    replace(COALESCE("Item-Org Attributes".product_family, "Products".product_family)::text, '_'::text, ' '::text) AS "Product Family",
    replace(COALESCE("Item-Org Attributes".product_line, "Products".product_line)::text, '_'::text, ' '::text) AS "Product Line",
    replace(COALESCE("Item-Org Attributes".product_model, "Products".product_model)::text, '_'::text, ' '::text) AS "Product Model",
    COALESCE("Item-Org Attributes".sourcing_segment, "Products".sourcing_segment) AS "Sourcing Segment",
    "substring"("Financial Hierarchy"."LEVEL05_NAME", 7, 100) AS "Label (L5)",
    "Financial Hierarchy"."LEVEL05_NAME" AS "Product Code & Label",
    "Financial Hierarchy".level01_name AS "Segment (L1)",
    "Financial Hierarchy".level03_name AS "Family Segment (L3)",
    "Financial Hierarchy".level02_name AS "Product/Segment (L2)",
    "Financial Hierarchy".level04_name AS "Family Group (L4)",
        CASE
            WHEN upper("Item-Org Attributes".description::text) ~~ 'NRC%'::text THEN 'NRC'::text
            ELSE
            CASE
                WHEN upper("Item-Org Attributes".product_model::text) ~~ '%COMMON%'::text THEN 'Spare'::text
                ELSE 'System'::text
            END
        END AS "Common vs System",
    "Order Attributes".promise_date AS "Promise Date",
    "Operating Units".ou_code AS "OU",
    COALESCE(btreg.region, "Bill To Customer".region) AS "Bill To Region",
    COALESCE(streg.region, "Ship To Customer".region) AS "Ship To Region",
    "Bill To Customer".country AS "Bill To Country",
    "Ship To Customer".country AS "Ship To Country",
    "Bill To Customer".customer_class_code AS "Bill To Class Code",
    "Ship To Customer".customer_class_code AS "Ship To Class Code",
    btreg."Mgmt Continent" AS "Bill To Mgmt Continent",
    streg."Mgmt Continent" AS "Ship To Mgmt Continent",
    "Order Attributes".inv_trx_number AS "Invoice TRX Number",
    "Sales Orders-F".unit_price AS "Unit Selling Price LC",
        CASE
            WHEN COALESCE("Cost Look".include_cost, 'Y'::character varying)::text = 'N'::text OR upper("Order Attributes".line_type_name::text) ~~ '%ONLY%'::text THEN 0::double precision
            ELSE
            CASE
                WHEN COALESCE("Sales Orders-F".virtual_organization_id, '300000003579286'::bigint) = ANY (ARRAY[0::bigint, '300000003579286'::bigint]) THEN edw_prod_dbo.edw_gettotalactualcostlc("Sales Orders-F".item_id::bigint, "Sales Orders-F".ship_from_org_id::bigint, CURRENT_DATE, 'OC'::character varying) * COALESCE("Sales Orders-F".qty_ordered, 0::double precision)
                ELSE edw_prod_dbo.edw_gettotalactualcostlc(COALESCE("Sales Orders-F".virtual_item_id::numeric, "Sales Orders-F".item_id)::bigint, COALESCE("Sales Orders-F".virtual_organization_id::numeric, "Sales Orders-F".ship_from_org_id)::bigint, CURRENT_DATE, 'OC'::character varying) * COALESCE("Sales Orders-F".qty_ordered, 0::double precision)
            END
        END AS "Ext Ship From Item Cost LC",
        CASE
            WHEN COALESCE("Cost Look".include_cost, 'Y'::character varying)::text = 'N'::text OR upper("Order Attributes".line_type_name::text) ~~ '%ONLY%'::text THEN 0::double precision
            ELSE
            CASE
                WHEN COALESCE("Sales Orders-F".virtual_organization_id, '300000003579286'::bigint) = ANY (ARRAY[0::bigint, '300000003579286'::bigint]) THEN edw_prod_dbo.edw_gettotalactualcostusd("Sales Orders-F".item_id::bigint, "Sales Orders-F".ship_from_org_id::bigint, CURRENT_DATE, "Ship From Org".currency_code, 'OC'::character varying) * COALESCE("Sales Orders-F".qty_ordered, 0::double precision)
                ELSE edw_prod_dbo.edw_gettotalactualcostusd("Sales Orders-F".item_id::bigint, "Sales Orders-F".ship_from_org_id::bigint, CURRENT_DATE, "Ship From Org".currency_code, 'OC'::character varying) * COALESCE("Sales Orders-F".qty_ordered, 0::double precision)
            END
        END AS "Ext Ship From Item Cost USD",
    TRIM(BOTH FROM COALESCE(COALESCE(mc.mgmt_parent, mo.top_level_customer_name), "Bill To Customer".customer_name)) AS "MGMT Customer",
    TRIM(BOTH FROM replace(COALESCE(COALESCE(COALESCE(mc.mgmt_region, mo.management_region), "Ship To Customer".region), "Bill To Customer".region)::text, 'ASIA-'::text, ''::text)) AS "MGMT Region",
    'BackBill'::text AS "Data_Type",
    'OC Backlog'::text AS source_system,
    "Sales Orders-F".line_id,
    ar_terms.ar_term_desc AS payment_terms,
    "Order Attributes".order_status,
    "Order Attributes".line_status,
    "Order Attributes".fulfill_line_status,
    "Order Attributes".order_creation_date,
    "Order Attributes".line_creation_date,
    "Order Attributes".last_update_date,
    COALESCE("Ship To Customer".primary_market_segment, "Bill To Customer".primary_market_segment) AS primary_market_segment,
    COALESCE("Ship To Customer".secondary_market_segment, "Bill To Customer".secondary_market_segment) AS secondary_market_segment,
    "Order Attributes".fulfillment_line_number,
    "Order Attributes".line_source_number,
    "Order Attributes".life_cycle_status,
    "Sales Orders-F".invoice_to_org_id,
    "Sales Orders-F".ship_to_org_id,
    round(edw_prod_dbo.edw_gettotalactualcostusd("Sales Orders-F".item_id::bigint, "Sales Orders-F".ship_from_org_id::bigint, CURRENT_DATE, edw_prod_dbo.edw_getoucurrency("Sales Orders-F".ship_from_org_id::bigint, 'OC'::character varying), 'OC'::character varying)::numeric, 2) AS item_cost_test,
    streg.region,
    "Sales Orders-F".item_id,
    "Sales Orders-F".ship_from_org_id,
    "Order Attributes".order_date,
    "Sales Orders-F".org_id,
    0 AS "Total_distribution_cost LC",
    edw_prod_dbo.edw_getcostfrozencostperavgusd("Sales Orders-F".item_id::bigint, "Item-Org Attributes".default_shipping_org::bigint, CURRENT_DATE, 'OC'::character varying) AS "Mfg_frozen_cost_at_per_avg_in_USD",
    edw_prod_dbo.edw_getcostfrozencostplannedusd("Sales Orders-F".item_id::bigint, "Sales Orders-F".ship_from_org_id::bigint, CURRENT_DATE, 'OC'::character varying) AS "Mfg_Frozen_Cost_at_Planned_in_USD",
    0 AS "Ship_Frozen_Cost_at_Per_Avg_in_USD"
   FROM edw_prod_dbo.w_sales_orders_f "Sales Orders-F"
     JOIN edw_prod_dbo.w_sales_orders_d "Order Attributes" ON "Sales Orders-F".line_id = "Order Attributes".line_id AND "Sales Orders-F".source_system::text = "Order Attributes".source_system::text
     LEFT JOIN edw_prod_dbo.w_sales_orders_d "EBS Invoice" ON "EBS Invoice".line_id = "Sales Orders-F".line_id AND "EBS Invoice".source_system::text = 'EBS'::text
     JOIN edw_prod_dbo.w_oper_unit_d "Operating Units" ON "Sales Orders-F".org_id = "Operating Units".org_id AND "Sales Orders-F".source_system::text = "Operating Units".source_system::text
     LEFT JOIN edw_prod_dbo.w_ar_terms_d ar_terms ON "Sales Orders-F".payment_term_id = ar_terms.terms_id AND "Sales Orders-F".source_system::text = ar_terms.source_system::text
     LEFT JOIN edw_prod_dbo.w_customer_site_d "Bill To Customer" ON "Sales Orders-F".invoice_to_org_id = "Bill To Customer".site_use_id AND "Bill To Customer".site_use_code::text = 'BILL_TO'::text AND "Bill To Customer".source_system::text = 'OC'::text
     LEFT JOIN edw_prod_dbo.w_customer_site_d "Ship To Customer" ON "Sales Orders-F".ship_to_org_id = "Ship To Customer".site_use_id AND "Ship To Customer".site_use_code::text = 'SHIP_TO'::text AND "Ship To Customer".source_system::text = 'OC'::text
     LEFT JOIN edw_prod_dbo.w_customer_d "Sold To Customer" ON "Sales Orders-F".sold_to_org_id = "Sold To Customer".cust_account_id AND "Sales Orders-F".source_system::text = "Sold To Customer".source_system::text
     LEFT JOIN edw_prod_dbo.w_organization_d "Ship From Org" ON "Sales Orders-F".ship_from_org_id = "Ship From Org".src_org_id AND "Sales Orders-F".source_system::text = "Ship From Org".source_system::text
     LEFT JOIN edw_prod_dbo.w_item_org_d "Item-Org Attributes" ON "Sales Orders-F".item_id = "Item-Org Attributes".inventory_item_id AND "Item-Org Attributes".organization_id = "Sales Orders-F".ship_from_org_id::character varying(20)::numeric AND "Item-Org Attributes".source_system::text = 'OC'::text
     LEFT JOIN edw_prod_dbo.w_product_d "Products" ON "Sales Orders-F".item_id = "Products".inventory_item_id AND "Sales Orders-F".source_system::text = "Products".source_system::text
     LEFT JOIN edw_prod_dbo.w_salesrep_d "Sales Rep" ON "Sales Orders-F".salesrep_id = "Sales Rep".salesrep_id AND "Sales Orders-F".org_id = "Sales Rep".org_id AND "Sales Orders-F".source_system::text = "Sales Rep".source_system::text
     LEFT JOIN edw_prod_dbo."W_PRODUCT_DH_V" "Financial Hierarchy" ON COALESCE("Sales Orders-F".product_code, "substring"("Order Attributes".sales_account::text, 6, 3)::numeric) = "Financial Hierarchy".node::numeric
     LEFT JOIN edw_prod_dbo.bbb_order_holds_v bh ON "Sales Orders-F".line_id = bh.line_id::numeric AND "Sales Orders-F".order_id = bh.header_id::numeric
     LEFT JOIN edw_prod_ref.bi_cust_account_groups "CUST ACCOUNT GROUPS BT" ON "Bill To Customer".customer_name::text = "CUST ACCOUNT GROUPS BT".customername::text
     LEFT JOIN edw_prod_ref.bi_country_region "Country Region BT" ON "Bill To Customer".country::text = "Country Region BT".region::text
     LEFT JOIN edw_prod_ref.bi_cust_account_groups "CUST ACCOUNT GROUPS ST" ON "Ship To Customer".customer_name::text = "CUST ACCOUNT GROUPS ST".customername::text
     LEFT JOIN edw_prod_ref.bi_country_region "Country Region ST" ON "Ship To Customer".country::text = "Country Region ST".region::text
     LEFT JOIN edw_prod_dbo.mgmt_cust_pn_lookup mc ON mc."Bill To Customer Name"::text = "Bill To Customer".customer_name::text AND mc."Brooks PN"::text = "Item-Org Attributes".item_number::text AND mc."Customer PN"::text = "Order Attributes".ordered_item::text
     LEFT JOIN edw_prod_dbo.management_override_new mo ON mo.customer_name::text = "Bill To Customer".customer_name::text
     LEFT JOIN edw_prod_dbo.country_region streg ON streg."Country Code"::text = "Ship To Customer".country::text
     LEFT JOIN edw_prod_dbo.country_region btreg ON btreg."Country Code"::text = "Bill To Customer".country::text
     LEFT JOIN edw_prod_dbo.bbb_include_cost_lookup "Cost Look" ON "Cost Look".lookup_value::text = (("left"("Order Attributes".order_header_type::text, 10) || '-'::text) || "left"("Order Attributes".line_type_name::text, 10))
  WHERE 1 = 1 AND "Order Attributes".order_header_type::text !~~ '%Intercompany%'::text AND "Order Attributes".order_header_type::text !~~ '%Internal%'::text AND "Order Attributes".order_header_type::text !~~ '%Vendor Sales%'::text AND "Order Attributes".order_header_type::text !~~ '%Eval%'::text AND "Order Attributes".order_header_type::text !~~ '%Transfer%'::text AND upper(COALESCE("Order Attributes".customer_po, '!'::character varying)::text) !~~ '%SPEC%'::text AND COALESCE("Order Attributes".fulfill_line_status, 'DNC'::character varying)::text <> 'DNC'::text AND "Order Attributes".ordered_item::text <> 'FREIGHT'::text AND COALESCE("Item-Org Attributes".product_family, '~'::character varying)::text !~~ 'CTI%'::text AND COALESCE("Item-Org Attributes".product_family, '~'::character varying)::text !~~ 'Poly%'::text AND (1 = 1 OR "Order Attributes".life_cycle_status::text = 'Bill Only'::text AND "Order Attributes".line_status::text = 'INVOICE_HOLD'::text AND "Financial Hierarchy".level01_name::text = 'CCG'::text) AND (COALESCE("Order Attributes".order_date, '2024-09-01'::date) > '2020-01-01'::date OR "Order Attributes".source_system::text <> 'EBS'::text AND "Order Attributes".ebs_line_id IS NULL) AND COALESCE("Order Attributes".life_cycle_status, 'Backlog'::character varying)::text <> 'Invoiced'::text AND ("Order Attributes".fulfill_line_status::text <> ALL (ARRAY['CLOSED'::character varying::text, 'CANCELLED'::character varying::text, 'CANCELED'::character varying::text])) AND ("Order Attributes".order_status::text <> ALL (ARRAY['DOO_REFERENCE'::character varying::text, 'DOO_DRAFT'::character varying::text, 'DOO_CREDIT_REVI'::character varying::text])) AND COALESCE("Sales Orders-F".qty_ordered, 9999::double precision) <> 9999::double precision AND COALESCE("Order Attributes".life_cycle_status, 'Backlog'::character varying)::text <> 'Invoiced'::text;