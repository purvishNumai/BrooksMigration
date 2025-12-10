-- DROP PROCEDURE edw_prod_dbo.loadbbbv2();

CREATE OR REPLACE PROCEDURE edw_prod_dbo.loadbbbv2()
 LANGUAGE plpgsql
AS $procedure$
BEGIN

truncate table "edw_prod_dbo".FNC_BACKLOG_BILLING_MV;

--***************Commenting first insertion query as few columns are missing from select**************************
INSERT INTO edw_prod_dbo.fnc_backlog_billing_mv
(
   "Hdr Order Type Name",
   "Order Number",
   "Line Number",
   "Shipment Number",
   "Ordered Quantity",
   "Order Exchange Rate",
   "Transaction Curr Code",
   "Cust PO Number",
   "Request Date",
   "Ordered Item",
   "Scheduled Ship Date",
   "Cancelled Quantity",
   "Shipped Quantity",
   "Unit Selling Price",
   "Actual Ship Date",
   "Line Order Type",
   "Line Order Type Name",
   "Ship From Func Currency",
   "Curr Exchange Rate",
   "Extended Price USD",
   "Order Date",
   "Booked Date",
   "Cost of Sale Account",
   created_by_name,
   "Ship From Org Name",
   "Ship From Org Code",
   "Bill To Customer Parent",
   "Bill To Customer Name",
   "Ship To Customer Parent",
   "Ship To Customer Name",
   "Ship From Item Cost LC",
   "Ship From Item Cost USD",
   "Extended Price LC",
   "Sales Rep",
   "Sales Region",
   "Hold Type",
   "Unit Selling PriceLC",
   "Product Class",
   "Sales Account",
   "GL Date",
   "Invoice Curr Code",
   "Quantity Credited",
   "Quantity Invoiced",
   "SOB Description",
   "Invoice Period AVG Rate",
   category,
   "Category Name",
   "Commodity Class",
   "Item Description",
   "Item Name",
   "Product Family",
   "Product Line",
   "Product Model",
   "Sourcing Segment",
   "Label (L5)",
   "Product Code & Label",
   "Segment (L1)",
   "Family Segment (L3)",
   "Product/Segment (L2)",
   "Family Group (L4)",
   "Common vs System",
   "Promise Date",
   ou,
   "Bill To Region",
   "Ship To Region",
   "Ship To Country",
   "Bill To Class Code",
   "Ship To Class Code",
   "Invoice TRX Number",
   "Ext Ship From Item Cost LC",
   "Ext Ship From Item Cost USD",
   "MGMT Customer",
   "MGMT Region",
   data_type,
   "source_system",
   "line_id",
   "Bill To Country",
   "order_status",
   LINE_STATUS,
   FULFILL_LINE_STATUS,
   ORDER_CREATION_DATE,
   LINE_CREATION_DATE,
   LAST_UPDATE_DATE,
   PAYMENT_TERMS,
   PRIMARY_MARKET_SEGMENT,
   SECONDARY_MARKET_SEGMENT,
   FULFILLMENT_LINE_NUMBER,
   "line_source_number",
   "Bill To Mgmt Continent",
   "Ship To Mgmt Continent",
   "Total_distribution_cost LC",
   Mfg_frozen_cost_at_per_avg_in_USD,
   Mfg_Frozen_Cost_at_Planned_in_USD,
   Ship_Frozen_Cost_at_Per_Avg_in_USD
)
SELECT
   "HDR Order Type name",
   "Order Number",
   "Line Number",
   "Shipment Number",
   "Ordered Quantity",
   "Order Exchange Rate",
   "Transaction Curr Code",
   "Cust PO Number",
   "Request Date",
   "Ordered Item",
   "Scheduled Ship Date",
   "Cancelled Quantity",
   "Shipped Quantity",
   "Unit Selling Price",
   "Actual Ship Date",
   "Line Order Type",
   "Line Order Type Name",
   "Ship From Func Currency",
   "Curr Exchange Rate",
   "Extended Price USD",
   "Order Date",
   "Booked Date",
   "Cost of Sale Account",
   created_by_name,
   "Ship From Org Name",
   "Ship From Org Code",
   "Bill To Customer Parent",
   "Bill To Customer Name",
   "Ship To Customer Parent",
   "Ship To Customer Name",
   "Ship From Item Cost LC",     
   "Ship From Item Cost USD",    
   "Extended Price LC",
   "Sales Rep",
   "Sales Region",
   '' AS "Hold Type",                    -- filler
   "Unit Selling Price LC",
   "Product Class",
   "Sales Account",
   "GL Date",
   "Invoice Curr Code",
   "Quantity Credited",
   "Quantity Invoiced",
   "SOB Description",
   "Invoice Period AVG Rate",
   "Category",
   "Category Name",
   "Commodity Class",
   "Item Description",
   "Item Name",
   "Product Family",
   "Product Line",
   "Product Model",
   "Sourcing Segment",
   "Label (L5)",
   "Product Code & Label",
   "Segment (L1)",
   "Family Segment (L3)",
   "Product/Segment (L2)",
   "Family Group (L4)",
   "Common vs System",
   "Promise Date",
   "OU",
   "Bill To Region",
   "Ship To Region",
   "Ship To Country",
   "Bill To Class Code",
   "Ship To Class Code",
   "Invoice TRX Number",
   --total_distribution_cost_lc AS 
   "Ext Ship From Item Cost LC",
   --totalactualcostusd AS 
   "Ext Ship From Item Cost USD",
   "MGMT Customer",
   "MGMT Region",
   "Data_Type",
   source_system,
   line_id,
   "Bill To Country",
   order_status,
   line_status,
   fulfill_line_status,
   order_creation_date,
   line_creation_date,
   last_update_date,
   payment_terms,
   primary_market_segment,
   secondary_market_segment,
   fulfillment_line_number,
   line_source_number,
   "Bill To Mgmt Continent",
   "Ship To Mgmt Continent",
   total_distribution_cost_lc,
   mfg_frozen_cost_at_per_avg_in_usd,
   mfg_frozen_cost_at_planned_in_usd,
   ship_frozen_cost_at_per_avg_in_usd
FROM edw_prod_dbo.bbb_om_table1_oc_invoc_v;





INSERT INTO edw_prod_dbo.fnc_backlog_billing_mv
("Hdr Order Type Name", "Order Number", "Line Number", "Shipment Number", "Ordered Quantity", "Order Exchange Rate", "Transaction Curr Code", "Cust PO Number", "Request Date", "Ordered Item", "Scheduled Ship Date", "Cancelled Quantity", "Shipped Quantity", "Unit Selling Price", "Actual Ship Date", "Line Order Type", "Line Order Type Name", "Ship From Func Currency", "Curr Exchange Rate", "Extended Price USD", "Order Date", "Booked Date", "Cost of Sale Account", created_by_name, "Ship From Org Name", "Ship From Org Code", "Bill To Customer Parent", "Bill To Customer Name", "Ship To Customer Parent", "Ship To Customer Name", "Ship From Item Cost LC", "Ship From Item Cost USD", "Extended Price LC", "Sales Rep", "Sales Region", "Hold Type", "Unit Selling PriceLC", "Product Class", "Sales Account", "GL Date" , "Invoice Curr Code", "Quantity Credited", "Quantity Invoiced", "SOB Description", "Invoice Period AVG Rate", category, "Category Name", "Commodity Class", "Item Description", "Item Name", "Product Family", "Product Line", "Product Model", "Sourcing Segment", "Label (L5)", "Product Code & Label", "Segment (L1)", "Family Segment (L3)", "Product/Segment (L2)", "Family Group (L4)", "Common vs System", "Promise Date", ou, "Bill To Region", "Ship To Region", "Ship To Country", "Bill To Class Code", "Ship To Class Code", 
"Invoice TRX Number", "Ext Ship From Item Cost LC", "Ext Ship From Item Cost USD", "MGMT Customer", "MGMT Region", data_type, source_system, line_id, "Bill To Country", order_status, line_status, fulfill_line_status, order_creation_date, line_creation_date, last_update_date, payment_terms, primary_market_segment, secondary_market_segment, fulfillment_line_number, line_source_number,
--"Invoice Line Number",
"Bill To Mgmt Continent", "Ship To Mgmt Continent", "Total_distribution_cost LC", mfg_frozen_cost_at_per_avg_in_usd, mfg_frozen_cost_at_planned_in_usd, ship_frozen_cost_at_per_avg_in_usd)

SELECT "Hdr Order Type Name", "Order Number", "Line Number", "Shipment Number", "Ordered Quantity", "Order Exchange Rate", "Transaction Curr Code", "Cust PO Number", "Request Date", "Ordered Item", "Scheduled Ship Date", "Cancelled Quantity", "Shipped Quantity", "Unit Selling Price", "Actual Ship Date", "Line Order Type", "Line Order Type Name", "Ship From Func Currency", "Curr Exchange Rate", "Extended Price USD", "Order Date", "Booked Date", "Cost of Sale Account", created_by_name, "Ship From Org Name", "Ship From Org Code", "Bill To Customer Parent", "Bill To Customer Name", "Ship To Customer Parent", "Ship To Customer Name", "Ship From Item Cost LC", "Ship From Item Cost USD", "Extended Price LC", "Sales Rep", "Sales Region", "Hold Type", "Unit Selling PriceLC", "Product Class", "Sales Account", CAST(NULLIF("GL Date", '') AS date), "Invoice Curr Code", "Quantity Credited", "Quantity Invoiced", "SOB Description", "Invoice Period AVG Rate", "Category", "Category Name", "Commodity Class", "Item Description", "Item Name", "Product Family", "Product Line", "Product Model", "Sourcing Segment", "Label (L5)", "Product Code & Label", "Segment (L1)", "Family Segment (L3)", "Product/Segment (L2)", "Family Group (L4)", "Common vs System", "Promise Date", "OU", "Bill To Region", "Ship To Region", "Ship To Country", "Bill To Class Code", "Ship To Class Code" 
, "Invoice TRX Number", "Ext Ship From Item Cost LC", "Ext Ship From Item Cost USD", "MGMT Customer", "MGMT Region", "Data_Type", source_system, line_id,"Bill To Country", order_status, line_status, fulfill_line_status, order_creation_date, line_creation_date, last_update_date,payment_terms, primary_market_segment, secondary_market_segment, fulfillment_line_number, line_source_number, 
--invoice_to_org_id,
"Bill To Mgmt Continent","Ship To Mgmt Continent","Total_distribution_cost LC", "Mfg_frozen_cost_at_per_avg_in_USD", "Mfg_Frozen_Cost_at_Planned_in_USD","Ship_Frozen_Cost_at_Per_Avg_in_USD"
FROM edw_prod_dbo.bbb_om_table2_oc_backlog_v;


INSERT INTO edw_prod_dbo.fnc_backlog_billing_mv
("Hdr Order Type Name", "Order Number", "Line Number", "Shipment Number", "Ordered Quantity", "Order Exchange Rate", "Transaction Curr Code",
"Cust PO Number", "Request Date", "Ordered Item", "Scheduled Ship Date", "Cancelled Quantity", "Shipped Quantity", 
"Unit Selling Price", "Actual Ship Date", "Line Order Type", "Line Order Type Name", "Ship From Func Currency", "Curr Exchange Rate",
"Extended Price USD", "Order Date", "Booked Date", "Cost of Sale Account", created_by_name, "Ship From Org Name", "Ship From Org Code",
"Bill To Customer Parent", "Bill To Customer Name", "Ship To Customer Parent", "Ship To Customer Name", "Ship From Item Cost LC",
"Ship From Item Cost USD", "Extended Price LC", "Sales Rep", "Sales Region","Hold Type", 
"Unit Selling PriceLC", "Product Class", "Sales Account", "GL Date", "Invoice Curr Code", "Quantity Credited", "Quantity Invoiced", 
"SOB Description", "Invoice Period AVG Rate", category, "Category Name", "Commodity Class", "Item Description", "Item Name",
"Product Family", "Product Line", "Product Model", "Sourcing Segment", "Label (L5)", "Product Code & Label", "Segment (L1)", 
"Family Segment (L3)", "Product/Segment (L2)", "Family Group (L4)", "Common vs System", "Promise Date", ou, "Bill To Region",
"Ship To Region", "Ship To Country", "Bill To Class Code", "Ship To Class Code", "Invoice TRX Number", "Ext Ship From Item Cost LC", 
"Ext Ship From Item Cost USD", "MGMT Customer", "MGMT Region", data_type, source_system, line_id, "Bill To Country", 
order_status, line_status, fulfill_line_status, order_creation_date, line_creation_date, last_update_date, payment_terms, primary_market_segment,
secondary_market_segment, fulfillment_line_number, line_source_number, "Invoice Line Number", "Bill To Mgmt Continent", 
"Ship To Mgmt Continent", mfg_frozen_cost_at_per_avg_in_usd)

SELECT "HDR Order Type name", "Order Number", "Line Number",  "Shipment Number", "Ordered Quantity", "Order Exchange Rate", "Transaction Curr Code", 
"Cust PO Number", "Request Date", "Ordered Item", "Scheduled Ship Date", "Cancelled Quantity", "Shipped Quantity", 
"Unit Selling Price", "Actual Ship Date", "Line Order Type", "Line Order Type Name", "Ship From Func Currency", "Curr Exchange Rate"
, "Extended Price USD", "Order Date", "Booked Date", "Cost of Sale Account", created_by_name, "Ship From Org Name", "Ship From Org Code"
, "Bill To Customer Parent", "Bill To Customer Name", "Ship To Customer Parent", "Ship To Customer Name", "Ship From Item Cost LC", 
"Ship From Item Cost USD", "Extended Price LC", "Sales Rep", "Sales Region",'' as "Hold Type", "Unit Selling Price LC",
"Product Class", "Sales Account", "GL Date", "Invoice Curr Code", "Quantity Credited", "Quantity Invoiced", "SOB Description", 
"Invoice Period AVG Rate", "Category", "Category Name", "Commodity Class", "Item Description", "Item Name", "Product Family", 
"Product Line", "Product Model", "Sourcing Segment", "Label (L5)", "Product Code & Label", "Segment (L1)", "Family Segment (L3)", 
"Product/Segment (L2)", "Family Group (L4)", "Common vs System", "Promise Date", "OU", "Bill To Region", "Ship To Region",
"Ship To Country", "Bill To Class Code", "Ship To Class Code", "Invoice TRX Number", "Ext Ship From Item Cost LC",
"Ext Ship From Item Cost USD", "MGMT Customer", "MGMT Region", "Data_Type", source_system, line_id,"Bill To Country" 
, order_status, line_status, fulfill_line_status, order_creation_date, line_creation_date, last_update_date,payment_terms, primary_market_segment, 
secondary_market_segment, fulfillment_line_number, line_source_number,"Invoice_Line Number","Bill To Mgmt Continent", "Ship To Mgmt Continent", 
mfg_frozen_cost_at_per_avg_in_usd
from edw_prod_dbo.bbb_table1_oc_subscription_invoc_v;


INSERT INTO edw_prod_dbo.fnc_backlog_billing_mv
("Hdr Order Type Name", "Order Number", "Line Number", "Shipment Number", "Ordered Quantity", "Order Exchange Rate", "Transaction Curr Code", "Cust PO Number", "Request Date", "Ordered Item", "Scheduled Ship Date", "Cancelled Quantity", "Shipped Quantity", "Unit Selling Price", "Actual Ship Date", "Line Order Type", "Line Order Type Name", "Ship From Func Currency", "Curr Exchange Rate", "Extended Price USD", "Order Date", "Booked Date", "Cost of Sale Account", created_by_name, "Ship From Org Name", "Ship From Org Code", "Bill To Customer Parent", "Bill To Customer Name", "Ship To Customer Parent", "Ship To Customer Name", "Ship From Item Cost LC", "Ship From Item Cost USD", "Extended Price LC", "Sales Rep", "Sales Region", "Hold Type", "Unit Selling PriceLC", "Product Class", "Sales Account", "GL Date" , "Invoice Curr Code", "Quantity Credited", "Quantity Invoiced", "SOB Description", "Invoice Period AVG Rate", category, "Category Name", "Commodity Class", "Item Description", "Item Name", "Product Family", "Product Line", "Product Model", "Sourcing Segment", "Label (L5)", "Product Code & Label", "Segment (L1)", "Family Segment (L3)", "Product/Segment (L2)", "Family Group (L4)", "Common vs System", "Promise Date", ou, "Bill To Region", "Ship To Region", "Ship To Country", "Bill To Class Code", "Ship To Class Code", 
"Invoice TRX Number", "Ext Ship From Item Cost LC", "Ext Ship From Item Cost USD", "MGMT Customer", "MGMT Region", data_type, source_system, line_id, "Bill To Country", order_status, line_status, fulfill_line_status, order_creation_date, line_creation_date, last_update_date, payment_terms, primary_market_segment, secondary_market_segment, fulfillment_line_number, line_source_number,
"Bill To Mgmt Continent", "Ship To Mgmt Continent", "Total_distribution_cost LC", mfg_frozen_cost_at_per_avg_in_usd, mfg_frozen_cost_at_planned_in_usd, ship_frozen_cost_at_per_avg_in_usd)
--"Bill To Country",

SELECT "Hdr Order Type Name", "Order Number", "Line Number", "Shipment Number", "Ordered Quantity", "Order Exchange Rate", "Transaction Curr Code", "Cust PO Number", "Request Date", "Ordered Item", "Scheduled Ship Date", "Cancelled Quantity", "Shipped Quantity", "Unit Selling Price", "Actual Ship Date", "Line Order Type", "Line Order Type Name", "Ship From Func Currency", "Curr Exchange Rate", "Extended Price USD", "Order Date", "Booked Date", "Cost of Sale Account", "CREATED_BY_NAME", "Ship From Org Name", "Ship From Org Code", "Bill To Customer Parent", "Bill To Customer Name", "Ship To Customer Parent", "Ship To Customer Name", "Ship From Item Cost LC", "Ship From Item Cost USD", "Extended Price LC", "Sales Rep", "Sales Region", "Hold Type", "Unit Selling PriceLC", "Product Class", "Sales Account", "GL Date", "Invoice Curr Code", "Quantity Credited", "Quantity Invoiced", "SOB Description", "Invoice Period AVG Rate", "Category", "Category Name", "Commodity Class", "Item Description", "Item Name", "Product Family", "Product Line", "Product Model", "Sourcing Segment", "Label (L5)", "Product Code & Label", "Segment (L1)", "Family Segment (L3)", "Product/Segment (L2)", "Family Group (L4)", "Common vs System", "Promise Date", "OU", "Bill To Region", "Ship To Region", "Ship To Country", "Bill To Class Code", "Ship To Class Code",
"Invoice TRX Number", "Ext Ship From Item Cost LC", "Ext Ship From Item Cost USD", "MGMT Customer", "MGMT Region", "Data_Type", source_system, "LINE_ID","Bill To Country", order_status, line_status, fulfill_line_status, order_creation_date, line_creation_date, last_update_date, "PAYMENT_TERMS", primary_market_segment, secondary_market_segment, fulfillment_line_number,line_source_number,
 "Bill To Mgmt Continent", "Ship To Mgmt Continent", "Total_distribution_cost LC", mfg_frozen_cost_at_per_avg_in_usd, mfg_frozen_cost_at_planned_in_usd, ship_frozen_cost_at_per_avg_in_usd
FROM edw_prod_dbo.bbb_om_table3_ebs_invoc_v;
--"Unit Selling Price LC",

INSERT INTO edw_prod_dbo.fnc_backlog_billing_mv
("Hdr Order Type Name", "Order Number", "Line Number", "Shipment Number", "Ordered Quantity", "Order Exchange Rate", 
"Transaction Curr Code", "Cust PO Number", "Request Date", "Ordered Item", "Scheduled Ship Date", "Cancelled Quantity",
"Shipped Quantity", "Unit Selling Price", "Actual Ship Date", "Line Order Type", "Line Order Type Name", "Ship From Func Currency",
"Curr Exchange Rate", "Extended Price USD", "Order Date", "Booked Date", "Cost of Sale Account", created_by_name, "Ship From Org Name",
"Ship From Org Code", "Bill To Customer Parent", "Bill To Customer Name", "Ship To Customer Parent", "Ship To Customer Name", 
"Ship From Item Cost LC", "Ship From Item Cost USD", "Extended Price LC", "Sales Rep", "Sales Region", "Hold Type",
"Unit Selling PriceLC", "Product Class", "Sales Account", "GL Date", "Invoice Curr Code", "Quantity Credited", "Quantity Invoiced", 
"SOB Description", "Invoice Period AVG Rate", category, "Category Name", "Commodity Class", "Item Description", "Item Name", 
"Product Family", "Product Line", "Product Model", "Sourcing Segment", "Label (L5)", "Product Code & Label", "Segment (L1)", 
"Family Segment (L3)", "Product/Segment (L2)", "Family Group (L4)", "Common vs System", "Promise Date", ou, "Bill To Region",
"Ship To Region", "Ship To Country", "Bill To Class Code", "Ship To Class Code", "Invoice TRX Number", "Ext Ship From Item Cost LC", 
"Ext Ship From Item Cost USD", "MGMT Customer", "MGMT Region", data_type, source_system, line_id, "Bill To Country", order_status, 
line_status, fulfill_line_status, order_creation_date, line_creation_date, last_update_date, payment_terms, primary_market_segment, 
secondary_market_segment, fulfillment_line_number, line_source_number, "Bill To Mgmt Continent", 
"Ship To Mgmt Continent", "Total_distribution_cost LC", mfg_frozen_cost_at_per_avg_in_usd, mfg_frozen_cost_at_planned_in_usd,
ship_frozen_cost_at_per_avg_in_usd)

SELECT "Hdr Order Type Name", "Order Number", "Line Number", "Shipment Number", "Ordered Quantity", "Order Exchange Rate",
"Transaction Curr Code", "Cust PO Number", "Request Date", "Ordered Item", "Scheduled Ship Date", "Cancelled Quantity", 
"Shipped Quantity", "Unit Selling Price", "Actual Ship Date", "Line Order Type", "Line Order Type Name", "Ship From Func Currency", 
"Curr Exchange Rate", "Extended Price USD", "Order Date", "Booked Date", "Cost of Sale Account", "CREATED_BY_NAME", "Ship From Org Name",
"Ship From Org Code", "Bill To Customer Parent", "Bill To Customer Name", "Ship To Customer Parent", "Ship To Customer Name", 
"Ship From Item Cost LC", "Ship From Item Cost USD", "Extended Price LC", "Sales Rep", "Sales Region", "Hold Type",
"Unit Selling PriceLC", "Product Class", "Sales Account", "GL Date", "Invoice Curr Code", "Quantity Credited", "Quantity Invoiced", 
"SOB Description", "Invoice Period AVG Rate", "Category", "Category Name", "Commodity Class", "Item Description", "Item Name",
"Product Family", "Product Line", "Product Model", "Sourcing Segment", "Label (L5)", "Product Code & Label", "Segment (L1)",
"Family Segment (L3)", "Product/Segment (L2)", "Family Group (L4)", "Common vs System", "Promise Date", "OU", "Bill To Region", 
"Ship To Region", "Ship To Country", "Bill To Class Code", "Ship To Class Code", "Invoice TRX Number", "Ext Ship From Item Cost LC",
"Ext Ship From Item Cost USD", "MGMT Customer", "MGMT Region", "Data_Type", "SOURCE_SYSTEM", line_id,"Bill To Country", order_status,
line_status,fulfill_line_status, order_creation_date, line_creation_date, last_update_date,payment_terms,
primary_market_segment, secondary_market_segment,fulfillment_line_number, line_source_number , "Bill To Mgmt Continent",
"Ship To Mgmt Continent","Total_distribution_cost LC", mfg_frozen_cost_at_per_avg_in_usd, mfg_frozen_cost_at_planned_in_usd, "Ship_Frozen_Cost_at_Per_Avg_in_USD"

FROM edw_prod_dbo.bbb_om_table4_backlog_ebs_v;



DELETE FROM "edw_prod_dbo".FNC_BACKLOG_BILLING_MV
WHERE "Bill To Class Code" IN ('INTERCOMPANY', 'VENDOR');

-- Delete rows with 'INTERCOMPANY' or 'VENDOR' in "Ship To Class Code"
DELETE FROM "edw_prod_dbo".FNC_BACKLOG_BILLING_MV
WHERE "Ship To Class Code" IN ('INTERCOMPANY', 'VENDOR');

-- Delete rows with specific keywords in "HDR Order Type name" (case-insensitive)
DELETE FROM "edw_prod_dbo".FNC_BACKLOG_BILLING_MV
WHERE UPPER("Hdr Order Type Name") LIKE '%CHARGEBACK%'
   OR UPPER("Hdr Order Type Name") LIKE '%CLOUD%'
   OR UPPER("Hdr Order Type Name") LIKE '%EVALUATION%'
   OR UPPER("Hdr Order Type Name") LIKE '%INTERCOMPANY%'
   OR UPPER("Hdr Order Type Name") LIKE '%INTERIM%'
   OR UPPER("Hdr Order Type Name") LIKE '%VENDOR%';

DELETE FROM "edw_prod_dbo".FNC_BACKLOG_BILLING_MV
WHERE UPPER("Item Name") LIKE '%FREIGHT%';



END;
$procedure$
;