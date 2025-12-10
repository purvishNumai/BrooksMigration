-- DROP PROCEDURE edw_prod_dbo.loadbookingsdata();

CREATE OR REPLACE PROCEDURE edw_prod_dbo.loadbookingsdata()
 LANGUAGE plpgsql
AS $procedure$
BEGIN

-- Remove duplicates from W_BOOKING_TRXN

WITH cte AS (
    SELECT line_id, qty_ordered, extended_price, source_system, booked_date,
           ROW_NUMBER() OVER (PARTITION BY line_id, qty_ordered, extended_price, source_system, booked_date ORDER BY line_id) AS duplicatecount
    FROM edw_prod_dbo.w_booking_trxn
    WHERE source_system = 'OC'
)
DELETE FROM edw_prod_dbo.w_booking_trxn b
USING cte
WHERE b.line_id = cte.line_id
AND cte.duplicatecount > 1;

-- ===============================================
-- Step 2: Remove duplicates from W_BOOKING_F
-- ===============================================
WITH cte AS (
    SELECT line_id, qty_ordered, extended_price, source_system, booked_date,
           ROW_NUMBER() OVER (PARTITION BY line_id, qty_ordered, extended_price, source_system, booked_date ORDER BY line_id) AS duplicatecount
    FROM edw_prod_dbo.w_booking_f
    WHERE source_system = 'OC'
)
DELETE FROM edw_prod_dbo.w_booking_f b
USING cte
WHERE b.line_id = cte.line_id
AND cte.duplicatecount > 1;

-- ===============================================
-- Step 3: Remove duplicates based on snapshot_date in W_BOOKING_TRXN
-- ===============================================
WITH cte AS (
    SELECT line_id, qty_ordered, extended_price, source_system, snapshot_date,
           ROW_NUMBER() OVER (PARTITION BY line_id, qty_ordered, extended_price, source_system, snapshot_date ORDER BY line_id) AS duplicatecount
    FROM edw_prod_dbo.w_booking_trxn
    WHERE source_system = 'OC'
)
DELETE FROM edw_prod_dbo.w_booking_trxn b
USING cte
WHERE b.line_id = cte.line_id
AND cte.duplicatecount > 1;

-- ===============================================
-- Step 4: Remove duplicates based on snapshot_date in W_BOOKING_F
-- ===============================================
WITH cte AS (
    SELECT line_id, qty_ordered, extended_price, source_system, snapshot_date,
           ROW_NUMBER() OVER (PARTITION BY line_id, qty_ordered, extended_price, source_system, snapshot_date ORDER BY line_id) AS duplicatecount
    FROM edw_prod_dbo.w_booking_f
    WHERE source_system = 'OC'
)
DELETE FROM edw_prod_dbo.w_booking_f b
USING cte
WHERE b.line_id = cte.line_id
AND cte.duplicatecount > 1;

-- ===============================================
-- Step 5: Update line_sequence in W_BOOKING_TRXN
-- ===============================================


UPDATE edw_prod_dbo.w_booking_trxn b
SET line_sequence = s.line_sequence
FROM edw_prod_dbo.w_booking_trxn_seq_v s
WHERE s.line_id = b.line_id
  AND s.transaction_type = b.transaction_type
  AND s.snapshot_date = b.snapshot_date
  AND COALESCE(b.line_sequence, 0) <> s.line_sequence;

-- ===============================================
-- Step 6: Update SOURCE_SYSTEM if null
-- ===============================================
UPDATE edw_prod_dbo.w_booking_trxn
SET source_system = 'OC'
WHERE source_system IS NULL;

-- ===============================================
-- Step 7: Truncate FNC_BOOKINGS_MV
-- ===============================================
TRUNCATE TABLE edw_prod_dbo.fnc_bookings_mv;

-----******************
INSERT INTO edw_prod_dbo.fnc_bookings_mv(
    "Hdr Order Type Name", "Order Number", "Line Number", "Shipment Number",
    "Ordered Quantity", "Order Exchange Rate", "Transaction Curr Code", "Cust PO Number",
    "Request Date", "Ordered Item", "Scheduled Ship Date", "Cancelled Quantity",
    "Shipped Quantity", "Unit Selling Price", "Actual Ship Date", "Line Order Type",
    "Line Order Type Name", "Ship From Func Currency", "Curr Exchange Rate",
    "Extended Price USD", "Order Date", "Booked Date", "Cost of Sale Account",
    "Ship From Org Name", "Ship From Org Code", "Bill To Customer Parent",
    "Bill To Customer Name", "Ship To Customer Parent", "Ship To Customer Name",
    "Ship From Item Cost LC", "Ship From Item Cost USD", "Extended Price LC",
    "Sales Rep", "Sales Region", "Hold Type", "Unit Selling PriceLC", "Product Class",
    "Sales Account", "GL Date", "Invoice Curr Code", "Quantity Credited", "Quantity Invoiced",
    "SOB Description", "Invoice Period AVG Rate", Category, "Category Name", "Commodity Class",
    "Item Description", "Item Name", "Product Family", "Product Line", "Product Model",
    "Sourcing Segment", "Label (L5)", "Product Code & Label", "Segment (L1)", "Family Segment (L3)",
    "Product/Segment (L2)", "Family Group (L4)", "Common vs System", "Promise Date", OU,
    "Bill To Region", "Ship To Region", "Ship To Country", "Bill To Class Code", "Ship To Class Code",
    "Invoice TRX Number", "Ext Ship From Item Cost LC", "Ext Ship From Item Cost USD", "MGMT Customer",
    "MGMT Region", Data_Type, SOURCE_SYSTEM, TRANSACTION_TYPE, SNAPSHOT_DATE, line_id
)
SELECT 
    "Hdr Order Type Name", "Order Number", "Line Number", "Shipment Number",
    "Ordered Quantity", "Order Exchange Rate", "Transaction Curr Code", "Cust PO Number",
    "Request Date", "Ordered Item", "Scheduled Ship Date", "Cancelled Quantity",
    "Shipped Quantity", "Unit Selling Price", "Actual Ship Date", "Line Order Type",
    "Line Order Type Name", "Ship From Func Currency", "Curr Exchange Rate",
    "Extended Price USD", "Order Date", "Booked Date", "Cost of Sale Account",
    "Ship From Org Name", "Ship From Org Code", "Bill To Customer Parent",
    "Bill To Customer Name", "Ship To Customer Parent", "Ship To Customer Name",
    "Ship From Item Cost LC", "Ship From Item Cost USD", "Extended Price LC",
    "Sales Rep", "Sales Region", "Hold Type", "Unit Selling PriceLC", "Product Class",
    "Sales Account", "GL Date", "Invoice Curr Code", "Quantity Credited", "Quantity Invoiced",
    "SOB Description", "Invoice Period AVG Rate", Category, "Category Name", "Commodity Class",
    "Item Description", "Item Name", "Product Family", "Product Line", "Product Model",
    "Sourcing Segment", "Label (L5)", "Product Code & Label", "Segment (L1)", "Family Segment (L3)",
    "Product/Segment (L2)", "Family Group (L4)", "Common vs System", "Promise Date", OU,
    "Bill To Region", "Ship To Region", "Ship To Country", "Bill To Class Code", "Ship To Class Code",
    "Invoice TRX Number", "Ext Ship From Item Cost LC", "Ext Ship From Item Cost USD", "MGMT Customer",
    "MGMT Region", Data_Type, 'EBS', TRANSACTION_TYPE, "Order Date", line_id
FROM edw_prod_ref.stg_bookings_pre_oc;

INSERT INTO edw_prod_dbo.fnc_bookings_mv
(
    "Hdr Order Type Name","Order Number","Line Number","Shipment Number","Ordered Quantity","Order Exchange Rate", "Transaction Curr Code",
    "Cust PO Number","Request Date","Ordered Item","Scheduled Ship Date","Cancelled Quantity","Shipped Quantity","Unit Selling Price",
    "Actual Ship Date","Line Order Type","Line Order Type Name","Ship From Func Currency","Curr Exchange Rate","Extended Price USD",
    "Order Date","Booked Date","Cost of Sale Account","Ship From Org Name","Ship From Org Code","Bill To Customer Parent",
    "Bill To Customer Name","Ship To Customer Parent","Ship To Customer Name","Ship From Item Cost LC","Ship From Item Cost USD",
    "Extended Price LC","Sales Rep","Sales Region","Hold Type","Unit Selling PriceLC","Product Class","Sales Account","GL Date",
    "Invoice Curr Code","Quantity Credited","Quantity Invoiced","SOB Description","Invoice Period AVG Rate",Category,"Category Name",
    "Commodity Class","Item Description","Item Name","Product Family", "Product Line","Product Model","Sourcing Segment","Label (L5)",
    "Product Code & Label","Segment (L1)","Family Segment (L3)","Product/Segment (L2)","Family Group (L4)","Common vs System",
    "Promise Date",OU,"Bill To Region","Ship To Region","Ship To Country",TRANSACTION_TYPE,"Bill To Class Code","Ship To Class Code",
    "Invoice TRX Number","Ext Ship From Item Cost LC","Ext Ship From Item Cost USD","MGMT Customer","MGMT Region",Data_Type,SOURCE_SYSTEM,
    SNAPSHOT_DATE,LINE_ID,STATUS,ORDER_STATUS,LINE_STATUS,FULFILL_LINE_STATUS,ORDER_CREATION_DATE,LINE_CREATION_DATE,
    LAST_UPDATE_DATE,PRIMARY_MARKET_SEGMENT,SECONDARY_MARKET_SEGMENT,FULFILLMENT_LINE_NUMBER,line_source_number
)
SELECT
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
    TRANSACTION_TYPE,
    "Bill To Class Code",
    "Ship To Class Code",
    "Invoice TRX Number",
    "Ext Ship From Item Cost LC",
    "Ext Ship From Item Cost USD",
    "MGMT Customer",
    "MGMT Region",
    "Data_Type",
    SOURCE_SYSTEM,
    SNAPSHOT_DATE,
    LINE_ID,
    BOOK_STATUS,
    ORDER_STATUS,
    LINE_STATUS,
    FULFILL_LINE_STATUS,
    ORDER_CREATION_DATE,
    LINE_CREATION_DATE,
    LAST_UPDATE_DATE,
    PRIMARY_MARKET_SEGMENT,
    SECONDARY_MARKET_SEGMENT,
   FULFILLMENT_LINE_NUMBER,
    "line_source_number"
FROM edw_prod_dbo.FNC_BOOKINGS_V v
WHERE 1 = CASE
            WHEN COALESCE(UPPER(v."Bill To Customer Name"), '~') LIKE '%BROOKS%'
               OR COALESCE(UPPER(v."Ship To Customer Name"), '~') LIKE '%BROOKS%'
            THEN 0
            ELSE 1
          END;


INSERT INTO edw_prod_dbo.fnc_bookings_mv
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
    Category,
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
    OU,
    "Bill To Region",
    "Ship To Region",
    "Ship To Country",
    TRANSACTION_TYPE,
    "Bill To Class Code",
    "Ship To Class Code",
    "Invoice TRX Number",
    "Ext Ship From Item Cost LC",
    "Ext Ship From Item Cost USD",
    "MGMT Customer",
    "MGMT Region",
    Data_Type,
    SOURCE_SYSTEM,
    SNAPSHOT_DATE,
    LINE_ID,
    STATUS,
    ORDER_STATUS,
    LINE_STATUS,
    FULFILL_LINE_STATUS,
    ORDER_CREATION_DATE,
    LINE_CREATION_DATE,
    LAST_UPDATE_DATE
)
SELECT 
    s."Hdr Order Type Name",
    s."Order Number",
    s."Line Number",
    s."Shipment Number",
    s."Ordered Quantity",
    s."Order Exchange Rate",
    s."Transaction Curr Code",
    s."Cust PO Number",
    s."Request Date",
    s."Ordered Item",
    s."Scheduled Ship Date",
    s."Cancelled Quantity",
    s."Shipped Quantity",
    s."Unit Selling Price",
    s."Actual Ship Date",
    s."Line Order Type",
    s."Line Order Type Name",
    s."Ship From Func Currency",
    s."Curr Exchange Rate",
    s."Extended Price USD",
    s."Order Date",
    s."Booked Date",
    s."Cost of Sale Account",
    s."Ship From Org Name",
    s."Ship From Org Code",
    s."Bill To Customer Parent",
    s."Bill To Customer Name",
    s."Ship To Customer Parent",
    s."Ship To Customer Name",
    s."Ship From Item Cost LC",
    s."Ship From Item Cost USD",
    s."Extended Price LC",
    s."Sales Rep",
    s."Sales Region",
    s."Hold Type",
    s."Unit Selling PriceLC",
    s."Product Class",
    s."Sales Account",
    s."GL Date",
    s."Invoice Curr Code",
    s."Quantity Credited",
    s."Quantity Invoiced",
    s."SOB Description",
    s."Invoice Period AVG Rate",
    s.Category,
    s."Category Name",
    s."Commodity Class",
    s."Item Description",
    s."Item Name",
    s."Product Family",
    s."Product Line",
    s."Product Model",
    s."Sourcing Segment",
    p.level05_name AS "Label (L5)",
    p.level05_name AS "Product Code & Label",
    p."level01_name" AS "Segment (L1)",
    p."level03_name" AS "Family Segment (L3)",
    p."level02_name" AS "Product/Segment (L2)",
    p."level04_name" AS "Family Group (L4)",
    s."Common vs System",
    s."Promise Date",
    s.OU,
    s."Bill To Region",
    s."Ship To Region",
    s."Ship To Country",
    s.TRANSACTION_TYPE,
    s."Bill To Class Code",
    s."Ship To Class Code",
    s."Invoice TRX Number",
    s."Ext Ship From Item Cost LC",
    s."Ext Ship From Item Cost USD",
    s."MGMT Customer",
    s."MGMT Region",
    s.Data_Type,
    'EBS' AS "SOURCE_SYSTEM",
    s."Order Date" AS "SNAPSHOT_DATE",
    s.LINE_ID,
    sb.STATUS,
    ORDER_STATUS,
    LINE_STATUS,
    FULFILL_LINE_STATUS,
    ORDER_CREATION_DATE,
    LINE_CREATION_DATE,
    LAST_UPDATE_DATE
FROM stage_bi.FNC_BOOKING_RPT_VW_MV s
JOIN edw_prod_dbo.W_SALES_ORDERS_D e ON e.line_id = s.LINE_ID
JOIN stage_bi.BI_GLOBAL_BOOKINGS_TBL sb ON sb.LINE_ID = s.LINE_ID AND sb.SNAPSHOT_DATE = s."Order Date"
JOIN edw_prod_dbo.W_PRODUCT_DH p ON p.NODE = s."Product Class"
WHERE s.OU NOT IN ('BAI', 'BAA', 'BAK')
  AND 1 = CASE 
            WHEN COALESCE(UPPER(s."Bill To Customer Name"), '~') LIKE '%BROOKS%' 
              OR COALESCE(UPPER(s."Ship To Customer Name"), '~') LIKE '%BROOKS%' 
            THEN 0 ELSE 1 
          END;



WITH cte AS (
    SELECT LINE_ID, 
           QTY_ORDERED, 
           EXTENDED_PRICE, 
           SOURCE_SYSTEM,
           BOOKED_DATE,
           ROW_NUMBER() OVER (
               PARTITION BY LINE_ID, QTY_ORDERED, EXTENDED_PRICE, SOURCE_SYSTEM, BOOKED_DATE
               ORDER BY LINE_ID
           ) AS duplicatecount
    FROM edw_prod_dbo.W_BOOKING_F
    WHERE SOURCE_SYSTEM = 'OC'
)
DELETE FROM edw_prod_dbo.W_BOOKING_F
WHERE LINE_ID IN (
    SELECT LINE_ID FROM cte WHERE duplicatecount > 1
);

WITH cte AS (
    SELECT  LINE_ID , 
            QTY_ORDERED , 
            EXTENDED_PRICE , 
            SOURCE_SYSTEM ,
            BOOKED_DATE ,
           ROW_NUMBER() OVER (
               PARTITION BY  LINE_ID ,  QTY_ORDERED ,  EXTENDED_PRICE ,  SOURCE_SYSTEM ,  BOOKED_DATE 
               ORDER BY  LINE_ID 
           ) AS duplicatecount
    FROM edw_prod_dbo. W_BOOKING_TRXN 
    WHERE  SOURCE_SYSTEM  = 'OC'
)
DELETE FROM edw_prod_dbo.W_BOOKING_TRXN 
WHERE  LINE_ID  IN (
    SELECT  LINE_ID  FROM cte WHERE duplicatecount > 1
);

DELETE FROM edw_prod_dbo.fnc_bookings_mv
WHERE snapshot_date::date = '2024-08-04'
AND source_system = 'OC';

END;
$procedure$
;