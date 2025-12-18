-- DROP PROCEDURE edw_prod_dbo.sp_refresh_redshift_objects_1_backup2025dec17();

CREATE OR REPLACE PROCEDURE edw_prod_dbo.sp_refresh_redshift_objects_1_backup2025dec17()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_start_time TIMESTAMPTZ := clock_timestamp();
    v_rows BIGINT := 0;
BEGIN
    PERFORM set_config('datestyle', 'ISO, MDY', true);

    -- Log Start
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_1', 'START', 'START', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 1. dm_shipping_v1 (FULL)
    v_start_time := clock_timestamp();
    TRUNCATE TABLE redshift_sync.dm_shipping_v1;
    INSERT INTO redshift_sync.dm_shipping_v1 
    ( sourceSystem, orgId, deliveryId, deliveryDetailId, soldToContactId, shipToContactId, billToContactId, customerId, moveOrderLineId, shipFromLocationId, shipToLocationId, shipToSiteUseId, deliverToLocationId, deliveryToSiteUseId, shipmentCurrency, deliveryNumber, deliveryStatusCode, deliveryStatus, sourceHeaderTypeName, shipmentNumber, shipDate, carrier, freightTermsCode, fobCode, waybill, subinventory, lotNumber, serialNumber, revision, releasedStatus, dateRequested, dateScheduled, packingInstructions, trackingNumber, custPoNumber, unitPrice, requestedQuantity, shippedQuantity, deliveredQuantity, cancelledQuantity, pickedQuantity, confirmDate, pickedDate, tpItemNumber, headerId, lineIdWSalesOrdersD, orderNumber, lifeCycleStatus, orderHeaderType, orderStatus, orderCurrency, holdType, customerPo, orderSource, orderSourceReference, serviceActivity, serviceAgreementNumber, serviceAgreementDescription, orderDate, orderCreationDate, orderCreatedBy, bookedDate, backlogDate, billOnlyShipDate, requestDate, promiseDate, scheduleDate, actualShipDate, fulfillmentDate, actualFulfillmentDate, latestAcceptableDate, bookedFlag, cancelledFlag, fulfilledFlag, lineNumber, lineStatus, lineCategory, lineType, lineTypeName, priceList, priceOverride, productClass, classification, orderShipmentNumber, orderedItem, itemTypeCode, modifierName, repairType, returnReasonCode, orderSubinventory, taxCode, taxDate, taxExcemptFlag, salesAccount, mktgAccountRule, mktgChannel, openFlag, pricingDate, shippableFlag, calcPriceFlag, fobPointCode, freightCarrierCode, mktgProductChannel, taxPointCode, scheduleStatusCode, shipmentPriorityCode, priceRequestCode, shippingMethodCode, orderUom, pricingUom, sourceType, lineCreationDate, lineCreatedBy, invTrxNumber, invTrxDate, invTrxRateType, invExchDate, invExchRate, invGlDate, invCurrencyCode, icPoNumber, icPoLine, refOrderNumber, refOrderLine, planDate, ebsLineId, fulfillmentLineNumber, customerPn, ouCode, ouName, stSiteUseId, stCustomerAccount, stCustomerName, stAccountName, stCustomerClassCode, stSemiNonSemi, stSiteUseCode, stSiteNumber, stSiteName, stAddress1, stAddress2, stAddress3, stAddress4, stCity, stState, stPostalCode, stCountry, stRegion, stTerritory, stChannel, stLocation, stPrimaryFlag, stStatus, stPrimaryMarketSegment, stSecondaryMarketSegment, stPartyId, stPartyNumber, stPartyType, stCustAccountId, stAccountStatus, stSiteStatus, soldCustAccountId, soldCustomerAccount, soldCustomerName, soldAccountName, soldCustomerClassCode, soldSemiNonSemi, soldRegion, invOrgCode, invOrgName, invOrgType, invOrgRegion, invOrgId, inventoryItemId, organizationId, organizationCode, itemNumber, description, buyerId, buyerName, plannerCode, plannerName, itemType, inventoryItemStatusCode, inventoryPlanningCode, itemSalesAccount, itemCostOfSalesAccount, itemExpenseAccount, atpFlag, atpRuleId, buildInWipFlag, checkShortagesFlag, customerOrderEnabledFlag, customerOrderFlag, internalOrderEnabledFlag, internalOrderFlag, inventoryAssetFlag, inventoryItemFlag, outsideOperationFlag, purchasingEnabledFlag, returnableFlag, serviceItemFlag, serviceableProductFlag, shippableItemFlag, soTransactionsFlag, itemStandardCost, standardCostCurrency, itemStandardCostUsd, leadTimeLotSize, fixedLeadTime, fullLeadTime, postprocessingLeadTime, preprocessingLeadTime, cumManufacturingLeadTime, cumulativeTotalLeadTime, variableLeadTime, fixedDaysSupply, fixedLotMultiplier, fixedOrderQuantity, reservableType, wipSupplyType, planningMakeBuyCode, primaryUomCode, primaryUnitOfMeasure, unitHeight, unitLength, unitOfIssue, unitVolume, unitWeight, unitWidth, volumeUomCode, weightUomCode, htsCode, productFamily, productLine, productModel, commodityClass, category, sourcingSegment, atpRuleName, itemCreationDate, countryOfOrigin, productCodeNode, productCodeLabel, segmentL1, familySegmentL3, productSegmentL2, familyGroupL4, materialCostUsd, itemStandardCostLc )
    select * from edw_prod_dbo.dm_shipping_v;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_1', 'FULL', 'dm_shipping_v1', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 2. inv_daily_shipping_v1 (FULL)
    v_start_time := clock_timestamp();
    TRUNCATE TABLE redshift_sync.inv_daily_shipping_v1;
    INSERT INTO redshift_sync.inv_daily_shipping_v1 
    ( confirmDate, orderNumber, orderType, org, wh, attention, customerPo, line, deliveryDetailId, delivery, subInventory, quantityShipped, orderCurrency, reportCurrency, unitPrice, functionalExchangeRate, functionalExtPrice, internalPartNumber, orderedItemNumber, itemDescription, productFamily, productLine, productModel, commodityClass, category, sourcingSegment, plannerCode, status, shipMethod, freightTerms, shippingTerms, freightCosts, carrier, tracking, wayBill, additionalTracking, dateOrdered, dateRequested, datePlanned, dateScheduled, datePromised, customerNumber, customerName, siteNumber, siteName, location, city, state, country, itemCreationDate, serialNumbers, soldToFirstName, soldToLastName, soldToEmail, soldToPhone, shipToFirstName, shipToLastName, shipToPhone, billToFirstName, billToLastName, billToPhone, actualShipmentDate, itemProductClass, shipmentPriority, makeBuy, atpRule, leadTimeDays, leadTimeWeeks, planningType, serviceActivityCode, copyExact, itemStatus, pickSubInventory, otdCode, otdComments, plannerNotes, otdPartNumber, otdSupplier, groupName, grossWeight, weightUom, htsCode, countryOfOrigin, pickedDate, extPriceUsd, icPoNumber, icPoLine, ebsSoNumber, ebsSoLine, sourceSystem )
    select * from edw_prod_dbo.inv_daily_shipping_v;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_1', 'FULL', 'inv_daily_shipping_v1', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 3. on_time_delivery_report_v1 (FULL)
    v_start_time := clock_timestamp();
    TRUNCATE TABLE edw_prod.redshift_sync.on_time_delivery_report_v1;
    INSERT INTO redshift_sync.on_time_delivery_report_v1 
    ( confirmDate, orderNumber, orderType, org, wH, attention, line, deliveryDetailId, delivery, subInventory, quantityShipped, orderCurrency, reportCurrency, unitPrice, functionalExchangeRate, functionalExtPrice, internalPartNumber, orderedItemNumber, itemDescription, productFamily, productLine, productModel, commodityClass, category, sourcingSegment, plannerCode, status, shipMethod, freightTerms, shippingTerms, freightCosts, carrier, trackingNumber, waybillNumber, additionalTracking, dateOrdered, dateRequested, datePlanned, dateScheduled, datePromised, customerNumber, customerName, siteNumber, siteName, location, city, state, country, itemCreationDate, serialNumbers, soldToFirstName, soldToLastName, soldToEmail, soldToPhone, shipToFirstName, shipToLastName, shipToPhone, billToFirstName, billToLastName, billToPhone, actualShipmentDate, itemProductClass, shipmentPriority, makeBuy, atpRule, leadTimeDays, leadTimeWeeks, planningType, serviceActivityCode, copyExact, itemStatus, pickSubInventory, otdCode, otdComments, plannerNotes, otdPartNumber, otdSupplier, groupName, grossWeight, weightUom, htsCode, countryOfOrigin, pickedDate, extPriceUsd, icPoNumber, icPoLine, ebsSoNumber, ebsSoLine, sourceSystem, classification, salesAccount, orderSource, itemTypeCode, stRegion, organizationCode, invOrgCode, requestedQuantity, stCustomerClassCode, marketingAccountRule )
    select * from edw_prod_dbo.on_time_delivery_report;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_1', 'FULL', 'on_time_delivery_report_v1', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 4. wkd_EMPLOYEE_DATA (FULL)
    v_start_time := clock_timestamp();
    TRUNCATE TABLE redshift_sync."wkd_EMPLOYEE_DATA";
    INSERT INTO redshift_sync."wkd_EMPLOYEE_DATA"
    ( effective_date, employee_id, employee_name, "position", "Group", division, subdivision, "function", hire_date, continuous_service_date, termination_date, company, "location", primary_work_address, country, region, cost_center_id, cost_center_name, legal_entity, product_code, department, department_name, manager_name, worker_type, conversion_check, termination_reason, time_type, conversion_check_date, weekly, filename, snapshot_date)
    Select * from stage_dbo."wkd_EMPLOYEE_DATA";
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_1', 'FULL', 'wkd_EMPLOYEE_DATA', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 5. w_contacts_d (FULL)
    v_start_time := clock_timestamp();
    TRUNCATE TABLE redshift_sync.w_contacts_d;
    insert into redshift_sync.w_contacts_d
    (contact_id,cust_acct_id,cust_contact_id,constact_status,contact_number,contact_name,contact_last_name,contact_middle_name,contact_first_name,contact_language,contact_addr_type,contact_address1,contact_address2,contact_postal_code,contact_city,contact_state,contact_province,contact_country,contact_email,contact_phone,integration_id,created_date,updated_date,source_system)
    select contact_id,cust_acct_id,cust_contact_id,constact_status,contact_number,contact_name,contact_last_name,contact_middle_name,contact_first_name,contact_language,contact_addr_type,contact_address1,contact_address2,contact_postal_code,contact_city,contact_state,contact_province,contact_country,contact_email,contact_phone,integration_id,created_date,updated_date,source_system
    from edw_prod_dbo.w_contacts_d;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_1', 'FULL', 'w_contacts_d', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 6. customer_site_attributes_v (FULL)
    v_start_time := clock_timestamp();
    truncate table redshift_sync.customer_site_attributes_v;
    insert into redshift_sync.customer_site_attributes_v(source_system, site_use_id, customer_account, customer_name, account_name, customer_class_code, semi_non_semi, site_use_code, site_number, site_name, address_1, address_2, address_3, address_4, city, state, postal_code, country, region, territory, channel, sales_rep, "location", primary_flag, status, primary_market_segment, secondary_market_segment, party_id, party_number, party_type, cust_account_id, account_status, site_status)  
    select * from edw_prod_dbo.customer_site_attributes_v;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_1', 'FULL', 'customer_site_attributes_v', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

     -- 7. bai_item_category_asgns (FULL)
    v_start_time := clock_timestamp();
    truncate Table redshift_sync.bai_item_category_asgns;
    INSERT INTO redshift_sync.bai_item_category_asgns
    (organization_name, organization_code, item_name, item_number, invoice_enabled_flag, default_shipping_org, internal_item, description, planner_code, item_description, product_class_ccid, category_type_name, category_name, cost_company, cost_product_class, cost_department, cost_account, cost_intercompany, sales_company, sales_product_class, sales_department, sales_account, sales_intercompany, focus_factory, product_family, product_line, product_model, item_price_code, commodity_class, category, sourcing_segment, cots_segment1, con_segment1, copy_segment1, engg_critical_segment1, ip_segment1, ord_availability, pln_segment1, rohs, test, country_of_origin, harm_tar_schedule, eccn, us_made, inventory_item_id, organization_id, category_set_id, category_id, last_updated_date, last_updated_by, created_date, created_by, "snapshot", cost_of_sale_account, current_exchange_rate, functional_currency, product_cost_frozen, make_buy_flag, item_where_used)
    Select * from   stage_ref.bai_item_category_asgns;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_1', 'FULL', 'bai_item_category_asgns', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- Log End
    v_start_time := clock_timestamp();
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_1', 'END', 'END', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);

EXCEPTION WHEN OTHERS THEN
    v_start_time := clock_timestamp();
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_1', 'UNKNOWN', 'GENERIC', v_start_time, clock_timestamp(), 0, 'FAILED', SQLERRM);
    RAISE;
END;
$procedure$
;