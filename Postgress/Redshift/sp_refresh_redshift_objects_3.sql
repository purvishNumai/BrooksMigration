-- DROP PROCEDURE edw_prod_dbo.sp_refresh_redshift_objects_3();

CREATE OR REPLACE PROCEDURE edw_prod_dbo.sp_refresh_redshift_objects_3()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_start_time TIMESTAMPTZ := clock_timestamp();
    v_rows BIGINT := 0;
BEGIN
    PERFORM set_config('datestyle', 'ISO, MDY', true);

    -- Log Start
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'START', 'START', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 1. w_purchase_order_d (INCREMENTAL)
    v_start_time := clock_timestamp();
    MERGE INTO redshift_sync.w_purchase_order_d AS tgt
    USING edw_prod_dbo.w_purchase_order_d AS src
    ON tgt.integration_id = src.integration_id
    WHEN MATCHED THEN UPDATE SET
        po_header_id = src.po_header_id, po_line_id = src.po_line_id,
        line_location_id = src.line_location_id, type_lookup_code = src.type_lookup_code,
        po_number = src.po_number, po_line_number = src.po_line_number,
        po_shipment_num = src.po_shipment_num, po_line_location_num = src.po_line_location_num,
        release_num = src.release_num, ship_to_location = src.ship_to_location,
        bill_to_location = src.bill_to_location, po_creation_date = src.po_creation_date,
        ship_via_lookup_code = src.ship_via_lookup_code, fob_lookup_code = src.fob_lookup_code,
        freight_terms_lookup_code = src.freight_terms_lookup_code, currency_code = src.currency_code,
        authorization_status = src.authorization_status, approved_flag = src.approved_flag,
        approved_date = src.approved_date, note_to_vendor = src.note_to_vendor,
        note_to_receiver = src.note_to_receiver, po_comments = src.po_comments,
        item_description = src.item_description, line_note_to_vendor = src.line_note_to_vendor,
        line_type = src.line_type, unit_price = src.unit_price,
        unit_meas_lookup_code = src.unit_meas_lookup_code, need_by_date = src.need_by_date,
        promised_date = src.promised_date, cancel_flag = src.cancel_flag,
        cancel_date = src.cancel_date, cancel_reason = src.cancel_reason,
        closed_code = src.closed_code, item_revision = src.item_revision,
        link_to_sales_order = src.link_to_sales_order, item_matl_cost = src.item_matl_cost,
        fai_required = src.fai_required, po_line_comments = src.po_line_comments,
        copy_exact = src.copy_exact, demand_source = src.demand_source,
        created_date = src.created_date, updated_date = src.updated_date,
        source_system = src.source_system, line_creation_date = src.line_creation_date,
        line_created_by = src.line_created_by, last_update_date = src.last_update_date,
        last_update_by = src.last_update_by, line_status = src.line_status,
        schedule_status = src.schedule_status, product_type = src.product_type,
        original_promise_date = src.original_promise_date
    WHEN NOT MATCHED THEN INSERT (
        po_header_id, po_line_id, line_location_id, type_lookup_code, po_number, po_line_number,
        po_shipment_num, po_line_location_num, release_num, ship_to_location, bill_to_location,
        po_creation_date, ship_via_lookup_code, fob_lookup_code, freight_terms_lookup_code,
        currency_code, authorization_status, approved_flag, approved_date, note_to_vendor,
        note_to_receiver, po_comments, item_description, line_note_to_vendor, line_type, unit_price,
        unit_meas_lookup_code, need_by_date, promised_date, cancel_flag, cancel_date, cancel_reason,
        closed_code, item_revision, link_to_sales_order, item_matl_cost, fai_required,
        po_line_comments, copy_exact, demand_source, integration_id, created_date, updated_date,
        source_system, line_creation_date, line_created_by, last_update_date, last_update_by,
        line_status, schedule_status, product_type, original_promise_date
    ) VALUES (
        src.po_header_id, src.po_line_id, src.line_location_id, src.type_lookup_code, src.po_number,
        src.po_line_number, src.po_shipment_num, src.po_line_location_num, src.release_num,
        src.ship_to_location, src.bill_to_location, src.po_creation_date, src.ship_via_lookup_code,
        src.fob_lookup_code, src.freight_terms_lookup_code, src.currency_code, src.authorization_status,
        src.approved_flag, src.approved_date, src.note_to_vendor, src.note_to_receiver, src.po_comments,
        src.item_description, src.line_note_to_vendor, src.line_type, src.unit_price,
        src.unit_meas_lookup_code, src.need_by_date, src.promised_date, src.cancel_flag,
        src.cancel_date, src.cancel_reason, src.closed_code, src.item_revision, src.link_to_sales_order,
        src.item_matl_cost, src.fai_required, src.po_line_comments, src.copy_exact, src.demand_source,
        src.integration_id, src.created_date, src.updated_date, src.source_system,
        src.line_creation_date, src.line_created_by, src.last_update_date, src.last_update_by,
        src.line_status, src.schedule_status, src.product_type, src.original_promise_date
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'INCREMENTAL', 'w_purchase_order_d', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- 2. Other Incremental Loads
    -- w_item_org_d
    v_start_time := clock_timestamp();
    MERGE INTO redshift_sync.w_item_org_d AS tgt
    USING edw_prod_dbo.w_item_org_d AS src
    ON tgt.integration_id = src.integration_id
    WHEN MATCHED THEN UPDATE SET
        inventory_item_id = src.inventory_item_id, organization_id = src.organization_id,
        organization_code = src.organization_code, item_number = src.item_number,
        description = src.description, buyer_id = src.buyer_id, buyer_name = src.buyer_name,
        planner_code = src.planner_code, planner_name = src.planner_name, item_type = src.item_type,
        inventory_item_status_code = src.inventory_item_status_code,
        inventory_planning_code = src.inventory_planning_code,
        sales_account = src.sales_account, cost_of_sales_account = src.cost_of_sales_account,
        expense_account = src.expense_account, atp_flag = src.atp_flag,
        atp_rule_id = src.atp_rule_id, build_in_wip_flag = src.build_in_wip_flag,
        check_shortages_flag = src.check_shortages_flag,
        customer_order_enabled_flag = src.customer_order_enabled_flag,
        customer_order_flag = src.customer_order_flag,
        internal_order_enabled_flag = src.internal_order_enabled_flag,
        internal_order_flag = src.internal_order_flag, inventory_asset_flag = src.inventory_asset_flag,
        inventory_item_flag = src.inventory_item_flag, outside_operation_flag = src.outside_operation_flag,
        purchasing_enabled_flag = src.purchasing_enabled_flag, returnable_flag = src.returnable_flag,
        service_item_flag = src.service_item_flag, serviceable_product_flag = src.serviceable_product_flag,
        shippable_item_flag = src.shippable_item_flag, so_transactions_flag = src.so_transactions_flag,
        item_standard_cost = src.item_standard_cost, standard_cost_currency = src.standard_cost_currency,
        item_standard_cost_usd = src.item_standard_cost_usd, lead_time_lot_size = src.lead_time_lot_size,
        fixed_lead_time = src.fixed_lead_time, full_lead_time = src.full_lead_time,
        postprocessing_lead_time = src.postprocessing_lead_time,
        preprocessing_lead_time = src.preprocessing_lead_time,
        cum_manufacturing_lead_time = src.cum_manufacturing_lead_time,
        cumulative_total_lead_time = src.cumulative_total_lead_time,
        variable_lead_time = src.variable_lead_time, fixed_days_supply = src.fixed_days_supply,
        fixed_lot_multiplier = src.fixed_lot_multiplier, fixed_order_quantity = src.fixed_order_quantity,
        reservable_type = src.reservable_type, wip_supply_type = src.wip_supply_type,
        planning_make_buy_code = src.planning_make_buy_code, primary_uom_code = src.primary_uom_code,
        primary_unit_of_measure = src.primary_unit_of_measure, unit_height = src.unit_height,
        unit_length = src.unit_length, unit_of_issue = src.unit_of_issue, unit_volume = src.unit_volume,
        unit_weight = src.unit_weight, unit_width = src.unit_width,
        volume_uom_code = src.volume_uom_code, weight_uom_code = src.weight_uom_code,
        hts_code = src.hts_code, country_of_origin = src.country_of_origin,
        created_date = src.created_date, updated_date = src.updated_date,
        source_system = src.source_system, product_family = src.product_family,
        product_line = src.product_line, product_model = src.product_model,
        internal_item = src.internal_item, eccn = src.eccn, sourcing_segment = src.sourcing_segment,
        category = src.category, commodity_class = src.commodity_class,
        copy_segment = src.copy_segment, atp_rule_name = src.atp_rule_name,
        material_cost = src.material_cost, material_cost_usd = src.material_cost_usd,
        material_overhead_cost = src.material_overhead_cost,
        material_overhead_cost_usd = src.material_overhead_cost_usd,
        resource_cost = src.resource_cost, resource_cost_usd = src.resource_cost_usd,
        overhead_cost = src.overhead_cost, overhead_cost_usd = src.overhead_cost_usd,
        outside_processing_cost = src.outside_processing_cost,
        outside_processing_cost_usd = src.outside_processing_cost_usd,
        default_shipping_org = src.default_shipping_org, item_creation_date = src.item_creation_date,
        dso_currency_code = src.dso_currency_code, atp_lead_time = src.atp_lead_time
    WHEN NOT MATCHED THEN INSERT (
        inventory_item_id, organization_id, organization_code, item_number, description, buyer_id,
        buyer_name, planner_code, planner_name, item_type, inventory_item_status_code,
        inventory_planning_code, sales_account, cost_of_sales_account, expense_account, atp_flag,
        atp_rule_id, build_in_wip_flag, check_shortages_flag, customer_order_enabled_flag,
        customer_order_flag, internal_order_enabled_flag, internal_order_flag, inventory_asset_flag,
        inventory_item_flag, outside_operation_flag, purchasing_enabled_flag, returnable_flag,
        service_item_flag, serviceable_product_flag, shippable_item_flag, so_transactions_flag,
        item_standard_cost, standard_cost_currency, item_standard_cost_usd, lead_time_lot_size,
        fixed_lead_time, full_lead_time, postprocessing_lead_time, preprocessing_lead_time,
        cum_manufacturing_lead_time, cumulative_total_lead_time, variable_lead_time,
        fixed_days_supply, fixed_lot_multiplier, fixed_order_quantity, reservable_type,
        wip_supply_type, planning_make_buy_code, primary_uom_code, primary_unit_of_measure,
        unit_height, unit_length, unit_of_issue, unit_volume, unit_weight, unit_width,
        volume_uom_code, weight_uom_code, hts_code, country_of_origin, integration_id,
        created_date, updated_date, source_system, product_family, product_line, product_model,
        internal_item, eccn, sourcing_segment, category, commodity_class, copy_segment,
        atp_rule_name, material_cost, material_cost_usd, material_overhead_cost,
        material_overhead_cost_usd, resource_cost, resource_cost_usd, overhead_cost, overhead_cost_usd,
        outside_processing_cost, outside_processing_cost_usd, default_shipping_org,
        item_creation_date, dso_currency_code, atp_lead_time
    ) VALUES (
        src.inventory_item_id, src.organization_id, src.organization_code, src.item_number,
        src.description, src.buyer_id, src.buyer_name, src.planner_code, src.planner_name,
        src.item_type, src.inventory_item_status_code, src.inventory_planning_code,
        src.sales_account, src.cost_of_sales_account, src.expense_account, src.atp_flag,
        src.atp_rule_id, src.build_in_wip_flag, src.check_shortages_flag,
        src.customer_order_enabled_flag, src.customer_order_flag, src.internal_order_enabled_flag,
        src.internal_order_flag, src.inventory_asset_flag, src.inventory_item_flag,
        src.outside_operation_flag, src.purchasing_enabled_flag, src.returnable_flag,
        src.service_item_flag, src.serviceable_product_flag, src.shippable_item_flag,
        src.so_transactions_flag, src.item_standard_cost, src.standard_cost_currency,
        src.item_standard_cost_usd, src.lead_time_lot_size, src.fixed_lead_time, src.full_lead_time,
        src.postprocessing_lead_time, src.preprocessing_lead_time, src.cum_manufacturing_lead_time,
        src.cumulative_total_lead_time, src.variable_lead_time, src.fixed_days_supply,
        src.fixed_lot_multiplier, src.fixed_order_quantity, src.reservable_type, src.wip_supply_type,
        src.planning_make_buy_code, src.primary_uom_code, src.primary_unit_of_measure,
        src.unit_height, src.unit_length, src.unit_of_issue, src.unit_volume, src.unit_weight,
        src.unit_width, src.volume_uom_code, src.weight_uom_code, src.hts_code, src.country_of_origin,
        src.integration_id, src.created_date, src.updated_date, src.source_system, src.product_family,
        src.product_line, src.product_model, src.internal_item, src.eccn, src.sourcing_segment,
        src.category, src.commodity_class, src.copy_segment, src.atp_rule_name, src.material_cost,
        src.material_cost_usd, src.material_overhead_cost, src.material_overhead_cost_usd,
        src.resource_cost, src.resource_cost_usd, src.overhead_cost, src.overhead_cost_usd,
        src.outside_processing_cost, src.outside_processing_cost_usd, src.default_shipping_org,
        src.item_creation_date, src.dso_currency_code, src.atp_lead_time
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'INCREMENTAL', 'w_item_org_d', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- w_po_buyer_d
    v_start_time := clock_timestamp();
    MERGE INTO redshift_sync.w_po_buyer_d AS tgt
    USING edw_prod_dbo.w_po_buyer_d AS src
    ON tgt.integration_id = src.integration_id
    WHEN MATCHED THEN UPDATE SET
        person_id = src.person_id, buyer_full_name = src.buyer_full_name,
        buyer_first_name = src.buyer_first_name, buyer_last_name = src.buyer_last_name,
        buyer_email_address = src.buyer_email_address, start_date = src.start_date,
        end_date = src.end_date, created_date = src.created_date,
        updated_date = src.updated_date, source_system = src.source_system
    WHEN NOT MATCHED THEN INSERT (
        person_id, buyer_full_name, buyer_first_name, buyer_last_name,
        buyer_email_address, start_date, end_date, integration_id,
        created_date, updated_date, source_system
    ) VALUES (
        src.person_id, src.buyer_full_name, src.buyer_first_name, src.buyer_last_name,
        src.buyer_email_address, src.start_date, src.end_date, src.integration_id,
        src.created_date, src.updated_date, src.source_system
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'INCREMENTAL', 'w_po_buyer_d', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- w_supplier_d
    v_start_time := clock_timestamp();
    MERGE INTO redshift_sync.w_supplier_d AS tgt
    USING edw_prod_dbo.w_supplier_d AS src
    ON tgt.integration_id = src.integration_id
    WHEN MATCHED THEN UPDATE SET
        vendor_id = src.vendor_id, vendor_name = src.vendor_name,
        vendor_name_alt = src.vendor_name_alt, vendor_code = src.vendor_code,
        enabled_flag = src.enabled_flag, vendor_type = src.vendor_type,
        customer_num = src.customer_num, one_time_flag = src.one_time_flag,
        freight_terms = src.freight_terms, fob_code = src.fob_code,
        pay_date_basis = src.pay_date_basis, pay_group = src.pay_group,
        invoice_currency_code = src.invoice_currency_code,
        payment_currency_code = src.payment_currency_code,
        payment_method = src.payment_method, start_date_active = src.start_date_active,
        end_date_active = src.end_date_active, vendor_creation_date = src.vendor_creation_date,
        vendor_last_update_date = src.vendor_last_update_date,
        ship_to_location = src.ship_to_location, bill_to_location = src.bill_to_location,
        group_col = src.group_col, cloud_supplier_number = src.cloud_supplier_number,
        supplier_classification = src.supplier_classification,
        brks_payment_terms = src.brks_payment_terms,
        agile_access_enabled = src.agile_access_enabled,
        gtm_screening_status = src.gtm_screening_status,
        gtm_sanction_status = src.gtm_sanction_status,
        created_date = src.created_date, updated_date = src.updated_date,
        source_system = src.source_system
    WHEN NOT MATCHED THEN INSERT (
        vendor_id, vendor_name, vendor_name_alt, vendor_code, enabled_flag, vendor_type,
        customer_num, one_time_flag, freight_terms, fob_code, pay_date_basis, pay_group,
        invoice_currency_code, payment_currency_code, payment_method, start_date_active,
        end_date_active, vendor_creation_date, vendor_last_update_date, ship_to_location,
        bill_to_location, group_col, cloud_supplier_number, supplier_classification,
        brks_payment_terms, agile_access_enabled, gtm_screening_status, gtm_sanction_status,
        integration_id, created_date, updated_date, source_system
    ) VALUES (
        src.vendor_id, src.vendor_name, src.vendor_name_alt, src.vendor_code, src.enabled_flag,
        src.vendor_type, src.customer_num, src.one_time_flag, src.freight_terms, src.fob_code,
        src.pay_date_basis, src.pay_group, src.invoice_currency_code, src.payment_currency_code,
        src.payment_method, src.start_date_active, src.end_date_active, src.vendor_creation_date,
        src.vendor_last_update_date, src.ship_to_location, src.bill_to_location, src.group_col,
        src.cloud_supplier_number, src.supplier_classification, src.brks_payment_terms,
        src.agile_access_enabled, src.gtm_screening_status, src.gtm_sanction_status,
        src.integration_id, src.created_date, src.updated_date, src.source_system
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'INCREMENTAL', 'w_supplier_d', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- w_supplier_site_d
    v_start_time := clock_timestamp();
    MERGE INTO redshift_sync.w_supplier_site_d AS tgt
    USING edw_prod_dbo.w_supplier_site_d AS src
    ON tgt.integration_id = src.integration_id
    WHEN MATCHED THEN UPDATE SET
        vendor_site_id = src.vendor_site_id, vendor_site_code = src.vendor_site_code,
        vendor_name = src.vendor_name, vendor_name_alt = src.vendor_name_alt,
        vendor_code = src.vendor_code, vendor_type = src.vendor_type,
        purchasing_site_flag = src.purchasing_site_flag, pay_site_flag = src.pay_site_flag,
        address_line1 = src.address_line1, address_line2 = src.address_line2,
        address_line3 = src.address_line3, address_line4 = src.address_line4,
        city = src.city, state = src.state, zip = src.zip, province = src.province,
        country = src.country, site_freight_terms = src.site_freight_terms,
        site_fob_code = src.site_fob_code, site_pay_date_basis = src.site_pay_date_basis,
        site_pay_group = src.site_pay_group, site_invoice_currency_code = src.site_invoice_currency_code,
        site_payment_currency_code = src.site_payment_currency_code,
        site_payment_method = src.site_payment_method, site_inactive_date = src.site_inactive_date,
        site_creation_date = src.site_creation_date, site_last_update_date = src.site_last_update_date,
        site_ship_to_location = src.site_ship_to_location, site_bill_to_location = src.site_bill_to_location,
        supplier_commodity = src.supplier_commodity, supplier_region = src.supplier_region,
        group_col = src.group_col, cloud_supplier_address = src.cloud_supplier_address,
        cloud_supplier_site = src.cloud_supplier_site, amex_ebs_site_id = src.amex_ebs_site_id,
        created_date = src.created_date, updated_date = src.updated_date,
        source_system = src.source_system
    WHEN NOT MATCHED THEN INSERT (
        vendor_site_id, vendor_site_code, vendor_name, vendor_name_alt, vendor_code, vendor_type,
        purchasing_site_flag, pay_site_flag, address_line1, address_line2, address_line3, address_line4,
        city, state, zip, province, country, site_freight_terms, site_fob_code, site_pay_date_basis,
        site_pay_group, site_invoice_currency_code, site_payment_currency_code, site_payment_method,
        site_inactive_date, site_creation_date, site_last_update_date, site_ship_to_location,
        site_bill_to_location, supplier_commodity, supplier_region, group_col, cloud_supplier_address,
        cloud_supplier_site, amex_ebs_site_id, integration_id, created_date, updated_date, source_system
    ) VALUES (
        src.vendor_site_id, src.vendor_site_code, src.vendor_name, src.vendor_name_alt, src.vendor_code,
        src.vendor_type, src.purchasing_site_flag, src.pay_site_flag, src.address_line1, src.address_line2,
        src.address_line3, src.address_line4, src.city, src.state, src.zip, src.province, src.country,
        src.site_freight_terms, src.site_fob_code, src.site_pay_date_basis, src.site_pay_group,
        src.site_invoice_currency_code, src.site_payment_currency_code, src.site_payment_method,
        src.site_inactive_date, src.site_creation_date, src.site_last_update_date,
        src.site_ship_to_location, src.site_bill_to_location, src.supplier_commodity, src.supplier_region,
        src.group_col, src.cloud_supplier_address, src.cloud_supplier_site, src.amex_ebs_site_id,
        src.integration_id, src.created_date, src.updated_date, src.source_system
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'INCREMENTAL', 'w_supplier_site_d', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- w_ap_terms_d
    v_start_time := clock_timestamp();
    MERGE INTO redshift_sync.w_ap_terms_d AS tgt
    USING edw_prod_dbo.w_ap_terms_d AS src
    ON tgt.integration_id = src.integration_id
    WHEN MATCHED THEN UPDATE SET
        terms_id = src.terms_id, ap_term_name = src.ap_term_name,
        ap_term_desc = src.ap_term_desc, created_date = src.created_date,
        updated_date = src.updated_date, source_system = src.source_system
    WHEN NOT MATCHED THEN INSERT (
        terms_id, ap_term_name, ap_term_desc, integration_id,
        created_date, updated_date, source_system
    ) VALUES (
        src.terms_id, src.ap_term_name, src.ap_term_desc, src.integration_id,
        src.created_date, src.updated_date, src.source_system
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'INCREMENTAL', 'w_ap_terms_d', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);


    -- 3. Full Loads
    -- dm_purchase_orders_v
    v_start_time := clock_timestamp();
    truncate table redshift_sync.dm_purchase_orders_v;
    insert into redshift_sync.dm_purchase_orders_v(source_system, unit_price, quantity, quantity_received, quantity_cancelled, quantity_billed, quantity_accepted, quantity_rejected, po_header_id, po_line_id, line_location_id, type_lookup_code, po_number, po_line_number, po_shipment_num, po_line_location_num, release_num, ship_to_location, bill_to_location, po_creation_date, ship_via_lookup_code, fob_lookup_code, freight_terms_lookup_code, currency_code, authorization_status, approved_flag, approved_date, note_to_vendor, note_to_receiver, po_comments, item_description, line_note_to_vendor, line_type, unit_pricew_purchase_order_d, unit_meas_lookup_code, need_by_date, promised_date, cancel_flag, cancel_date, cancel_reason, closed_code, item_revision, link_to_sales_order, item_matl_cost, fai_required, po_line_comments, copy_exact, demand_source, line_creation_date, line_created_by, line_status, schedule_status, product_type, person_id, buyer_full_name, buyer_first_name, buyer_last_name, buyer_email_address, vendor_id, vendor_name, vendor_name_alt, vendor_code, enabled_flag, vendor_type, customer_num, one_time_flag, freight_terms, fob_code, pay_date_basis, pay_group, invoice_currency_code, payment_currency_code, payment_method, start_date_active, end_date_active, vendor_creation_date, vendor_last_update_date, group_col, cloud_supplier_number, supplier_classification, brks_payment_terms, agile_access_enabled, gtm_screening_status, gtm_sanction_status, vendor_site_id, vendor_site_code, purchasing_site_flag, pay_site_flag, address_line1, address_line2, address_line3, address_line4, city, state, zip, province, country, site_freight_terms, site_fob_code, site_pay_date_basis, site_pay_group, site_invoice_currency_code, site_payment_currency_code, site_payment_method, site_inactive_date, site_creation_date, site_last_update_date, supplier_commodity, supplier_region, group_col_w_supplier_site_d, cloud_supplier_address, cloud_supplier_site, amex_ebs_site_id, terms_id_w_ap_terms_d, ap_term_name, ap_term_desc, inventory_item_id, organization_id, item_number, item_master_description, default_ship_org, internal_item, product_family, product_line, product_model, commodity_class, category, sourcing_segment, country_of_origin, eccn, copy_segment, where_used, category_name, item_id, item_num, description, item_buyer_name, planner_code, planner_name, item_type, inventory_item_status_code, inventory_planning_code, sales_account, cost_of_sales_account, expense_account, atp_flag, atp_rule_id, build_in_wip_flag, check_shortages_flag, customer_order_enabled_flag, customer_order_flag, internal_order_enabled_flag, internal_order_flag, inventory_asset_flag, inventory_item_flag, outside_operation_flag, purchasing_enabled_flag, returnable_flag, service_item_flag, serviceable_product_flag, shippable_item_flag, so_transactions_flag, item_standard_cost, standard_cost_currency, item_standard_cost_usd, lead_time_lot_size, fixed_lead_time, full_lead_time, postprocessing_lead_time, preprocessing_lead_time, cum_manufacturing_lead_time, cumulative_total_lead_time, variable_lead_time, fixed_days_supply, fixed_lot_multiplier, fixed_order_quantity, reservable_type, wip_supply_type, planning_make_buy_code, primary_uom_code, primary_unit_of_measure, unit_height, unit_length, unit_of_issue, unit_volume, unit_weight, unit_width, volume_uom_code, weight_uom_code, hts_code, material_cost, material_cost_usd, material_overhead_cost, material_overhead_cost_usd, resource_cost, resource_cost_usd, overhead_cost, overhead_cost_usd, outside_processing_cost, outside_processing_cost_usd, inv_org_code, inv_org_name, inv_org_type, inv_org_id, org_id_w_oper_unit_d, ou_code, ou_name, supplier_payment_term_name, supplier_payment_term_description)
    select * from  edw_prod_dbo.dm_purchase_orders_v;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'FULL', 'dm_purchase_orders_v', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- w_po_delivery_f
    v_start_time := clock_timestamp();
    Truncate table redshift_sync.w_po_delivery_f;
    insert into redshift_sync.w_po_delivery_f
    (report_run_date, business_unit, destination_org_code, source_document_type, buyer_name, ship_to_location, purchase_order_number, po_line_number, po_shipment_number, po_distribution_number, po_type, need_by_date, promised_date, po_release_number, po_release_creation_date, po_line_creation_date, receipt_number, delivered_date, calendar_month, supplier_name, supplier_country, supplier_site, supplier_site_country, pack_slip_number, item_number, item_description, quantity_ordered, delivery_quantity, transaction_currency_code, entered_currency_code, accounted_currency_code, po_unit_price, func_po_unit_price, delivered_amount, func_corp_rate_at_rcpt_to_usd, delivered_amount_usd, corp_rate_at_rcpt_to_func_curr, delivered_amount_func_curr, charge_account_number, natural_account, receiver_name, dest_type_or_trans_type, cost_product_class, product_family, product_line, product_model, destination_subinventory, "locator", commodity_class, category, sourcing_segment, abc_code, vendor_type_lookup_code, rpt_transaction_type, std_cost, std_cost_usd, mfg_site_std_cost_usd, extended_std_cost, extended_std_cost_usd, extended_mfg_site_std_cost_usd, material_cost_entered, material_cost_accounted, material_cost_usd, material_overhead_cost_entered, material_overhead_cost_accounted, material_overhead_cost_usd, receipt_yw, coo, hts, po_uom, stock_uom, po_unit_price_conv_stock_uom, three_month_item_avg_usd, three_month_item_org_avg, six_month_item_avg_usd, six_month_item_org_avg, transaction_id, created_date, updated_date, source_system)
    select * from edw_prod_dbo.w_po_delivery_f;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'FULL', 'w_po_delivery_f', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- w_purchase_order_f
    v_start_time := clock_timestamp();
    TRUNCATE TABLE redshift_sync.w_purchase_order_f;
    INSERT INTO redshift_sync.w_purchase_order_f
    (po_header_id, po_line_id, po_line_location_id, agent_id, vendor_id, vendor_site_id, terms_id, item_id, ship_to_organization_id, unit_price, quantity, quantity_received, quantity_cancelled, quantity_billed, quantity_accepted, quantity_rejected, integration_id, created_date, updated_date, source_system, org_id)
    select 
    po_header_id, po_line_id, po_line_location_id, agent_id, vendor_id, vendor_site_id, terms_id, item_id, ship_to_organization_id, unit_price, quantity, quantity_received, quantity_cancelled, quantity_billed, quantity_accepted, quantity_rejected, integration_id, created_date, updated_date, source_system, org_id
    from 
    edw_prod_dbo.w_purchase_order_f;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'INCREMENTAL', 'w_purchase_order_f ', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- INV_PO_COMMITMENT_V
    v_start_time := clock_timestamp();
    Truncate table redshift_sync.INV_PO_COMMITMENT_V;  
    insert into redshift_sync.INV_PO_COMMITMENT_V
    (ou_name, inv_org_code, inv_org_name, buyer_full_name, supplier_name, supplier_site, supplier_contact_name, supplier_email_address, buyer_email_address, purchase_order_type, document_number, line_number, line_location_num, release_num, po_shipment_num, item_number, item_description, po_creation_date, promised_date, need_by_date, total_ordered_quantity, remaining_quantity, quantity_received, quantity_cancelled, quantity_billed, quantity_accepted, quantity_rejected, unit_price, extended_price, remaining_amount, currency_code, functional_currency_code, functional_exchange_rate, total_functional_amount, functional_remaining_amount, product_family, product_line, product_model, planner_code, cancel_flag, line_status, schedule_status, product_type, line_type, link_to_sales_order, line_note_to_vendor, note_to_vendor, note_to_receiver, commodity_class, category, sourcing_segment, po_status, where_used, ship_to_location, inventory_item_status_code, source_system, item_standard_cost_usd, material_cost_usd, unit_price_usd, ext_price_usd, remaining_amount_usd)
    select * from  edw_prod_dbo.INV_PO_COMMITMENT_V  ;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'FULL', 'INV_PO_COMMITMENT_V', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- w_po_approved_supplier_list_d
    v_start_time := clock_timestamp();
    Truncate table  redshift_sync.W_PO_APPROVED_SUPPLIER_LIST_D;
    INSERT INTO redshift_sync.w_po_approved_supplier_list_d
    ( organization_code, item_number, description, inventory_item_status_code, item_type, product_family, product_line, product_model, commodity_class, sourcing_segment, asl_supplier_name, asl_supplier_number, asl_supplier_site_code, asl_status, disable_flag, last_po_number, last_po_type, last_po_supplier_name, last_po_supplier_number, last_po_supplier_site_code, last_receipt_date, asl_source_system, po_receipt_source_system, asl_id, inventory_item_id, owning_organization_id, using_organization_id, vendor_id, vendor_site_id, rcv_transaction_id)
    Select * from edw_prod_dbo.w_po_approved_supplier_list_d;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'FULL', 'w_po_approved_supplier_list_d', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);

    -- w_msc_sourcing_rules_d
    v_start_time := clock_timestamp();
    Truncate table redshift_sync.w_msc_sourcing_rules_d;
    INSERT INTO redshift_sync.w_msc_sourcing_rules_d
    ( organization_code, item_number, description, inventory_item_status_code, item_type, product_family, product_line, product_model, commodity_class, sourcing_segment, assignment_set_name, sourcing_rule_name, effective_start_date, effective_end_date, partner_1, partner_1_percent, partner_2, partner_2_percent, partner_3, partner_3_percent, partner_4, partner_4_percent, source_system)
    Select * from edw_prod_dbo.w_msc_sourcing_rules_d;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'FULL', 'w_msc_sourcing_rules_d', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);  

    -- inv_po_copy_exact_v
    v_start_time := clock_timestamp();
    truncate table redshift_sync.inv_po_copy_exact_v;
    INSERT INTO redshift_sync.inv_po_copy_exact_v
    ( item_number, org_code, item_description, item_status, item_type, make_buy, copy_exact, source_system) 
    Select * from edw_prod_dbo.inv_po_copy_exact_v;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'FULL', 'inv_po_copy_exact_v', v_start_time, clock_timestamp(), v_rows, 'SUCCESS', NULL);  

    -- Log End
    v_start_time := clock_timestamp();
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'END', 'END', v_start_time, clock_timestamp(), 0, 'SUCCESS', NULL);

EXCEPTION WHEN OTHERS THEN
    v_start_time := clock_timestamp();
    CALL edw_prod_dbo.sp_log_audit('sp_refresh_redshift_objects_3', 'UNKNOWN', 'GENERIC', v_start_time, clock_timestamp(), 0, 'FAILED', SQLERRM);
    RAISE;
END;
$procedure$
;