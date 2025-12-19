-- DROP PROCEDURE edw_prod_dbo.generateoccostsdata();

CREATE OR REPLACE PROCEDURE edw_prod_dbo.generateoccostsdata()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    /*====================================================
      1. FAST DELETE (NO LOOP, NO CTID)
    ====================================================*/
    DELETE FROM edw_prod_dbo.w_item_costs_f
    WHERE source_system = 'OC';


    /*====================================================
      2. UNLOGGED COST ELEMENTS
    ====================================================*/
    DROP TABLE IF EXISTS tmp_cost_elements;
    CREATE UNLOGGED TABLE tmp_cost_elements AS
    SELECT
        cscd.std_cost_id,
        cscd.creation_date,
        cscd.last_update_date,
        cscd.unit_cost::float,
        cce.costelementbpeocostelementcode AS cost_element_code
    FROM oc_prod_dbo.cst_std_cost_details cscd
    JOIN oc_prod_dbo.cst_cost_elements_b cce
      ON cscd.cost_element_id = cce.costelementbpeocostelementid
    WHERE cscd.unit_cost IS NOT NULL
      AND cscd.unit_cost <> 0;

    CREATE INDEX ON tmp_cost_elements(std_cost_id);


    /*====================================================
      3. UNLOGGED DETAILS (ONE TIME BUILD)
    ====================================================*/
    DROP TABLE IF EXISTS tmp_details;
    CREATE UNLOGGED TABLE tmp_details AS
    SELECT
        iop.organizationid::bigint AS organization_id,
        csc.inventory_item_id::bigint AS inventory_item_id,
        csc.total_cost::float AS total_cost,
        ce.unit_cost::float AS unit_cost,
        ce.cost_element_code,
        csc.effective_start_date::date,
        COALESCE(csc.effective_end_date, '9999-12-31')::date AS effective_end_date,
        ce.creation_date::date,
        ce.last_update_date::date
    FROM oc_prod_dbo.cst_std_costs csc
    JOIN tmp_cost_elements ce
      ON ce.std_cost_id = csc.std_cost_id
    JOIN oc_prod_dbo.cst_val_units_b cvub
      ON csc.val_unit_id = cvub.val_unit_id
    JOIN oc_prod_dbo.cst_val_unit_details cvud
      ON cvub.cost_book_id = cvud.cost_book_id
     AND cvub.val_unit_id = cvud.val_unit_id
    JOIN oc_prod_dbo.cst_val_unit_combinations cvuc
      ON cvuc.val_unit_combination_id = cvud.val_unit_combination_id
    JOIN oc_prod_dbo.inv_org_parameters iop
      ON iop.organizationcode = cvuc.inv_org_code
    WHERE csc.status_code = 'PUBLISHED'
      AND EXISTS (
          SELECT 1
          FROM oc_prod_dbo.cst_val_structures_b cvsb
          WHERE cvsb.val_structure_id = cvub.val_structure_id
            AND cvsb.val_structure_type_code = 'ASSET'
      );

    CREATE INDEX ON tmp_details(inventory_item_id, organization_id);


    /*====================================================
      4. SINGLE SCAN INSERT (NO UNION ALL)
    ====================================================*/
    INSERT INTO edw_prod_dbo.w_item_costs_f (
        id,
        inventory_item_id,
        organization_id,
        item_standard_cost,
        material_cost,
        material_overhead_cost,
        resource_cost,
        overhead_cost,
        outside_processing_cost,
        last_updated,
        create_date,
        last_update_date,
        creation_date,
        source_system,
        integration_id,
        standard_cost_revision_date,
        effective_start_date,
        effective_end_date
    )
    SELECT
        -1,
        inventory_item_id,
        organization_id,
        SUM(total_cost) AS item_standard_cost,
        SUM(CASE WHEN cost_element_code = 'Material' THEN unit_cost ELSE 0 END),
        SUM(CASE WHEN cost_element_code = 'Material Overhead' THEN unit_cost ELSE 0 END),
        SUM(CASE WHEN cost_element_code = 'Resource' THEN unit_cost ELSE 0 END),
        SUM(CASE WHEN cost_element_code = 'Overhead' THEN unit_cost ELSE 0 END),
        SUM(CASE WHEN cost_element_code = 'Outside Processing' THEN unit_cost ELSE 0 END),
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        MAX(last_update_date),
        MAX(creation_date),
        'OC',
        inventory_item_id || '~' || organization_id || '~OC',
        MAX(effective_end_date),
        MIN(effective_start_date),
        MAX(effective_end_date)
    FROM tmp_details
    GROUP BY inventory_item_id, organization_id;


    /*====================================================
      5. SPECIFIC UPDATE (UNCHANGED)
    ====================================================*/
    UPDATE edw_prod_dbo.w_item_costs_f
    SET effective_end_date = '2024-08-29'
    WHERE inventory_item_id = 100000099129079
      AND organization_id = 300000004007248
      AND effective_start_date = '1900-01-01';


    DROP TABLE IF EXISTS tmp_cost_elements;
    DROP TABLE IF EXISTS tmp_details;

END;
$procedure$
;