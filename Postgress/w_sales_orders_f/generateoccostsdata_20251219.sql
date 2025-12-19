-- DROP PROCEDURE edw_prod_dbo.generateoccostsdata_20251219();

CREATE OR REPLACE PROCEDURE edw_prod_dbo.generateoccostsdata_20251219()
 LANGUAGE plpgsql
AS $procedure$
Declare
batch_size INT := 300000;
rows_affected INT := 1;
 batch_no INT := 0;

BEGIN
WHILE rows_affected > 0 LOOP
        DELETE FROM edw_prod_dbo.w_item_costs_f
        WHERE ctid IN (
            SELECT ctid 
            FROM edw_prod_dbo.w_item_costs_f 
            WHERE source_system = 'OC'
            LIMIT batch_size
        );
-- Capture number of rows deleted in this batch
        GET DIAGNOSTICS rows_affected = ROW_COUNT;
batch_no := batch_no + 1;
 -- Optional: log progress
        RAISE NOTICE 'Batch %, deleted % rows', batch_no, rows_affected;
 END LOOP;

-- Delete all OC costs OLD CODE 
--DELETE FROM edw_prod_dbo.w_item_costs_f WHERE source_system = 'OC';

-- Main cost calculation and insert
--27 Milion Record for cost_elements


-- WITH cost_elements AS (
--     SELECT
--         cscd.std_cost_detail_id,
--         cscd.std_cost_id,
--         cscd.creation_date,
--         cscd.last_update_date,
--         COALESCE(CAST(cscd.unit_cost AS FLOAT), 0) AS unit_cost,
--         cce.costelementbpeocostelementcode AS cost_element_code
--     FROM oc_prod_dbo.cst_std_cost_details cscd
--     JOIN oc_prod_dbo.cst_cost_elements_b cce
--       ON cscd.cost_element_id = cce.costelementbpeocostelementid
--     WHERE COALESCE(CAST(cscd.unit_cost AS FLOAT), 0) <> 0
-- ),

---Optimized code start

DROP TABLE IF EXISTS tmp_cost_elements;
CREATE TEMP TABLE tmp_cost_elements AS SELECT
        cscd.std_cost_detail_id,
        cscd.std_cost_id,
        cscd.creation_date,
        cscd.last_update_date,
        COALESCE(CAST(cscd.unit_cost AS FLOAT), 0) AS unit_cost,
        cce.costelementbpeocostelementcode AS cost_element_code
    FROM oc_prod_dbo.cst_std_cost_details cscd
    JOIN oc_prod_dbo.cst_cost_elements_b cce
      ON cscd.cost_element_id = cce.costelementbpeocostelementid
    WHERE COALESCE(CAST(cscd.unit_cost AS FLOAT), 0) <> 0;

    CREATE INDEX idx_tmp_cost_join ON tmp_cost_elements(std_cost_id);
    ANALYZE tmp_cost_elements;



    DROP TABLE IF EXISTS tmp_details;
    CREATE TEMP TABLE tmp_details AS SELECT
        ce.std_cost_detail_id,
        cvub_sub.inv_org_code,
        CAST(iop.organizationid AS bigint) AS organization_id,
        CAST(csc.inventory_item_id AS bigint) AS inventory_item_id,
        CAST(csc.total_cost AS FLOAT) AS total_cost,
        CAST(ce.unit_cost AS FLOAT) AS unit_cost,
        ce.cost_element_code,
        CAST(csc.effective_start_date AS DATE) AS effective_start_date,
        CAST(COALESCE(csc.effective_end_date, '9999-12-31') AS DATE) AS effective_end_date,
        CAST(ce.creation_date AS DATE) AS creation_date,
        CAST(ce.last_update_date AS DATE) AS last_update_date
    FROM oc_prod_dbo.cst_std_costs csc
    -- JOIN cost_elements ce ON ce.std_cost_id = csc.std_cost_id -- Old code
    JOIN tmp_cost_elements ce ON ce.std_cost_id = csc.std_cost_id -- New code
    JOIN (
        SELECT
            cvuc.inv_org_code AS inv_org_code,
            cvub.cost_org_id,
            cvub.cost_book_id,
            cvub.val_unit_id
        FROM oc_prod_dbo.cst_val_units_b cvub
        JOIN oc_prod_dbo.cst_val_unit_details cvud
          ON cvub.cost_book_id = cvud.cost_book_id
         AND cvub.val_unit_id = cvud.val_unit_id
        JOIN oc_prod_dbo.cst_val_unit_combinations cvuc
          ON cvuc.val_unit_combination_id = cvud.val_unit_combination_id
        WHERE EXISTS (
            SELECT 1
            FROM oc_prod_dbo.cst_val_structures_b cvsb
            WHERE cvsb.val_structure_id = cvub.val_structure_id
              AND cvsb.val_structure_type_code = 'ASSET'
        )
    ) cvub_sub ON csc.val_unit_id = cvub_sub.val_unit_id
    JOIN oc_prod_dbo.inv_org_parameters iop ON iop.organizationcode = cvub_sub.inv_org_code
    WHERE csc.status_code = 'PUBLISHED';

    CREATE INDEX idx_tmp_details_join ON tmp_details(cost_element_code);
    ANALYZE tmp_details;

    ---Optimized code end
--With added here in details
-- WITH details AS (
--     SELECT
--         ce.std_cost_detail_id,
--         cvub_sub.inv_org_code,
--         CAST(iop.organizationid AS bigint) AS organization_id,
--         CAST(csc.inventory_item_id AS bigint) AS inventory_item_id,
--         CAST(csc.total_cost AS FLOAT) AS total_cost,
--         CAST(ce.unit_cost AS FLOAT) AS unit_cost,
--         ce.cost_element_code,
--         CAST(csc.effective_start_date AS DATE) AS effective_start_date,
--         CAST(COALESCE(csc.effective_end_date, '9999-12-31') AS DATE) AS effective_end_date,
--         CAST(ce.creation_date AS DATE) AS creation_date,
--         CAST(ce.last_update_date AS DATE) AS last_update_date
--     FROM oc_prod_dbo.cst_std_costs csc
--     -- JOIN cost_elements ce ON ce.std_cost_id = csc.std_cost_id -- Old code
--     JOIN idx_tmp_cost_join ce ON ce.std_cost_id = csc.std_cost_id -- New code
--     JOIN (
--         SELECT
--             cvuc.inv_org_code AS inv_org_code,
--             cvub.cost_org_id,
--             cvub.cost_book_id,
--             cvub.val_unit_id
--         FROM oc_prod_dbo.cst_val_units_b cvub
--         JOIN oc_prod_dbo.cst_val_unit_details cvud
--           ON cvub.cost_book_id = cvud.cost_book_id
--          AND cvub.val_unit_id = cvud.val_unit_id
--         JOIN oc_prod_dbo.cst_val_unit_combinations cvuc
--           ON cvuc.val_unit_combination_id = cvud.val_unit_combination_id
--         WHERE EXISTS (
--             SELECT 1
--             FROM oc_prod_dbo.cst_val_structures_b cvsb
--             WHERE cvsb.val_structure_id = cvub.val_structure_id
--               AND cvsb.val_structure_type_code = 'ASSET'
--         )
--     ) cvub_sub ON csc.val_unit_id = cvub_sub.val_unit_id
--     JOIN oc_prod_dbo.inv_org_parameters iop ON iop.organizationcode = cvub_sub.inv_org_code
--     WHERE csc.status_code = 'PUBLISHED'
-- )

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
    inventory_item_id::bigint,
    organization_id::bigint,
    SUM(item_standard_cost) AS item_standard_cost,
    SUM(material_cost) AS material_cost,
    SUM(material_overhead_cost) AS material_overhead_cost,
    SUM(resource_cost) AS resource_cost,
    SUM(overhead_cost) AS overhead_cost,
    SUM(outside_processing_cost) AS outside_processing_cost,
    CURRENT_TIMESTAMP AS last_updated,
    CURRENT_TIMESTAMP AS create_date,
    last_update_date,
    creation_date,
    'OC' AS source_system,
    LEFT(CAST(inventory_item_id AS VARCHAR) || '~' || CAST(organization_id AS VARCHAR) || '~OC', 10) AS integration_id,
    effective_end_date,
    effective_start_date,
    effective_end_date
FROM (
    SELECT DISTINCT
        inventory_item_id::bigint,
        organization_id::bigint,
        COALESCE(total_cost, 0.0) AS item_standard_cost,
        0.0 AS material_cost,
        0.0 AS material_overhead_cost,
        0.0 AS resource_cost,
        0.0 AS overhead_cost,
        0.0 AS outside_processing_cost,
        last_update_date,
        creation_date,
        effective_end_date,
        effective_start_date
    -- FROM details --old code
    FROM tmp_details --new code
    
    UNION ALL
    SELECT
        inventory_item_id::bigint,
        organization_id::bigint,
        0 AS item_standard_cost,
        unit_cost AS material_cost,
        0 AS material_overhead_cost,
        0 AS resource_cost,
        0 AS overhead_cost,
        0 AS outside_processing_cost,
        last_update_date,
        creation_date,
        effective_end_date,
        effective_start_date
    -- FROM details WHERE cost_element_code = 'Material' --old code    
    FROM tmp_details WHERE cost_element_code = 'Material' --new code
    UNION ALL
    SELECT
        inventory_item_id::bigint,
        organization_id::bigint,
        0 AS item_standard_cost,
        0 AS material_cost,
        unit_cost AS material_overhead_cost,
        0 AS resource_cost,
        0 AS overhead_cost,
        0 AS outside_processing_cost,
        last_update_date,
        creation_date,
        effective_end_date,
        effective_start_date
    -- FROM details WHERE cost_element_code = 'Material Overhead' --old code    
    FROM tmp_details WHERE cost_element_code = 'Material Overhead' --new code
    UNION ALL
    SELECT
        inventory_item_id::bigint,
        organization_id::bigint,
        0 AS item_standard_cost,
        0 AS material_cost,
        0 AS material_overhead_cost,
        unit_cost AS resource_cost,
        0 AS overhead_cost,
        0 AS outside_processing_cost,
        last_update_date,
        creation_date,
        effective_end_date,
        effective_start_date
    -- FROM details WHERE cost_element_code = 'Resource' --old code
    FROM tmp_details WHERE cost_element_code = 'Resource' --new code
    UNION ALL
    SELECT
        inventory_item_id::bigint,
        organization_id::bigint,
        0 AS item_standard_cost,
        0 AS material_cost,
        0 AS material_overhead_cost,
        0 AS resource_cost,
        unit_cost AS overhead_cost,
        0 AS outside_processing_cost,
        last_update_date,
        creation_date,
        effective_end_date,
        effective_start_date
    -- FROM details WHERE cost_element_code = 'Overhead' --old code
    FROM tmp_details WHERE cost_element_code = 'Overhead' --new code
    UNION ALL
    SELECT
        inventory_item_id::bigint,
        organization_id::bigint,
        0 AS item_standard_cost,
        0 AS material_cost,
        0 AS material_overhead_cost,
        0 AS resource_cost,
        0 AS overhead_cost,
        unit_cost AS outside_processing_cost,
        last_update_date,
        creation_date,
        effective_end_date,
        effective_start_date
    -- FROM details WHERE cost_element_code = 'Outside Processing' --old code
    FROM tmp_details WHERE cost_element_code = 'Outside Processing' --new code
) d
GROUP BY
    inventory_item_id,
    organization_id,
    last_update_date,
    creation_date,
    effective_end_date,
    effective_start_date;

-- Update effective_end_date for a specific record
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