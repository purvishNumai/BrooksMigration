CREATE OR REPLACE PROCEDURE edw_prod_dbo.generateocbookingsdata_part2()
 LANGUAGE plpgsql
AS $procedure$

DECLARE
    v_start_time timestamp;
    v_runtime interval;
BEGIN

    v_start_time := clock_timestamp();
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part2 - Started at %', v_start_time;
     -- mirror MSSQL update OC.related_line_id where EBS.related_line_id points to OC.line_id
    UPDATE edw_prod_dbo.w_sales_orders_d oc
    SET related_line_id = ebs.line_id
    FROM edw_prod_dbo.w_sales_orders_d ebs
    WHERE oc.source_system = 'OC'
      AND ebs.source_system = 'EBS'
      AND ebs.related_line_id = oc.line_id
      AND oc.related_line_id IS NULL;

    -- mirror MSSQL update EBS.related_line_id where OC.related_line_id points to EBS.line_id
    UPDATE edw_prod_dbo.w_sales_orders_d ebs
    SET related_line_id = oc.line_id
    FROM edw_prod_dbo.w_sales_orders_d oc
    WHERE oc.source_system = 'OC'
      AND ebs.source_system = 'EBS'
      AND oc.related_line_id = ebs.line_id
      AND ebs.related_line_id IS NULL;

   -- Calc and Log Duration
    v_runtime := clock_timestamp() - v_start_time;
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part2 - Completed. Duration %', v_runtime;
END;
$procedure$
;