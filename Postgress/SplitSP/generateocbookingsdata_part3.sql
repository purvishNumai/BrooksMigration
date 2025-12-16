CREATE OR REPLACE PROCEDURE edw_prod_dbo.generateocbookingsdata_part3()
 LANGUAGE plpgsql
AS $procedure$

DECLARE
    v_start_time timestamp;
    v_runtime interval;
BEGIN

    v_start_time := clock_timestamp();
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part3 - Started at %', v_start_time;
    -- downstream procs
    CALL edw_prod_dbo.updatecloudorderswithebsorgs();
    CALL edw_prod_dbo.updatearsodata();

    -- Calc and Log Duration
    v_runtime := clock_timestamp() - v_start_time;
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part3 - Completed. Duration %', v_runtime;
END;
$procedure$
;