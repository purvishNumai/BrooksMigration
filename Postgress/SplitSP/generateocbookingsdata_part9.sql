CREATE OR REPLACE PROCEDURE edw_prod_dbo.generateocbookingsdata_part9()
 LANGUAGE plpgsql
AS $procedure$

DECLARE
    v_start_time timestamp;
    v_runtime interval;
BEGIN
    
    v_start_time := clock_timestamp();
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part9 - Started at %', v_start_time;

    -- downstream procs
    CALL edw_prod_dbo.loadbookingsdata();

    -- Calc and Log Duration
    v_runtime := clock_timestamp() - v_start_time;
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part9 - Completed. Duration %', v_runtime;
END;
$procedure$
;