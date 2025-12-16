CREATE OR REPLACE PROCEDURE edw_prod_dbo.generateocbookingsdata_part8()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_start_time   timestamp;
    v_end_time     timestamp;
    v_runtime_ms   numeric;
BEGIN
    
    -- Start Time
    v_start_time := clock_timestamp();
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part8 - Started at %', v_start_time;
    -------------------------------------------------------------------------
    -- call loaders
    -------------------------------------------------------------------------
    CALL edw_prod_dbo.loadbbbv2();

    -- Calc and Log Duration
    -- End time
    v_end_time := clock_timestamp();
    -- Duration in milliseconds
    v_runtime_ms := EXTRACT(EPOCH FROM (v_end_time - v_start_time)) * 1000;
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part8 - Completed. Start: %, End: %, Duration: % ms',v_start_time,v_end_time,v_runtime_ms;
    
END;
$procedure$
;