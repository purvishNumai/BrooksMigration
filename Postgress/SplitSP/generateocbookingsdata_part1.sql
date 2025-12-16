CREATE OR REPLACE PROCEDURE edw_prod_dbo.generateocbookingsdata_part1()
 LANGUAGE plpgsql
AS $procedure$

DECLARE
    v_start_time   timestamp;
    v_end_time     timestamp;
    v_runtime_ms   numeric;
BEGIN
    -- Start time
    v_start_time := clock_timestamp();
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part1 - Started at %',v_start_time;

    CALL edw_prod_dbo.edw_log_start_job('GenerateOCBookingsData');
    CALL edw_prod_dbo.generateoccostsdata();


    -- Calc and Log Duration
    -- End time
    v_end_time := clock_timestamp();
    -- Duration in milliseconds
    v_runtime_ms := EXTRACT(EPOCH FROM (v_end_time - v_start_time)) * 1000;
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part1 - Completed. Start: %, End: %, Duration: % ms',v_start_time,v_end_time,v_runtime_ms;
END;
$procedure$
;