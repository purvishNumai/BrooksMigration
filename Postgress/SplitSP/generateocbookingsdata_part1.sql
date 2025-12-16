CREATE OR REPLACE PROCEDURE edw_prod_dbo.generateocbookingsdata_part1()
 LANGUAGE plpgsql
AS $procedure$

DECLARE
    v_start_time timestamp;
    v_runtime interval;
BEGIN
    v_start_time := clock_timestamp();
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part1 - Started at %', v_start_time;

    CALL edw_prod_dbo.edw_log_start_job('GenerateOCBookingsData');
    CALL edw_prod_dbo.generateoccostsdata();


    -- Calc and Log Duration
    v_runtime := clock_timestamp() - v_start_time;
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part1 - Completed. Duration %', v_runtime;
END;
$procedure$
;