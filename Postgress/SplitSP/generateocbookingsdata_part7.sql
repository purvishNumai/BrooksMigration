CREATE OR REPLACE PROCEDURE edw_prod_dbo.generateocbookingsdata_part7()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_start_time   timestamp;
    v_end_time     timestamp;
    v_runtime_ms   numeric;
BEGIN
    
    -- Start Time
    v_start_time := clock_timestamp();
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part7 - Started at %', v_start_time;
    -------------------------------------------------------------------------
    -- set initial/ change transaction_type in w_booking_f (EBS)
    -------------------------------------------------------------------------
    WITH snap_dates AS (
        SELECT line_id, MIN(snapshot_date) AS snapshot_date
        FROM edw_prod_dbo.w_booking_f
        WHERE source_system = 'EBS'
        GROUP BY line_id
    )
    UPDATE edw_prod_dbo.w_booking_f b
    SET transaction_type = 'Initial'
    FROM snap_dates s
    WHERE b.line_id = s.line_id
      AND b.snapshot_date = s.snapshot_date
      AND b.transaction_type IS NULL
      AND b.source_system = 'EBS';

    UPDATE edw_prod_dbo.w_booking_f
    SET transaction_type = 'Change'
    WHERE transaction_type IS NULL
      AND source_system = 'EBS';

    -------------------------------------------------------------------------
    -- update salesrep region
    -------------------------------------------------------------------------
    UPDATE edw_prod_dbo.w_salesrep_d s
    SET region = c.country
    FROM edw_prod_ref.bi_country_region c
    WHERE s.region = c.region
      AND COALESCE(s.region, '~') <> COALESCE(c.region, '~');

    -- Calc and Log Duration
    -- End time
    v_end_time := clock_timestamp();
    -- Duration in milliseconds
    v_runtime_ms := EXTRACT(EPOCH FROM (v_end_time - v_start_time)) * 1000;
    RAISE NOTICE 'Wrapper: generateocbookingsdata_part7 - Completed. Start: %, End: %, Duration: % ms',v_start_time,v_end_time,v_runtime_ms;

END;
$procedure$
;