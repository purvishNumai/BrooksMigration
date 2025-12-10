-- DROP PROCEDURE edw_prod_dbo.edw_log_start_job(varchar);

CREATE OR REPLACE PROCEDURE edw_prod_dbo.edw_log_start_job(IN process_name character varying)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Set process to zombie state if there is a process still marked as processing
    UPDATE edw_prod_ref.edw_processing_log as epl
    SET status = 'Z',
        message_text = 'Unable to determine process status.',
        completion_date = NOW()
    WHERE epl.process_name = $1
      AND status = 'P';

    -- Insert new processing log entry
    INSERT INTO edw_prod_ref.edw_processing_log (
        process_name,
        start_date,
        completion_date,
        status,
        message_text
    )
    VALUES (
        $1,
        NOW(),
        NULL,
        'P',
        'Processing'
    );
END;
$procedure$
;