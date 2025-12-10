-- DROP PROCEDURE edw_prod_dbo.edw_log_update_job(varchar, varchar, text);

CREATE OR REPLACE PROCEDURE edw_prod_dbo.edw_log_update_job(IN p_process_name character varying, IN p_status character varying, IN p_message_text text)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Set process to provided status
    UPDATE edw_prod_ref.edw_processing_log as epl
    SET status         = p_status,
        message_text   = p_message_text,
        completion_date = NOW()
    WHERE epl.process_name = p_process_name
      AND status = 'P';
END;
$procedure$
;